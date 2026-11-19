package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Standalone local capacity sample, compiled outside the connector and run in a new JVM per phase. */
public final class ServiceRecoveryBenchmark {
    private static final String TABLE = "orders";
    private static final int BUCKETS = 2;
    private static final int CDC_INTERVAL_MS = 500;
    private static final int CDC_ROWS = 64;

    public static void main(String[] args) throws Exception {
        Path base = Path.of(args[0]);
        boolean strong = args[1].equals("strong");
        String phase = args[2];
        int batches = Integer.parseInt(args[3]);
        int batchSize = Integer.parseInt(args[4]);
        int rows = Math.multiplyExact(batches, batchSize);
        if (batches <= 0 || batchSize <= 0 || rows < CDC_ROWS) {
            throw new IllegalArgumentException("Use positive batch dimensions with at least 64 rows in total");
        }
        Path warehouse = Files.createDirectories(base.resolve("warehouse"));
        Path ledger = base.resolve("recovery");
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse(warehouse.toString());
        config.setDatabase("default");
        config.setStorageType("local");
        config.setDiskTmpDir(Files.createDirectories(base.resolve("spill")).toString());
        config.setBucketMode("fixed");
        config.setBucketCount(BUCKETS);
        config.setEnableAutoCompaction(false);
        config.setWriteBufferSize(8);
        config.setEnableAsyncCommit(true);
        config.setCommitIntervalMs(CDC_INTERVAL_MS);
        config.setBatchAccumulationSize(CDC_ROWS * 10);
        config.setEnableDeduplicateUpdateAfterCompletenessCheck(true);
        if (strong) {
            config.loadNodeConfig(Collections.singletonMap("localRecoveryDir", ledger.toString()));
        }
        TapConnectorContext context = mock(TapConnectorContext.class);
        Log log = mock(Log.class);
        when(context.getLog()).thenReturn(log);
        KVMap<Object> state = PaimonStartupRecoveryIntegrationTest.taskState(base.resolve("task.properties"));
        when(context.getStateMap()).thenReturn(state);
        PaimonService service = new PaimonService(config, log);
        service.setFlushOffsetCallback(offset -> { throw new AssertionError("Unexpected offset callback"); });
        service.init();
        long startup = System.nanoTime();
        service.startLocalRecovery(context);
        long startupMs = millisSince(startup);
        TapTable schema = new TapTable(TABLE).add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("value", "STRING"));
        if (!phase.equals("recover")) { service.createTable(schema); }
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(
                Collections.singletonMap("warehouse", warehouse.toUri().toString()))))) {
            FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("default", TABLE));
            if (phase.equals("recover")) {
                verify(table, rows, false, 1);
                System.out.println("SAMPLE quiet_recovery_ms=" + startupMs);
            } else {
                List<List<TapRecordEvent>> initial = new ArrayList<>();
                for (int b = 0; b < batches; b++) {
                    initial.add(events(b * batchSize, batchSize, false));
                }
                long start = System.nanoTime();
                for (List<TapRecordEvent> batch : initial) {
                    service.writeRecords(batch, schema, context);
                }
                long writeNs = System.nanoTime() - start;
                if (table.snapshotManager().latestSnapshotIdFromFileSystem() != null) {
                    throw new AssertionError("Snapshot published before afterInitialSync");
                }
                long[] manifest = preparedBytes(ledger);
                if (phase.equals("crash")) {
                    System.out.flush();
                    Runtime.getRuntime().halt(42);
                }
                AtomicLong peakHeap = new AtomicLong();
                Thread sampler = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        peakHeap.accumulateAndGet(heapUsed(), Math::max);
                        try { Thread.sleep(2L); }
                        catch (InterruptedException stop) { return; }
                    }
                });
                long beforeCommit = heapUsed();
                peakHeap.set(beforeCommit);
                sampler.start();
                long publish = System.nanoTime();
                long publishNs;
                try {
                    service.afterInitialSync(context, schema);
                    publishNs = System.nanoTime() - publish;
                } finally {
                    peakHeap.accumulateAndGet(heapUsed(), Math::max);
                    sampler.interrupt();
                    sampler.join();
                }
                verify(table, rows, false, 1);
                List<Long> files = new ArrayList<>();
                Map<String, Integer> perBucket = new TreeMap<>();
                Map<String, Long> bytesPerBucket = new TreeMap<>();
                try (Stream<Path> tree = Files.walk(warehouse.resolve("default.db").resolve(TABLE))) {
                    for (Path p : (Iterable<Path>) tree.filter(Files::isRegularFile)::iterator) {
                        if (p.getFileName().toString().startsWith("data-") &&
                                p.getParent().getFileName().toString().startsWith("bucket-")) {
                            long bytes = Files.size(p);
                            String bucket = p.getParent().getFileName().toString();
                            files.add(bytes);
                            perBucket.merge(bucket, 1, Integer::sum);
                            bytesPerBucket.merge(bucket, bytes, Long::sum);
                        }
                    }
                }
                Collections.sort(files);
                if (files.isEmpty() || perBucket.size() != BUCKETS) { throw new AssertionError("Missing bucket files"); }
                List<TapRecordEvent> cdc = events(0, CDC_ROWS, true);
                long cdcStart = System.nanoTime();
                service.writeRecords(cdc, schema, context);
                long ackMs = millisSince(cdcStart);
                while (!Long.valueOf(2).equals(table.snapshotManager().latestSnapshotIdFromFileSystem())) {
                    if (millisSince(cdcStart) >= 30_000) { throw new AssertionError("CDC timer did not publish"); }
                    Thread.sleep(5);
                }
                long visibleMs = millisSince(cdcStart);
                verify(table, rows, true, 2);
                System.out.printf("SAMPLE mode=%s full_ms=%d publish_ms=%d snapshot_count=1 files=%d " +
                                "files_per_bucket=%s bytes_per_bucket=%s data_bytes=%d min_bytes=%d lower_median_bytes=%d max_bytes=%d " +
                                "prepare_count=%d prepare_bytes=%d ledger_bytes=%d " +
                                "heap_before_commit=%d heap_sample_peak=%d cdc_ack_ms=%d cdc_visible_ms=%d%n",
                        args[1], TimeUnit.NANOSECONDS.toMillis(writeNs + publishNs), TimeUnit.NANOSECONDS.toMillis(publishNs),
                        files.size(), perBucket.values().toString().replace(" ", ""), bytesPerBucket.values().toString().replace(" ", ""),
                        files.stream().mapToLong(Long::longValue).sum(), files.get(0), files.get((files.size() - 1) / 2),
                        files.get(files.size() - 1), manifest[0], manifest[1], manifest[2],
                        beforeCommit, peakHeap.get(), ackMs, visibleMs);
            }
        } finally {
            service.close();
        }
    }

    private static long heapUsed() { return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(); }
    private static long millisSince(long start) { return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start); }

    private static List<TapRecordEvent> events(int start, int size, boolean cdc) {
        List<TapRecordEvent> events = new ArrayList<>(size);
        for (int id = start; id < start + size; id++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", id);
            row.put("value", value(id, cdc));
            TapInsertRecordEvent event = new TapInsertRecordEvent().init().table(TABLE).after(row);
            event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, cdc ? "CDC" : "INITIAL_SYNC");
            event.addInfo(TapCallbackOffset.KEY_NODE_IDS, Collections.singletonList("source"));
            if (cdc) {
                event.addInfo(TapCallbackOffset.KEY_STREAM_OFFSET, "p" + id);
                event.addInfo(TapCallbackOffset.KEY_SOURCE_TIME, (long) id);
            }
            events.add(event);
        }
        return events;
    }

    private static String value(int id, boolean cdc) {
        return (cdc ? "updated-order-" : "initial-order-") + id + "-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    }

    /** Sample immediately after the final acknowledged full batch, before publication and GC. */
    private static long[] preparedBytes(Path dir) throws Exception {
        long[] stats = new long[3];
        if (Files.exists(dir)) {
            try (Stream<Path> tree = Files.walk(dir)) {
                for (Path file : (Iterable<Path>) tree.filter(Files::isRegularFile)::iterator) {
                    long bytes = Files.size(file);
                    stats[2] += bytes;
                    if (file.getFileName().toString().matches("prepare-[0-9]+")) {
                        stats[0]++;
                        stats[1] += bytes;
                    }
                }
            }
        }
        return stats;
    }

    private static void verify(FileStoreTable table, int rows, boolean cdc, long snapshot) throws Exception {
        if (!Long.valueOf(snapshot).equals(table.snapshotManager().latestSnapshotIdFromFileSystem())) {
            throw new AssertionError("Expected snapshot " + snapshot);
        }
        Set<Integer> seen = new HashSet<>();
        ReadBuilder read = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> {
                int id = row.getInt(0);
                if (id < 0 || id >= rows || !seen.add(id) || !value(id, cdc && id < CDC_ROWS).equals(row.getString(1).toString())) {
                    throw new AssertionError("Unexpected key/value for id " + id);
                }
            });
        }
        if (seen.size() != rows) { throw new AssertionError("Missing rows: " + seen.size() + "/" + rows); }
    }
}
