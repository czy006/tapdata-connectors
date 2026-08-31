package io.tapdata.connector.paimon.write.bucket;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Version-locked split and failure fixture for sequential index-bootstrap tests. */
final class PaimonSequentialIndexBootstrapFixture {

    final FileStoreTable table = mock(FileStoreTable.class);
    final ReadBuilder readBuilder = mock(ReadBuilder.class);
    final DataTableScan scan = mock(DataTableScan.class);
    final TableScan.Plan plan = mock(TableScan.Plan.class);
    final TableRead tableRead = mock(TableRead.class);
    final AtomicInteger currentTimeSamples = new AtomicInteger();
    final List<Event> events = Collections.synchronizedList(new ArrayList<>());

    private final Map<Split, ReaderPlan> readerPlans = new IdentityHashMap<>();
    private final Map<Split, String> splitNames = new IdentityHashMap<>();
    private final List<Split> plannedSplits = new ArrayList<>();
    private final InternalRowSerializer partitionSerializer =
            new InternalRowSerializer(RowType.of(DataTypes.INT()));
    private final Map<String, String> options = new LinkedHashMap<>();
    private final long sampledCurrentTime;

    private volatile int[] projection;
    private volatile Map<String, String> copyOptions;
    private volatile Filter<Integer> bucketFilter;

    PaimonSequentialIndexBootstrapFixture(Duration indexTtl, long sampledCurrentTime)
            throws IOException {
        this.sampledCurrentTime = sampledCurrentTime;
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.CROSS_PARTITION_UPSERT_BOOTSTRAP_PARALLELISM.key(), "2");
        if (indexTtl != null) {
            options.put(
                    CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key(),
                    indexTtl.toMillis() + " ms");
        }

        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("ignored", DataTypes.STRING())
                        .column("pk", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .options(options)
                        .build();
        TableSchema tableSchema = TableSchema.create(0L, schema);
        when(table.rowType()).thenReturn(tableSchema.logicalRowType());
        when(table.schema()).thenReturn(tableSchema);
        when(table.partitionKeys()).thenReturn(tableSchema.partitionKeys());
        when(table.options()).thenReturn(options);
        when(table.copy(any(Map.class)))
                .thenAnswer(
                        invocation -> {
                            copyOptions = new LinkedHashMap<>(invocation.getArgument(0));
                            return table;
                        });
        when(table.newReadBuilder()).thenReturn(readBuilder);
        when(readBuilder.withProjection(any(int[].class)))
                .thenAnswer(
                        invocation -> {
                            projection = invocation.getArgument(0);
                            return readBuilder;
                        });
        when(readBuilder.newScan()).thenReturn(scan);
        when(scan.withBucketFilter(any()))
                .thenAnswer(
                        invocation -> {
                            bucketFilter = invocation.getArgument(0);
                            return scan;
                        });
        when(scan.plan()).thenReturn(plan);
        when(plan.splits())
                .thenAnswer(
                        invocation ->
                                plannedSplits.stream()
                                        .filter(
                                                split ->
                                                        bucketFilter == null
                                                                || bucketFilter.test(
                                                                        ((DataSplit) split)
                                                                                .bucket()))
                                        .collect(Collectors.toList()));
        when(readBuilder.newRead()).thenReturn(tableRead);
        when(tableRead.createReader(any(Split.class)))
                .thenAnswer(invocation -> readerPlans.get(invocation.getArgument(0)).open());
    }

    LongSupplier currentTimeSupplier() {
        return () -> {
            currentTimeSamples.incrementAndGet();
            return sampledCurrentTime;
        };
    }

    ReaderPlan addSplit(
            String name,
            int partition,
            int bucket,
            long[] fileCreationTimes,
            List<List<InternalRow>> batches) {
        BinaryRow binaryPartition =
                partitionSerializer.toBinaryRow(GenericRow.of(partition)).copy();
        List<DataFileMeta> files =
                Arrays.stream(fileCreationTimes)
                        .mapToObj(PaimonSequentialIndexBootstrapFixture::newFile)
                        .collect(Collectors.toList());
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(binaryPartition)
                        .withBucket(bucket)
                        .withBucketPath("")
                        .withDataFiles(files)
                        .build();
        ReaderPlan readerPlan = new ReaderPlan(name, batches);
        plannedSplits.add(split);
        readerPlans.put(split, readerPlan);
        splitNames.put(split, name);
        return readerPlan;
    }

    ReaderPlan addSplit(String name, int partition, int bucket, long fileCreationTime, int... keys) {
        List<InternalRow> rows =
                Arrays.stream(keys).mapToObj(GenericRow::of).collect(Collectors.toList());
        return addSplit(
                name,
                partition,
                bucket,
                new long[] {fileCreationTime},
                Collections.singletonList(rows));
    }

    void assertLatestTrimmedProjection() {
        assertEquals(
                CoreOptions.StartupMode.LATEST.toString(),
                copyOptions.get(CoreOptions.SCAN_MODE.key()));
        org.junit.jupiter.api.Assertions.assertArrayEquals(new int[] {2}, projection);
    }

    void assertAllReaderEventsOn(long callerThreadId) {
        assertTrue(!events.isEmpty(), "expected reader lifecycle events");
        for (Event event : events) {
            assertEquals(
                    callerThreadId,
                    event.threadId,
                    () -> event.action + " did not execute on the caller thread");
        }
    }

    List<String> eventActions() {
        synchronized (events) {
            return events.stream().map(event -> event.action).collect(Collectors.toList());
        }
    }

    static List<String> drain(RecordReader<InternalRow> reader) throws Exception {
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> ownedReader = reader) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = ownedReader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        rows.add(row.getInt(0) + "|" + row.getInt(1) + "|" + row.getInt(2));
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return rows;
    }

    private void record(String action) {
        events.add(new Event(action, Thread.currentThread().getId()));
    }

    private static DataFileMeta newFile(long timeMillis) {
        return DataFileMeta.create(
                "",
                1L,
                1L,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                EMPTY_STATS,
                null,
                0L,
                1L,
                0L,
                DataFileMeta.DUMMY_LEVEL,
                Collections.emptyList(),
                Timestamp.fromLocalDateTime(
                        Instant.ofEpochMilli(timeMillis)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDateTime()),
                0L,
                null,
                FileSource.APPEND,
                null,
                null,
                null,
                null);
    }

    final class ReaderPlan {
        final String name;
        final List<List<InternalRow>> batches;
        final AtomicInteger createCount = new AtomicInteger();
        final AtomicInteger readBatchCount = new AtomicInteger();
        final AtomicInteger releaseCount = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();

        IOException createFailure;
        IOException readBatchFailure;
        int readBatchFailureCall = 1;
        RuntimeException releaseFailure;
        IOException closeFailure;

        private ReaderPlan(String name, List<List<InternalRow>> batches) {
            this.name = name;
            this.batches = batches;
        }

        ReaderPlan failCreate(IOException failure) {
            this.createFailure = failure;
            return this;
        }

        ReaderPlan failReadBatch(IOException failure) {
            this.readBatchFailure = failure;
            return this;
        }

        ReaderPlan failReadBatchOnCall(int call, IOException failure) {
            this.readBatchFailureCall = call;
            this.readBatchFailure = failure;
            return this;
        }

        ReaderPlan failRelease(RuntimeException failure) {
            this.releaseFailure = failure;
            return this;
        }

        ReaderPlan failClose(IOException failure) {
            this.closeFailure = failure;
            return this;
        }

        private RecordReader<InternalRow> open() throws IOException {
            createCount.incrementAndGet();
            record("create:" + name);
            if (createFailure != null) {
                throw createFailure;
            }
            return new RecordReader<InternalRow>() {
                private int batchIndex;

                @Override
                public RecordIterator<InternalRow> readBatch() throws IOException {
                    int call = readBatchCount.incrementAndGet();
                    record("read:" + name);
                    if (readBatchFailure != null && call == readBatchFailureCall) {
                        throw readBatchFailure;
                    }
                    if (batchIndex >= batches.size()) {
                        return null;
                    }
                    int currentBatch = batchIndex;
                    List<InternalRow> rows = batches.get(batchIndex++);
                    return new RecordIterator<InternalRow>() {
                        private int rowIndex;

                        @Override
                        public InternalRow next() {
                            return rowIndex < rows.size() ? rows.get(rowIndex++) : null;
                        }

                        @Override
                        public void releaseBatch() {
                            releaseCount.incrementAndGet();
                            record("release:" + name + ":" + currentBatch);
                            if (releaseFailure != null) {
                                throw releaseFailure;
                            }
                        }
                    };
                }

                @Override
                public void close() throws IOException {
                    closeCount.incrementAndGet();
                    record("close:" + name);
                    if (closeFailure != null) {
                        throw closeFailure;
                    }
                }
            };
        }
    }

    private static final class Event {
        private final String action;
        private final long threadId;

        private Event(String action, long threadId) {
            this.action = action;
            this.threadId = threadId;
        }
    }
}
