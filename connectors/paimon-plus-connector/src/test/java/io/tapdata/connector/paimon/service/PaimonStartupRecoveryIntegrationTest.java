package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** Production service and Paimon catalog in fresh JVMs, with a file-backed substitute for node KV. */
public class PaimonStartupRecoveryIntegrationTest {
    private static final String[] TABLES = {"orders_a", "orders_b"};
    @TempDir Path directory;

    @Test
    void silentRestartPublishesEveryAcknowledgedTableAndSecondRestartDoesNotRepublish() throws Exception {
        runChild("crash", 42);
        assertTables(false);
        runChild("recover", 0);
        assertTables(true);
        runChild("recover", 0);
        assertTables(true);
    }

    @Test
    void gracefulStopDuringFullLoadPreservesPreparedRowsWithoutPublishing() throws Exception {
        runChild("stop", 0);
        assertTables(false);
        runChild("recover", 0);
        assertTables(true);
    }

    @Test
    void startupRefusesTruncatedPreparedFileBeforeAcceptingTraffic() throws Exception {
        runChild("crash", 42);
        try (java.util.stream.Stream<Path> files = Files.walk(directory.resolve("warehouse/default.db/orders_a"))) {
            Path data = files.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().startsWith("data-"))
                    .findFirst().get();
            Files.write(data, new byte[0]);
        }
        runChild("reject", 0);
        try (Catalog catalog = catalog(directory)) {
            assertNull(physical(catalog, TABLES[0]).snapshotManager().latestSnapshotIdFromFileSystem());
        }
    }

    @Test
    void replayOfKeyChangesAndDeletesConvergesAfterEvictingTheFieldCache() throws Exception {
        runChild("crash", 42);
        runChild("cdc", 0);
        runChild("cdc", 0);
        try (Catalog catalog = catalog(directory)) {
            for (String name : TABLES) {
                assertEquals(Collections.singletonMap(3, "updated"), contents(physical(catalog, name)));
            }
        }
    }

    private void runChild(String mode, int expected) throws Exception {
        Path log = directory.resolve(mode + ".log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/java.lang=ALL-UNNAMED", "-cp", classpath, getClass().getName(), directory.toString(), mode)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(child.waitFor(90, TimeUnit.SECONDS), "child timed out: " + mode);
            assertEquals(expected, child.exitValue(), () -> {
                try { return new String(Files.readAllBytes(log), StandardCharsets.UTF_8); }
                catch (Exception failure) { return failure.toString(); }
            });
        } finally {
            if (child.isAlive()) { child.destroyForcibly(); }
        }
    }

    public static void main(String[] args) throws Exception {
        Path base = Paths.get(args[0]);
        String mode = args[1];
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("local");
        config.setDatabase("default");
        config.setWarehouse(Files.createDirectories(base.resolve("warehouse")).toString());
        config.setDiskTmpDir(Files.createDirectories(base.resolve("spill")).toString());
        config.setBucketMode("fixed");
        config.setBucketCount(1);
        config.setEnableAutoCompaction(false);
        config.setWriteBufferSize(8);
        config.setEnableDeduplicateUpdateAfterCompletenessCheck(true);
        config.setEnablePrimaryKeyUpdate(true);
        Map<String, Object> node = new HashMap<>();
        node.put("localRecoveryDir", base.resolve("recovery").toString());
        node.put("cdcCommitTrigger", "timeOrRows");
        config.loadNodeConfig(node);
        config.setBatchAccumulationSize(2);
        Log log = mock(Log.class);
        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getLog()).thenReturn(log);
        KVMap<Object> state = taskState(base.resolve("task.properties"));
        when(context.getStateMap()).thenReturn(state);
        PaimonService service = new PaimonService(config, log);
        service.setFlushOffsetCallback(offset -> { throw new AssertionError("Invented source callback: " + offset); });
        service.init();
        if ("reject".equals(mode)) {
            Exception failure = assertThrows(Exception.class, () -> service.startLocalRecovery(context));
            StringBuilder causes = new StringBuilder();
            for (Throwable t = failure; t != null; t = t.getCause()) { causes.append(t).append('\n'); }
            assertTrue(causes.toString().contains("length"), causes.toString());
            return;
        }
        service.startLocalRecovery(context);
        if ("cdc".equals(mode)) {
            // Open the original writer and then overflow the ten-table field cache before changing its key.
            service.afterInitialSync(context, mappedTable(TABLES[0]));
            for (int i = 0; i < 10; i++) {
                TapTable extra = mappedTable("extra_" + i);
                service.createTable(extra);
                service.afterInitialSync(context, extra);
            }
            for (String name : TABLES) {
                TapTable schema = mappedTable(name);
                TapRecordEvent update = new TapUpdateRecordEvent().init().table(name)
                        .before(row(1, name + "1")).after(row(3, "updated"));
                TapRecordEvent delete = new TapDeleteRecordEvent().init().table(name).before(row(2, name + "2"));
                for (TapRecordEvent event : Arrays.asList(update, delete)) {
                    event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
                    event.addInfo(TapCallbackOffset.KEY_NODE_IDS, Collections.singletonList("source"));
                    event.addInfo(TapCallbackOffset.KEY_STREAM_OFFSET, "P2");
                    event.addInfo(TapCallbackOffset.KEY_SOURCE_TIME, 2L);
                }
                service.writeRecords(Arrays.asList(update, delete), schema, context);
            }
        } else if (!"recover".equals(mode)) {
            for (String name : TABLES) {
                TapTable table = mappedTable(name);
                service.createTable(table);
                for (int id : Arrays.asList(1, 2)) {
                    TapInsertRecordEvent row = new TapInsertRecordEvent().init().table(name).after(row(id, name + id));
                    row.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
                    row.addInfo(TapCallbackOffset.KEY_NODE_IDS, Collections.singletonList("source"));
                    service.writeRecords(Collections.singletonList(row), table, context);
                }
            }
            if ("crash".equals(mode)) { Runtime.getRuntime().halt(42); }
        }
        service.close();
    }

    @SuppressWarnings("unchecked")
    public static KVMap<Object> taskState(Path file) throws Exception {
        Properties data = new Properties();
        if (Files.exists(file)) {
            try (InputStream in = Files.newInputStream(file)) { data.load(in); }
        }
        KVMap<Object> map = mock(KVMap.class);
        when(map.get(anyString())).thenAnswer(call -> data.get(call.getArgument(0)));
        when(map.putIfAbsent(anyString(), any())).thenAnswer(call -> {
            Object previous = data.putIfAbsent(call.getArgument(0), call.getArgument(1));
            if (previous == null) { save(data, file); }
            return previous;
        });
        doAnswer(call -> {
            data.put(call.getArgument(0), call.getArgument(1));
            save(data, file);
            return null;
        }).when(map).put(anyString(), any());
        return map;
    }

    private static Map<String, Object> row(int id, String value) {
        Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("value", value);
        return values;
    }

    private static TapTable mappedTable(String name) {
        return new TapTable(name).add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("value", "STRING"));
    }

    private static Map<Integer, String> contents(FileStoreTable table) throws Exception {
        ReadBuilder read = table.newReadBuilder();
        Map<Integer, String> rows = new HashMap<>();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> assertNull(rows.put(row.getInt(0), row.getString(1).toString())));
        }
        return rows;
    }

    private static void save(Properties data, Path file) throws Exception {
        try (OutputStream out = Files.newOutputStream(file)) { data.store(out, null); }
    }

    private static Catalog catalog(Path base) {
        return CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(
                Collections.singletonMap("warehouse", base.resolve("warehouse").toUri().toString()))));
    }

    private static FileStoreTable physical(Catalog catalog, String name) throws Exception {
        return (FileStoreTable) catalog.getTable(Identifier.create("default", name));
    }

    private void assertTables(boolean published) throws Exception {
        try (Catalog catalog = catalog(directory)) {
            for (String name : TABLES) {
                FileStoreTable table = physical(catalog, name);
                assertEquals(published ? Long.valueOf(1) : null, table.snapshotManager().latestSnapshotIdFromFileSystem());
                if (!published) { continue; }
                Map<Integer, String> rows = contents(table);
                assertEquals(2, rows.size());
                assertEquals(name + "1", rows.get(1));
                assertEquals(name + "2", rows.get(2));
            }
        }
    }
}
