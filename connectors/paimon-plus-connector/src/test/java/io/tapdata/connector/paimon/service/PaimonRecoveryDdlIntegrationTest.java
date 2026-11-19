package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaimonRecoveryDdlIntegrationTest {
    private static final Identifier ORDERS = Identifier.create("default", "orders");
    private static final String TABLE = ORDERS.getObjectName();

    @TempDir Path directory;
    private final TapTable mapped = new TapTable(TABLE)
            .add(new TapField("id", "INT").primaryKeyPos(1)).add(new TapField("value", "STRING"));
    private final TapConnectorContext context = mock(TapConnectorContext.class);

    @ParameterizedTest
    @ValueSource(strings = {"clear", "drop"})
    void destructiveDdlLeavesRegisteredPhysicalTableAndPendingBatchIntact(String operation) throws Exception {
        PaimonService service = initializedService();
        try (Catalog catalog = catalog()) {
            service.startLocalRecovery(context);
            assertTrue(service.createTable(mapped));
            FileStoreTable original = physical(catalog);
            appendNative(original, "seed-writer", 1, "v1");
            String uuid = original.uuid();
            String commitUser = original.snapshotManager().latestSnapshot().commitUser();

            insert(service, 2, true);
            assertEquals(1L, preparedFiles());
            Exception failure = assertThrows(Exception.class, () -> {
                if (operation.equals("clear")) {
                    service.clearTable(TABLE);
                } else {
                    service.dropTable(TABLE);
                }
            });
            assertTrue(failure.getMessage().contains("controlled new generation"), failure::toString);

            catalog.invalidateTable(ORDERS);
            FileStoreTable current = physical(catalog);
            assertEquals(uuid, current.uuid());
            assertEquals(Long.valueOf(1), current.snapshotManager().latestSnapshotIdFromFileSystem());
            assertEquals(commitUser, current.snapshotManager().latestSnapshot().commitUser());
            assertEquals(Collections.singletonMap(1, "v1"), rows(current));
            assertEquals(1L, preparedFiles());
        } finally {
            closeAfterRejection(service, "controlled new generation");
        }
    }

    @Test
    void silentRestartRejectsExternalRollbackAndReusedSnapshotIdAfterLocalGc() throws Exception {
        try (PaimonService writer = initializedService()) {
            writer.startLocalRecovery(context);
            createAndPublishFirstRow(writer);
            insert(writer, 2, false);
        }
        assertEquals(0L, preparedFiles());
        try (Catalog catalog = catalog()) {
            FileStoreTable table = physical(catalog);
            assertEquals(Long.valueOf(2), table.snapshotManager().latestSnapshotIdFromFileSystem());
            assertEquals(Map.of(1, "v1", 2, "v2"), rows(table));
            String uuid = table.uuid();
            long schemaId = table.schema().id();
            String writer = table.snapshotManager().latestSnapshot().commitUser();

            table.rollbackTo(1);
            appendNative(table, "external-writer", 3, "fork");
            catalog.invalidateTable(ORDERS);
            FileStoreTable forked = physical(catalog);
            assertEquals(uuid, forked.uuid());
            assertEquals(schemaId, forked.schema().id());
            assertEquals(Long.valueOf(2), forked.snapshotManager().latestSnapshotIdFromFileSystem());
            assertNotEquals(writer, forked.snapshotManager().latestSnapshot().commitUser());
            assertEquals(Map.of(1, "v1", 3, "fork"), rows(forked));

            PaimonService restarted = initializedService();
            try {
                Exception failure = assertThrows(Exception.class, () -> restarted.startLocalRecovery(context));
                assertTrue(failure.getMessage().contains("diverged from the confirmed snapshot"), failure::toString);
            } finally {
                closeAfterRejection(restarted, "diverged from the confirmed snapshot");
            }
            assertEquals(Long.valueOf(2), forked.snapshotManager().latestSnapshotIdFromFileSystem());
            assertEquals(Map.of(1, "v1", 3, "fork"), rows(forked));
        }
    }

    @Test
    void normalCreateTablePreflightRejectsMappedPrimaryKeyDriftOnExistingTable() throws Exception {
        PaimonService service = initializedService();
        try (Catalog catalog = catalog()) {
            service.startLocalRecovery(context);
            createAndPublishFirstRow(service);
            TapTable changed = new TapTable(TABLE).add(new TapField("id", "INT"))
                    .add(new TapField("value", "STRING").primaryKeyPos(1));
            PaimonFatalWriteException failure = assertThrows(PaimonFatalWriteException.class,
                    () -> service.createTable(changed));
            assertTrue(failure.getMessage().contains("primary key must match"), failure::toString);
            FileStoreTable current = physical(catalog);
            assertEquals(Collections.singletonList("id"), current.schema().primaryKeys());
            assertEquals(Long.valueOf(1), current.snapshotManager().latestSnapshotIdFromFileSystem());
            assertEquals(Collections.singletonMap(1, "v1"), rows(current));
        } finally {
            closeAfterRejection(service, "primary key must match");
        }
    }

    private PaimonService initializedService() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("local");
        config.setDatabase("default");
        config.setWarehouse(Files.createDirectories(directory.resolve("warehouse")).toString());
        config.setDiskTmpDir(Files.createDirectories(directory.resolve("spill")).toString());
        config.setBucketMode("fixed");
        config.setBucketCount(1);
        config.setEnableAutoCompaction(false);
        config.setWriteBufferSize(8);
        config.setEnableDeduplicateUpdateAfterCompletenessCheck(true);
        Map<String, Object> node = new HashMap<>();
        node.put("localRecoveryDir", directory.resolve("recovery").toString());
        node.put("cdcCommitTrigger", "timeOrRows");
        config.loadNodeConfig(node);
        config.setBatchAccumulationSize(1);

        Log log = mock(Log.class);
        when(context.getLog()).thenReturn(log);
        KVMap<Object> nodeState = PaimonStartupRecoveryIntegrationTest.taskState(directory.resolve("task.properties"));
        when(context.getStateMap()).thenReturn(nodeState);
        PaimonService service = new PaimonService(config, log);
        service.setFlushOffsetCallback(offset -> fail("Unexpected callback without a heartbeat: " + offset));
        service.init();
        return service;
    }

    private void createAndPublishFirstRow(PaimonService service) throws Exception {
        assertTrue(service.createTable(mapped));
        insert(service, 1, true);
        service.afterInitialSync(context, mapped);
        assertEquals(0L, preparedFiles());
    }

    private void insert(PaimonService service, int id, boolean initial) throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("id", id);
        data.put("value", "v" + id);
        TapInsertRecordEvent event = new TapInsertRecordEvent().init().table(TABLE).after(data);
        // PDK delivers stage names using these literals. A CDC batch also carries its source offset.
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, initial ? "INITIAL_SYNC" : "CDC");
        event.addInfo(TapCallbackOffset.KEY_NODE_IDS, Collections.singletonList("source"));
        if (!initial) {
            event.addInfo(TapCallbackOffset.KEY_STREAM_OFFSET, "P" + id);
            event.addInfo(TapCallbackOffset.KEY_SOURCE_TIME, (long) id);
        }
        service.writeRecords(Collections.singletonList(event), mapped, context);
    }

    private long preparedFiles() throws Exception {
        try (Stream<Path> files = Files.walk(directory.resolve("recovery"))) {
            return files.filter(path -> path.getFileName().toString().startsWith("prepare-")).count();
        }
    }

    private Catalog catalog() {
        return CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(
                Collections.singletonMap("warehouse", directory.resolve("warehouse").toUri().toString()))));
    }

    private static FileStoreTable physical(Catalog catalog) throws Exception {
        return (FileStoreTable) catalog.getTable(ORDERS);
    }

    private static void appendNative(FileStoreTable table, String user, int id, String value) throws Exception {
        StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(user);
        try (StreamTableWrite write = builder.newWrite(); StreamTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(value)));
            commit.commit(1, write.prepareCommit(true, 1));
        }
    }

    private static Map<Integer, String> rows(FileStoreTable table) throws Exception {
        Map<Integer, String> result = new HashMap<>();
        ReadBuilder read = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> assertNull(result.put(row.getInt(0), row.getString(1).toString())));
        }
        return result;
    }

    private static void closeAfterRejection(PaimonService service, String expected) throws Exception {
        try {
            service.close();
        } catch (Exception stickyFailure) {
            assertTrue(stickyFailure.getMessage().contains(expected), stickyFailure::toString);
        }
    }
}
