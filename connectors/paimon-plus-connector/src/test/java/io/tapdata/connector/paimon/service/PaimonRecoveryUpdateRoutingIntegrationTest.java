package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaimonRecoveryUpdateRoutingIntegrationTest {
    @TempDir Path directory;

    @ParameterizedTest
    @CsvSource({"HASH_FIXED,false", "HASH_FIXED,true", "HASH_DYNAMIC,false", "HASH_DYNAMIC,true",
            "KEY_DYNAMIC,false", "KEY_DYNAMIC,true"})
    void retractsKnownOldKeysAndRefusesAnUnknownOldKey(BucketMode mode, boolean detectKeyChanges) throws Exception {
        run(mode, detectKeyChanges, true);
    }

    @Test
    void ordinaryModeStillAcceptsAnAfterOnlyUpdate() throws Exception {
        run(BucketMode.HASH_FIXED, false, false);
    }

    private void run(BucketMode mode, boolean detectKeyChanges, boolean recovery) throws Exception {
        boolean globalIndex = mode == BucketMode.KEY_DYNAMIC;
        TapTable model = new TapTable("events")
                .add(new TapField("part", "INT").primaryKeyPos(globalIndex ? null : 1))
                .add(new TapField("id", "INT").primaryKeyPos(globalIndex ? 1 : 2))
                .add(new TapField("v", "STRING"));
        Path warehouse = Files.createDirectories(directory.resolve("warehouse"));
        Log log = mock(Log.class);
        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getLog()).thenReturn(log);
        doReturn(PaimonStartupRecoveryIntegrationTest.taskState(directory.resolve("kv"))).when(context).getStateMap();

        PaimonConfig config = new PaimonConfig();
        config.setStorageType("local");
        config.setWarehouse(warehouse.toString());
        config.setDatabase("default");
        config.setDiskTmpDir(Files.createDirectories(directory.resolve("spill")).toString());
        config.setPartitionKey(List.of("part"));
        config.setBucketMode(mode == BucketMode.HASH_FIXED ? "fixed" : "dynamic");
        config.setBucketCount(2);
        config.setEnableAutoCompaction(false);
        if (recovery) {
            config.loadNodeConfig(Map.of("localRecoveryDir", directory.resolve("state").toString(),
                    "cdcCommitTrigger", "timeOrRows"));
        }
        if (detectKeyChanges) {
            config.setEnablePrimaryKeyUpdate(true);
        }
        config.setBatchAccumulationSize(1);
        PaimonService service = new PaimonService(config, log);
        PaimonFatalWriteException expected = null;
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(
                Options.fromMap(Map.of("warehouse", warehouse.toUri().toString()))))) {
            service.setFlushOffsetCallback(ignored -> fail("No heartbeat submitted"));
            service.init();
            service.startLocalRecovery(context);
            service.createTable(model);
            FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("default", "events"));
            assertEquals(mode, table.bucketMode());
            TapRecordEvent seed = new TapInsertRecordEvent().init().table("events").after(row(1, 7));
            seed.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
            seed.addInfo(TapCallbackOffset.KEY_NODE_IDS, List.of("src"));
            service.writeRecords(List.of(seed), model, context);
            service.afterInitialSync(context, model);
            assertRows(table, "1/7=payload");

            if (!recovery) {
                service.writeRecords(List.of(update(null, Map.of("part", 1, "id", 7, "v", "changed"))), model, context);
                assertRows(table, "1/7=changed");
                return;
            }
            Map<String, Object> oldKey = globalIndex ? Map.of("id", 7) : Map.of("part", 1, "id", 7);
            service.writeRecords(List.of(update(oldKey, row(2, 8))), model, context);
            assertRows(table, "2/8=payload");
            Map<String, Object> oldLocation = globalIndex ? Map.of("id", 8) : Map.of("part", 2, "id", 8);
            service.writeRecords(List.of(update(oldLocation, row(3, 8))), model, context);
            assertRows(table, "3/8=payload");

            Long beforeRejection = table.snapshotManager().latestSnapshotIdFromFileSystem();
            expected = assertThrows(PaimonFatalWriteException.class, () -> service.writeRecords(
                    List.of(update(row(3, 8), row(3, 10)), update(null, row(4, 9))), model, context));
            assertTrue(expected.getMessage().contains("PAIMON_LOCAL_RECOVERY_MISSING_OLD_KEY"), expected::getMessage);
            assertEquals(beforeRejection, table.snapshotManager().latestSnapshotIdFromFileSystem());
            assertRows(table, "3/8=payload");
        } finally {
            try {
                service.close();
            } catch (Exception failure) {
                if (failure != expected) {
                    throw failure;
                }
            }
        }
    }

    private static Map<String, Object> row(int part, int id) {
        return Map.of("part", part, "id", id, "v", "payload");
    }

    private static TapUpdateRecordEvent update(Map<String, Object> before, Map<String, Object> after) {
        TapUpdateRecordEvent event = new TapUpdateRecordEvent().init().table("events").before(before).after(after);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo(TapCallbackOffset.KEY_NODE_IDS, List.of("src"));
        event.addInfo(TapCallbackOffset.KEY_STREAM_OFFSET, "P2");
        event.addInfo(TapCallbackOffset.KEY_SOURCE_TIME, 2L);
        return event;
    }

    private static void assertRows(FileStoreTable table, String expected) throws Exception {
        List<String> rows = new ArrayList<>();
        ReadBuilder read = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0) + "/" + row.getInt(1) + "=" + row.getString(2)));
        }
        assertEquals(List.of(expected), rows);
    }
}
