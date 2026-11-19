package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class PaimonRecoveryTimeKeyIntegrationTest {
    @TempDir Path directory;

    @ParameterizedTest
    @CsvSource({"fixed,insert", "fixed,update", "fixed,delete", "dynamic,insert", "dynamic,update", "dynamic,delete"})
    void submillisecondKeyCannotOverwriteOrDeleteItsMillisecondNeighbour(String bucket, String operation) throws Throwable {
        verify(directory, bucket, operation);
    }

    @Test
    void ordinaryStartedTargetKeepsTheLegacyIntegerTimeCodec() throws Throwable {
        Path warehouse = Files.createDirectories(directory.resolve("warehouse"));
        TapConnectorContext context = mock(TapConnectorContext.class);
        doReturn(mock(Log.class)).when(context).getLog();
        doReturn(PaimonStartupRecoveryIntegrationTest.taskState(directory.resolve("kv"))).when(context).getStateMap();
        when(context.getFlushOffsetCallback()).thenReturn(ignored -> fail("No heartbeat was submitted"));
        when(context.getConnectionConfig()).thenReturn(DataMap.create().kv("warehouse", warehouse.toString())
                .kv("database", "default").kv("diskTmpDir", Files.createDirectories(directory.resolve("spill")).toString()));
        TapCodecsRegistry registry = new TapCodecsRegistry();
        PaimonConnector connector = new PaimonConnector();
        connector.registerCapabilities(new ConnectorFunctions(), registry);
        connector.onStart(context);
        try {
            Map<String, Object> converted = row(new TapCodecsFilterManager(registry), "12:34:56.123456");
            assertEquals(45_296_123, converted.get("id"));
            assertEquals(3_723_456, converted.get("clock"));
        } finally {
            connector.onStop(context);
        }
    }

    public static void verify(Path base, String bucket, String operation) throws Throwable {
        TapTable model = new TapTable("events").add(new TapField("id", "TIME(3)").primaryKeyPos(1))
                .add(new TapField("clock", "TIME(3)"));
        Path warehouse = Files.createDirectories(base.resolve("warehouse"));
        TapConnectorContext context = mock(TapConnectorContext.class);
        doReturn(mock(Log.class)).when(context).getLog();
        doReturn(PaimonStartupRecoveryIntegrationTest.taskState(base.resolve("kv"))).when(context).getStateMap();
        when(context.getFlushOffsetCallback()).thenReturn(ignored -> fail("No heartbeat was submitted"));
        when(context.getConnectionConfig()).thenReturn(DataMap.create().kv("warehouse", warehouse.toString())
                .kv("database", "default").kv("diskTmpDir", Files.createDirectories(base.resolve("spill")).toString())
                .kv("bucketMode", bucket).kv("bucketCount", 1).kv("enableAutoCompaction", false));
        when(context.getNodeConfig()).thenReturn(DataMap.create().kv("localRecoveryDir", base.resolve("state").toString())
                .kv("cdcCommitTrigger", "timeOrRows").kv("batchAccumulationSize", 1).kv("enablePrimaryKeyUpdate", true));
        TapCodecsRegistry registry = new TapCodecsRegistry();
        ConnectorFunctions functions = new ConnectorFunctions();
        PaimonConnector connector = new PaimonConnector();
        connector.registerCapabilities(functions, registry);
        TapCodecsFilterManager codecs = new TapCodecsFilterManager(registry);
        connector.onStart(context);
        PaimonFatalWriteException expected = null;
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(
                Options.fromMap(Map.of("warehouse", warehouse.toUri().toString()))))) {
            Map<String, Object> document = new HashMap<>();
            document.put("clock", new TapTimeValue(DateTime.withTimeStr("01:02:03.456789")));
            Map<String, Object> jsonRow = new HashMap<>();
            jsonRow.put("document", new TapMapValue(document));
            codecs.transformFromTapValueMap(jsonRow);
            assertEquals("{\"clock\":3723456}", jsonRow.get("document"));
            functions.getCreateTableV2Function().createTable(context, new TapCreateTableEvent().table(model));
            Map<String, Object> aligned = row(codecs, "12:34:56.123000");
            TapRecordEvent seed = new TapInsertRecordEvent().init().table("events").after(aligned);
            seed.addInfo(TapCallbackOffset.KEY_NODE_IDS, List.of("source"));
            seed.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
            functions.getWriteRecordFunction().writeRecord(context, List.of(seed), model, ignored -> {});
            functions.getAfterInitialSyncFunction().afterInitialSync(context, model);
            FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("default", "events"));
            assertSeed(table);

            Map<String, Object> finer = row(codecs, "12:34:56.123456");
            TapRecordEvent change;
            switch (operation) {
                case "insert":
                    change = new TapInsertRecordEvent().init().table("events").after(finer);
                    break;
                case "update":
                    change = new TapUpdateRecordEvent().init().table("events").before(finer)
                            .after(row(codecs, "12:34:56.124000"));
                    break;
                case "delete":
                    change = new TapDeleteRecordEvent().init().table("events").before(finer);
                    break;
                default:
                    throw new IllegalArgumentException(operation);
            }
            change.addInfo(TapCallbackOffset.KEY_NODE_IDS, List.of("source"));
            change.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
            change.addInfo(TapCallbackOffset.KEY_STREAM_OFFSET, "P1");
            change.addInfo(TapCallbackOffset.KEY_SOURCE_TIME, 1L);
            expected = assertThrows(PaimonFatalWriteException.class, () -> functions.getWriteRecordFunction()
                    .writeRecord(context, List.of(change), model, ignored -> fail("Unsafe key acknowledged")));
            assertTrue(expected.getMessage().contains("TIME primary key"), expected::getMessage);
            assertSeed(table);
        } finally {
            try {
                connector.onStop(context);
            } catch (Throwable closeFailure) {
                if (expected == null || closeFailure != expected) {
                    throw closeFailure;
                }
            }
        }
    }

    private static Map<String, Object> row(TapCodecsFilterManager codecs, String key) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", new TapTimeValue(DateTime.withTimeStr(key)));
        row.put("clock", new TapTimeValue(DateTime.withTimeStr("01:02:03.456789")));
        codecs.transformFromTapValueMap(row);
        return row;
    }

    private static void assertSeed(FileStoreTable table) throws Exception {
        assertEquals(1L, table.snapshotManager().latestSnapshotIdFromFileSystem());
        List<List<Integer>> rows = new ArrayList<>();
        ReadBuilder read = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(List.of(row.getInt(0), row.getInt(1))));
        }
        assertEquals(List.of(List.of(45_296_123, 3_723_456)), rows);
    }
}
