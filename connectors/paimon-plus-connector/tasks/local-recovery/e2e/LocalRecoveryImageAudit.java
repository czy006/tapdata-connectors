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
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Opt-in audit. A failed comparison means the connector acknowledged a different materialized state. */
public final class LocalRecoveryImageAudit {
    private static final Identifier ORDERS = Identifier.create("default", "orders");

    private enum Case {
        NULL_CONTROL(null),
        UPDATE_NULL_WITH_TARGET_DEFAULT(null),
        NULL_WITH_TARGET_DEFAULT("PAIMON_LOCAL_RECOVERY_DEFAULTED_NULL"),
        MISSING_TARGET_COLUMN("mapped field is missing from the target: payload"),
        LOSSY_TARGET_PRIMARY_KEY("mapped field id has type INT but the target has FLOAT");

        final String expectedRejection;
        Case(String expectedRejection) { this.expectedRejection = expectedRejection; }
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        int mismatches = 0;
        for (Case test : Case.values()) {
            Path dir = Files.createDirectories(root.resolve(test.name()));
            try {
                if (!check(test, dir)) { mismatches++; }
            } catch (RuntimeException failure) {
                // close() may surface the same sticky fatal error again through try-with-resources.
                Throwable rejection = failure;
                while (rejection != null && !(rejection instanceof PaimonFatalWriteException)) {
                    rejection = rejection.getCause();
                }
                if (rejection == null || test.expectedRejection == null
                        || !rejection.getMessage().contains(test.expectedRejection)) {
                    throw failure;
                }
                try (Catalog catalog = open(dir.resolve("warehouse"))) {
                    FileStoreTable table = (FileStoreTable) catalog.getTable(ORDERS);
                    if (table.snapshotManager().latestSnapshotIdFromFileSystem() != null) { throw failure; }
                    System.out.println("REJECT " + test + " snapshot=none error=" + rejection.getMessage());
                }
            }
        }
        if (mismatches != 0) {
            throw new AssertionError(mismatches + " acknowledged source/target mismatches");
        }
    }

    private static boolean check(Case test, Path dir) throws Exception {
        Path warehouse = Files.createDirectories(dir.resolve("warehouse"));
        Map<Integer, Map<String, String>> expected = new LinkedHashMap<>();
        if (test == Case.LOSSY_TARGET_PRIMARY_KEY) {
            expected.put(16_777_216, Collections.singletonMap("payload", "first"));
            expected.put(16_777_217, Collections.singletonMap("payload", "second"));
        } else {
            expected.put(7, Collections.singletonMap("payload", test == Case.MISSING_TARGET_COLUMN ? "source-value" : null));
        }
        TapTable mapped = new TapTable(ORDERS.getObjectName())
                .add(new TapField("id", "INT").primaryKeyPos(1)).add(new TapField("payload", "STRING"));
        Schema.Builder schema = Schema.newBuilder()
                .column("id", test == Case.LOSSY_TARGET_PRIMARY_KEY ? DataTypes.FLOAT() : DataTypes.INT());
        if (test != Case.MISSING_TARGET_COLUMN) {
            schema.column("payload", DataTypes.STRING(), null, (test == Case.NULL_WITH_TARGET_DEFAULT || test == Case.UPDATE_NULL_WITH_TARGET_DEFAULT) ? "DEFAULTED" : null);
        }
        schema.primaryKey("id").option("bucket", "1").option("write-only", "true");
        Log log = mock(Log.class);
        TapConnectorContext ctx = mock(TapConnectorContext.class);
        when(ctx.getLog()).thenReturn(log);
        KVMap<Object> state = PaimonStartupRecoveryIntegrationTest.taskState(dir.resolve("task.properties"));
        when(ctx.getStateMap()).thenReturn(state);
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("local");
        config.setWarehouse(warehouse.toString());
        config.setDatabase("default");
        config.setDiskTmpDir(Files.createDirectories(dir.resolve("spill")).toString());
        config.setBucketMode("fixed");
        config.setBucketCount(1);
        config.setEnableAutoCompaction(false);
        config.setEnableDeduplicateUpdateAfterCompletenessCheck(true);
        config.loadNodeConfig(Map.of("localRecoveryDir", dir.resolve("recovery").toString(), "cdcCommitTrigger", "timeOrRows"));
        config.setBatchAccumulationSize(1);
        boolean verifiedRejection = false;
        try (Catalog catalog = open(warehouse); PaimonService service = new PaimonService(config, log)) {
            catalog.createDatabase("default", true);
            catalog.createTable(ORDERS, schema.build(), false);
            service.setFlushOffsetCallback(offset -> { throw new AssertionError("Unexpected heartbeat"); });
            service.init();
            service.startLocalRecovery(ctx);
            service.createTable(mapped);
            List<TapRecordEvent> batch = new ArrayList<>();
            for (Map.Entry<Integer, Map<String, String>> entry : expected.entrySet()) {
                Map<String, Object> values = new LinkedHashMap<>(entry.getValue());
                values.put("id", entry.getKey());
                if (test == Case.UPDATE_NULL_WITH_TARGET_DEFAULT) { values.put("payload", "initial"); }
                TapInsertRecordEvent row = new TapInsertRecordEvent().init().table(ORDERS.getObjectName()).after(values);
                row.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
                row.addInfo(TapCallbackOffset.KEY_NODE_IDS, Collections.singletonList("source"));
                batch.add(row);
            }
            service.writeRecords(batch, mapped, ctx);
            service.afterInitialSync(ctx, mapped);
            FileStoreTable table = (FileStoreTable) catalog.getTable(ORDERS);
            if (test == Case.UPDATE_NULL_WITH_TARGET_DEFAULT) {
                Map<String, Object> after = new LinkedHashMap<>();
                after.put("id", 7);
                after.put("payload", null);
                TapUpdateRecordEvent update = new TapUpdateRecordEvent().init().table(ORDERS.getObjectName())
                        .before(Map.of("id", 7, "payload", "initial")).after(after);
                update.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
                update.addInfo(TapCallbackOffset.KEY_NODE_IDS, Collections.singletonList("source"));
                update.addInfo(TapCallbackOffset.KEY_STREAM_OFFSET, "P2");
                update.addInfo(TapCallbackOffset.KEY_SOURCE_TIME, 2L);
                try {
                    service.writeRecords(Collections.singletonList(update), mapped, ctx);
                } catch (RuntimeException rejection) {
                    if (!rejectsNullDefault(rejection)) { throw rejection; }
                    Map<Integer, Map<String, String>> before = Map.of(7, Map.of("payload", "initial"));
                    if (!Long.valueOf(1).equals(table.snapshotManager().latestSnapshotIdFromFileSystem())
                            || !before.equals(readRows(table, test))) {
                        throw new AssertionError("Rejected update changed the target", rejection);
                    }
                    verifiedRejection = true;
                    System.out.println("REJECT " + test + " snapshot=1 target=" + before
                            + " error=PAIMON_DEDUPLICATE_INCOMPLETE_UPDATE_AFTER");
                    return true;
                }
            }
            Map<Integer, Map<String, String>> actual = readRows(table, test);
            boolean matches = expected.equals(actual);
            System.out.println((matches ? "MATCH " : "MISMATCH ") + test + " snapshot="
                    + table.snapshotManager().latestSnapshotIdFromFileSystem() + " source=" + expected + " target=" + actual);
            return matches;
        } catch (Exception stopFailure) {
            if (verifiedRejection && rejectsNullDefault(stopFailure)) { return true; }
            throw stopFailure;
        }
    }

    private static Catalog open(Path warehouse) {
        return CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(
                Collections.singletonMap("warehouse", warehouse.toUri().toString()))));
    }

    private static boolean rejectsNullDefault(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            String message = t.getMessage();
            if (message != null && message.contains("PAIMON_DEDUPLICATE_INCOMPLETE_UPDATE_AFTER")
                    && message.contains("defaultedNullFields=[payload]")) {
                return true;
            }
        }
        return false;
    }

    private static Map<Integer, Map<String, String>> readRows(FileStoreTable table, Case test) throws Exception {
        Map<Integer, Map<String, String>> actual = new LinkedHashMap<>();
        ReadBuilder read = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> {
                int key = test == Case.LOSSY_TARGET_PRIMARY_KEY ? (int) row.getFloat(0) : row.getInt(0);
                Map<String, String> payload = row.getFieldCount() == 1 ? Collections.emptyMap()
                        : Collections.singletonMap("payload", row.isNullAt(1) ? null : row.getString(1).toString());
                if (actual.put(key, payload) != null) { throw new AssertionError("Duplicate physical key " + key); }
            });
        }
        return actual;
    }
}
