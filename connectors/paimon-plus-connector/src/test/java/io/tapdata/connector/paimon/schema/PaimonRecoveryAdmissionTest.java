package io.tapdata.connector.paimon.schema;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonRecoveryAdmissionTest {
    private final PaimonConfig config = new PaimonConfig().loadNodeConfig(Map.of("localRecoveryDir", "/state/t1"));

    @ParameterizedTest
    @CsvSource({
            "merge-engine,partial-update,merge-engine",
            "ignore-delete,true,ignore-delete",
            "ignore-update-before,true,ignore-update-before",
            "deletion-vectors.enabled,true,deletion-vectors",
            "row-tracking.enabled,true,row-tracking",
            "changelog-producer,input,changelog-producer",
            "snapshot.num-retained.min,2,snapshot.num-retained.min",
            "sequence.field,value,sequence.field",
            "rowkind.field,value,rowkind.field",
            "record-level.expire-time,1 d,record-level.expire-time",
            "partition.expiration-time,1 d,partition.expiration-time",
            "scan.fallback-branch,history,scan.fallback-branch",
            "scan.mode,from-snapshot-full,scan.mode"
    })
    void excludesTableOptionsWithDifferentBusinessOrPublicationSemantics(String key, String value, String reason) {
        FileStoreTable table = table(BucketMode.HASH_FIXED, schema().primaryKey("id").option(key, value));
        rejected(table, reason);
    }

    @Test
    void rejectsUnverifiableKeyAndBucketModesOnlyWhenEnabled() {
        FileStoreTable append = table(BucketMode.BUCKET_UNAWARE, schema());
        rejected(append, "stable primary key");
        assertDoesNotThrow(() -> PaimonRecoveryAdmission.validateTable(new PaimonConfig(), "events", append));
        rejected(table(BucketMode.POSTPONE_MODE, schema().primaryKey("id")), "bucket mode");
    }

    @Test
    void requiresCompleteBusinessRowsAndRejectsSyntheticHashMapping() {
        config.setEnableDeduplicateUpdateAfterCompletenessCheck(false);
        rejected(table(BucketMode.HASH_FIXED, schema().primaryKey("id")), "CompletenessCheck");
        config.setEnableDeduplicateUpdateAfterCompletenessCheck(true);
        config.setHashKey(true);
        rejected(table(BucketMode.HASH_FIXED, schema().column("_hash_key", DataTypes.STRING()).primaryKey("_hash_key")),
                "_hash_key");
    }

    @Test
    void comparesTheMappedKeyWithTheEffectivePhysicalKey() {
        TapTable input = new TapTable("events").add(new TapField("id", "INT").primaryKeyPos(1));
        FileStoreTable original = table(BucketMode.HASH_FIXED, schema().primaryKey("id"));
        assertDoesNotThrow(() -> PaimonRecoveryAdmission.validateInput(config, input, original));
        FileStoreTable different = table(BucketMode.HASH_FIXED, schema().primaryKey("value"));
        var error = assertThrows(PaimonFatalWriteException.class,
                () -> PaimonRecoveryAdmission.validateInput(config, input, different));
        assertTrue(error.getMessage().contains("mapped TapData primary key"));
    }

    @Test
    void checksEveryMappedFieldAndItsPhysicalTypeAtCreationAndOnTakeover() {
        TapTable input = new TapTable("events").add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("payload", "STRING"));
        Schema.Builder lossy = Schema.newBuilder().column("id", DataTypes.FLOAT())
                .column("payload", DataTypes.STRING()).primaryKey("id");
        Schema.Builder missing = Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id");
        for (Schema.Builder incompatible : new Schema.Builder[]{lossy, missing}) {
            assertThrows(PaimonFatalWriteException.class,
                    () -> PaimonRecoveryAdmission.validateNewTable(config, input, incompatible.build()));
            FileStoreTable existing = table(BucketMode.HASH_DYNAMIC, incompatible);
            assertThrows(PaimonFatalWriteException.class,
                    () -> PaimonRecoveryAdmission.validateInput(config, input, existing));
            assertDoesNotThrow(() -> PaimonRecoveryAdmission.validateInput(new PaimonConfig(), input, existing));
        }
        FileStoreTable wider = table(BucketMode.HASH_FIXED, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .column("low", DataTypes.DOUBLE()).primaryKey("id"));
        TapTable narrow = new TapTable("events").add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("low", "FLOAT"));
        assertDoesNotThrow(() -> PaimonRecoveryAdmission.validateInput(config, narrow, wider));
        Schema reordered = Schema.newBuilder().column("payload", DataTypes.STRING())
                .column("id", DataTypes.INT()).primaryKey("id").build();
        assertDoesNotThrow(() -> PaimonRecoveryAdmission.validateNewTable(config, input, reordered));
    }

    @Test
    void checksTheFinalNewTableSchemaBeforeCatalogCreation() {
        TapTable input = new TapTable("events").add(new TapField("id", "INT").primaryKeyPos(1));
        assertDoesNotThrow(() -> PaimonRecoveryAdmission.validateNewTable(config, input,
                schema().primaryKey("id").option("bucket", "-1").build()));
        assertTrue(assertThrows(PaimonFatalWriteException.class, () ->
                PaimonRecoveryAdmission.validateNewTable(config, input, schema().build()))
                .getMessage().contains("stable primary key"));
        assertTrue(assertThrows(PaimonFatalWriteException.class, () ->
                PaimonRecoveryAdmission.validateNewTable(config, input, schema().primaryKey("id").option("bucket", "-2").build()))
                .getMessage().contains("bucket mode"));
    }

    @Test
    void checksFallbackBeforeCreatingTableAndAllowsAnInactiveOption() {
        TapTable input = new TapTable("events").add(new TapField("id", "INT").primaryKeyPos(1));
        Schema.Builder builder = schema().primaryKey("id");
        PaimonFatalWriteException error = assertThrows(PaimonFatalWriteException.class, () ->
                PaimonRecoveryAdmission.validateNewTable(config, input,
                        builder.option("scan.fallback-branch", "history").build()));
        assertTrue(error.getMessage().contains("scan.fallback-branch"));
        assertDoesNotThrow(() -> PaimonRecoveryAdmission.validateNewTable(config, input,
                builder.option("scan.fallback-branch", "  ").build()));
    }

    private static Schema.Builder schema() {
        return Schema.newBuilder().column("id", DataTypes.INT()).column("value", DataTypes.STRING());
    }

    private static FileStoreTable table(BucketMode mode, Schema.Builder builder) {
        TableSchema schema = TableSchema.create(0, builder.build());
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.bucketMode()).thenReturn(mode);
        when(table.schema()).thenReturn(schema);
        when(table.coreOptions()).thenReturn(CoreOptions.fromMap(schema.options()));
        return table;
    }

    private void rejected(FileStoreTable table, String reason) {
        var error = assertThrows(PaimonFatalWriteException.class,
                () -> PaimonRecoveryAdmission.validateTable(config, "events", table));
        assertTrue(error.getMessage().contains(reason), error::getMessage);
    }
}
