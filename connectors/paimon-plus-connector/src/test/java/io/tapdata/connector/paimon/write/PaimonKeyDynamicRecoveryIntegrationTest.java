package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.connector.paimon.schema.PaimonRecoveryAdmission;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class PaimonKeyDynamicRecoveryIntegrationTest {
    @TempDir Path root;

    @Test
    void rebuiltIndexMovesAndDeletesKeysAcrossPartitionsAfterPreparedWriterCrash() throws Exception {
        try (var fixture = new DynamicRecoveryFixture(root, true, Map.of())) {
            assertEquals(List.of("3@1=30", "4@3=44", "5@4=55", "6@6=60", "7@7=70"),
                    fixture.run(DynamicRecoveryFixture.Scenario.ORDERED));
        }
    }

    @Test
    void finiteTtlRejectedBeforeFirstWrite() throws Exception {
        try (var fixture = new DynamicRecoveryFixture(root, true, Map.of("cross-partition-upsert.index-ttl", "1 ms"))) {
            var table = fixture.table();
            var failure = assertThrows(PaimonFatalWriteException.class,
                    () -> PaimonRecoveryAdmission.validateTable(fixture.config, "events", table));
            assertTrue(failure.getMessage().contains("cross-partition-upsert.index-ttl"));
            assertNull(table.snapshotManager().latestSnapshotId());
        }
    }

    @Test
    void nativeFiniteTtlForgetsKeysThatStillExistInOlderPartitions() throws Exception {
        try (var fixture = new DynamicRecoveryFixture(root, true, Map.of("cross-partition-upsert.index-ttl", "1 ms"))) {
            List<String> rows = fixture.run(DynamicRecoveryFixture.Scenario.EXPIRED_CROSS_PARTITION_INDEX);
            assertTrue(rows.containsAll(List.of("4@1=40", "4@3=44", "5@2=50", "5@4=55")), rows::toString);
        }
    }

    @Test
    void paimonRejectsSequenceFieldForCrossPartitionUpdatesDuringSchemaValidation() {
        Schema schema = Schema.newBuilder().column("k", DataTypes.INT()).column("p", DataTypes.INT())
                .column("version", DataTypes.INT()).primaryKey("k").partitionKeys("p")
                .option("bucket", "-1").option("sequence.field", "version").build();
        var error = assertThrows(IllegalArgumentException.class,
                () -> SchemaValidation.validateTableSchema(TableSchema.create(0, schema)));
        assertTrue(error.getMessage().contains("sequence.field in cross partition update"), error::getMessage);
    }
}
