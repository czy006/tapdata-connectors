package io.tapdata.connector.paimon.read;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Paths;
import static org.junit.jupiter.api.Assertions.*;

class PaimonReadPositionIntegrationTest {
    @Test void realSnapshotsRestoreExactlyNextAndCommonTimeExcludesLaterCommit() throws Exception {
        java.nio.file.Path root = Files.createTempDirectory(Files.createDirectories(Paths.get("target/stream-read-validation")), "position-");
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(root.toUri())))) {
            catalog.createDatabase("test", true);
            Identifier id = Identifier.create("test", "orders");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).option("bucket", "-1").build(), false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(id);
            PaimonReadPositionResolver resolver = new PaimonReadPositionResolver(table, "orders", () -> true);
            resolver.validate(1);
            write(table, 1);
            long time = table.snapshotManager().snapshot(1).timeMillis();
            // Delay is only to create distinct physical commit timestamps, not to assert scheduling.
            Thread.sleep(5);
            write(table, 2);
            assertEquals(2, resolver.atTimestamp(time, System.currentTimeMillis()));
            org.apache.paimon.table.source.StreamTableScan scan = table.newReadBuilder().newStreamScan();
            scan.restore(2L);
            org.apache.paimon.table.source.TableScan.Plan plan = scan.plan();
            assertFalse(plan.splits().isEmpty());
            assertEquals(3L, scan.checkpoint());
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader = table.newReadBuilder().newRead().createReader(plan.splits())) {
                java.util.List<Integer> actual = new java.util.ArrayList<>();
                reader.forEachRemaining(row -> actual.add(row.getInt(0)));
                assertEquals(java.util.Collections.singletonList(2), actual);
            }
        }
    }
    static void write(FileStoreTable table, int id) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            writer.write(GenericRow.of(id));
            commit.commit(writer.prepareCommit());
        }
    }
}
