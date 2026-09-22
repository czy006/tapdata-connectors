package io.tapdata.connector.paimon.read;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.paimon.catalog.*;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;

class PaimonReadSchemaGuardIntegrationTest {
    @Test void commentsAreCompatibleButNewColumnsFailEvenOnBoundOldTable() throws Exception {
        java.nio.file.Path root = Files.createTempDirectory(Files.createDirectories(Paths.get("target/stream-read-validation")), "schema-");
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(root.toUri())))) {
            catalog.createDatabase("test", true);
            Identifier id = Identifier.create("test", "orders");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).option("bucket", "-1").build(), false);
            FileStoreTable old = (FileStoreTable) catalog.getTable(id);
            TapTable tap = new TapTable("orders").add(new TapField("id", "INT"));
            PaimonReadSchemaGuard guard = new PaimonReadSchemaGuard(old, tap, new PaimonReadPositionResolver(old, "orders", () -> true));
            guard.beforePlan();
            catalog.alterTable(id, Collections.singletonList(SchemaChange.updateColumnComment("id", "description")), false);
            guard.beforePlan();
            catalog.alterTable(id, Collections.singletonList(SchemaChange.addColumn("extra", DataTypes.STRING())), false);
            assertThrows(IllegalStateException.class, guard::beforePlan);
            assertEquals(1, old.rowType().getFieldCount(), "Bound reader still sees original structure");
        }
    }
    @Test void historyUsesNativeFieldIdProjectionAfterReload() throws Exception {
        java.nio.file.Path root = Files.createTempDirectory(Files.createDirectories(Paths.get("target/stream-read-validation")), "history-schema-");
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(root.toUri())))) {
            catalog.createDatabase("test", true);
            Identifier id = Identifier.create("test", "orders");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).option("bucket", "-1").build(), false);
            FileStoreTable original = (FileStoreTable) catalog.getTable(id);
            PaimonReadPositionIntegrationTest.write(original, 7);
            catalog.alterTable(id, Collections.singletonList(SchemaChange.renameColumn("id", "renamed")), false);
            FileStoreTable reloaded = (FileStoreTable) catalog.getTable(id);
            TapTable tap = new TapTable("orders").add(new TapField("renamed", "INT"));
            PaimonReadSchemaGuard guard = new PaimonReadSchemaGuard(reloaded, tap, new PaimonReadPositionResolver(reloaded, "orders", () -> true));
            org.apache.paimon.table.source.StreamTableScan scan = reloaded.newReadBuilder().newStreamScan();
            scan.restore(1L);
            org.apache.paimon.table.source.TableScan.Plan plan = scan.plan();
            guard.beforePlan(); guard.beforeRead(plan);
            java.util.List<Integer> values = new java.util.ArrayList<>();
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader = reloaded.newReadBuilder().newRead().createReader(plan.splits())) {
                reader.forEachRemaining(row -> values.add(row.getInt(0)));
            }
            assertEquals(Collections.singletonList(7), values);
            assertThrows(IllegalStateException.class, () -> new PaimonReadSchemaGuard(reloaded,
                    new TapTable("orders").add(new TapField("renamed", "BIGINT")), new PaimonReadPositionResolver(reloaded, "orders", () -> true)));
        }
    }
}
