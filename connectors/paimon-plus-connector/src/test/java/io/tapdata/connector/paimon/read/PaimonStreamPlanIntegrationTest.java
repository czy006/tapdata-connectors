package io.tapdata.connector.paimon.read;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.*;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class PaimonStreamPlanIntegrationTest {
    @Test void nativePlanEmitsRecordThenCompletedNext() throws Exception {
        java.nio.file.Path root = Files.createTempDirectory(Files.createDirectories(Paths.get("target/stream-read-validation")), "plan-");
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(root.toUri())))) {
            catalog.createDatabase("test", true);
            Identifier id = Identifier.create("test", "orders");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).option("bucket", "-1").build(), false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(id);
            PaimonReadPositionIntegrationTest.write(table, 42);
            ReadBuilder builder = table.newReadBuilder(); StreamTableScan scan = builder.newStreamScan(); scan.restore(1L);
            PaimonReadPositionResolver resolver = new PaimonReadPositionResolver(table, "orders", () -> true);
            PaimonStreamReadCoordinator.Source source = new PaimonStreamReadCoordinator.Source() {
                public String name() { return "orders"; }
                public Long checkpoint() { return scan.checkpoint(); }
                public void validatePosition(long next) throws Exception { resolver.validate(next); }
                public void beforePlan() { }
                public TableScan.Plan plan() { return scan.plan(); }
                public void beforeRead(TableScan.Plan plan) { }
                public RecordReader<InternalRow> reader(Split split) throws Exception { return builder.newRead().createReader(split); }
                public TapEvent convert(InternalRow row) { TapInsertRecordEvent event = new TapInsertRecordEvent().init(); event.setTableId("orders"); event.setAfter(Collections.singletonMap("id", row.getInt(0))); return event; }
            };
            List<PaimonStreamReadCoordinator.Item> items = new ArrayList<>();
            assertTrue(PaimonStreamReadCoordinator.readPlan(source, () -> true, items::add));
            assertEquals(2, items.size()); assertEquals(42, ((TapInsertRecordEvent) items.get(0).event).getAfter().get("id"));
            assertEquals(2, items.get(1).after); assertTrue(items.get(1).isFinished());
            items.clear(); assertFalse(PaimonStreamReadCoordinator.readPlan(source, () -> true, items::add)); assertTrue(items.isEmpty());
        }
    }
}
