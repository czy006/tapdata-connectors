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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Run with the module test classpath and a fresh scratch directory. A regression exits nonzero. */
public class FallbackBranchReviewProbe {
    private static final Identifier ORDERS = Identifier.create("default", "orders");

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        run(Files.createDirectory(root.resolve("control")), false);
        run(Files.createDirectory(root.resolve("fallback")), true);
    }

    private static void run(Path root, boolean useFallback) throws Exception {
        Path warehouse = Files.createDirectory(root.resolve("warehouse"));
        Options options = Options.fromMap(Map.of("warehouse", warehouse.toUri().toString()));
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("default", true);
            catalog.createTable(ORDERS, Schema.newBuilder()
                    .column("p", DataTypes.INT()).column("id", DataTypes.INT()).column("v", DataTypes.INT())
                    .primaryKey("p", "id").partitionKeys("p").option("bucket", "1")
                    .option("write-only", "true").build(), false);
            FileStoreTable table = table(catalog);
            FileStoreTable seedTable = table;
            if (useFallback) {
                table.createBranch("history");
                seedTable = table.switchToBranch("history");
            }
            StreamWriteBuilder builder = seedTable.newStreamWriteBuilder().withCommitUser("seed");
            try (StreamTableWrite writer = builder.newWrite(); StreamTableCommit commit = builder.newCommit()) {
                writer.write(GenericRow.of(1, 1, 100));
                commit.commit(1, writer.prepareCommit(false, 1));
            }
            if (useFallback) {
                catalog.alterTable(ORDERS, List.of(SchemaChange.setOption("scan.fallback-branch", "history")), false);
            }
            List<String> baseline = rows(table(catalog));
            assertEquals(List.of("1/1=100"), baseline);
            PaimonConfig config = new PaimonConfig();
            config.setStorageType("local");
            config.setDatabase("default");
            config.setWarehouse(warehouse.toString());
            config.setDiskTmpDir(Files.createDirectory(root.resolve("spill")).toString());
            config.setBucketMode("fixed");
            config.setBucketCount(1);
            config.setEnableAutoCompaction(false);
            config.loadNodeConfig(Map.of("localRecoveryDir", root.resolve("recovery").toString(),
                    "cdcCommitTrigger", "timeOrRows"));
            config.setBatchAccumulationSize(1);
            Log log = mock(Log.class);
            TapConnectorContext context = mock(TapConnectorContext.class);
            when(context.getLog()).thenReturn(log);
            KVMap<Object> state = PaimonStartupRecoveryIntegrationTest.taskState(root.resolve("node.properties"));
            when(context.getStateMap()).thenReturn(state);
            TapTable source = new TapTable("orders").add(new TapField("p", "INT").primaryKeyPos(1))
                    .add(new TapField("id", "INT").primaryKeyPos(2)).add(new TapField("v", "INT"));
            AtomicReference<Object> acknowledged = new AtomicReference<>();
            PaimonFatalWriteException verified = null;
            try (PaimonService service = new PaimonService(config, log)) {
                service.setFlushOffsetCallback(offset -> acknowledged.set(
                        ((Map<?, ?>) offset).get(TapCallbackOffset.KEY_STREAM_OFFSET)));
                service.init();
                service.startLocalRecovery(context);
                if (useFallback) {
                    PaimonFatalWriteException error = assertThrows(PaimonFatalWriteException.class,
                            () -> service.createTable(source));
                    assertTrue(error.getMessage().contains("scan.fallback-branch"), error::getMessage);
                    assertNull(acknowledged.get());
                    assertNull(table(catalog).snapshotManager().latestSnapshotIdFromFileSystem());
                    assertEquals(baseline, rows(table(catalog)));
                    verified = error;
                    System.out.println("fallback=true refused before target publication or P1 acknowledgement");
                    return;
                }
                service.createTable(source);
                TapInsertRecordEvent event = new TapInsertRecordEvent().init().table("orders")
                        .after(Map.of("p", 1, "id", 2, "v", 200));
                event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
                event.addInfo(TapCallbackOffset.KEY_NODE_IDS, Collections.singletonList("source"));
                event.addInfo(TapCallbackOffset.KEY_STREAM_OFFSET, "P1");
                event.addInfo(TapCallbackOffset.KEY_SOURCE_TIME, 1L);
                service.writeRecords(Collections.singletonList(event), source, context);
                service.processHeartbeat(PaimonConnectorMicroBatchIntegrationTest.heartbeat("P1", "source", 1L));
            } catch (PaimonFatalWriteException rejection) {
                if (rejection != verified) { throw rejection; }
                // close() reports the same sticky error after the createTable assertion above.
                return;
            }
            FileStoreTable current = table(catalog);
            List<String> actual = rows(current);
            List<String> expected = Arrays.asList("1/1=100", "1/2=200");
            System.out.printf("fallback=%s P0=%s P1=%s expected=%s actual=%s mainSnapshot=%s%n", useFallback,
                    baseline, acknowledged.get(), expected, actual, current.snapshotManager().latestSnapshotIdFromFileSystem());
            assertEquals("P1", acknowledged.get());
            assertEquals(expected, actual, "Target ordinary batch at acknowledged source position P1");
        }
    }

    private static FileStoreTable table(Catalog catalog) throws Exception {
        catalog.invalidateTable(ORDERS);
        return (FileStoreTable) catalog.getTable(ORDERS);
    }

    private static List<String> rows(FileStoreTable table) throws Exception {
        List<String> rows = new ArrayList<>();
        ReadBuilder read = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0) + "/" + row.getInt(1) + "=" + row.getInt(2)));
        }
        Collections.sort(rows);
        return rows;
    }
}
