package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.connector.paimon.service.PaimonConnectorMicroBatchIntegrationTest.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonRecoveryHeartbeatIntegrationTest {
    @TempDir Path directory;

    @Test
    void firstDeliveredInitialStreamPositionIsForwardedOnceAndCannotRegressCdc() throws Throwable {
        Path warehouse = Files.createDirectories(directory.resolve("warehouse"));
        TapConnectorContext context = context(warehouse, new ArrayList<>());
        List<TapCallbackOffset> acknowledged = new ArrayList<>();
        when(context.getFlushOffsetCallback()).thenReturn(value -> acknowledged.add((TapCallbackOffset) value));
        ConnectorFunctions functions = new ConnectorFunctions();
        PaimonConnector connector = new PaimonConnector();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        connector.onStart(context);
        try {
            HeartbeatEvent batch = initialPosition("ignored", "x");
            batch.addInfo(TapCallbackOffset.KEY_BATCH_OFFSET, "batch");
            functions.getProcessControlFunction().processControl(context, batch);
            functions.getProcessControlFunction().processControl(context, initialPosition(null, "x"));
            functions.getProcessControlFunction().processControl(context, initialPosition("P0", "x"));
            functions.getProcessControlFunction().processControl(context, initialPosition("duplicate", "x"));
            functions.getProcessControlFunction().processControl(context, heartbeat("P1", "x", 20L));
            functions.getProcessControlFunction().processControl(context, initialPosition("late", "x"));
            assertEquals(2, acknowledged.size());
            TapCallbackOffset first = acknowledged.get(0);
            assertEquals("INITIAL_SYNC", first.get(TapCallbackOffset.KEY_SYNC_STAGE));
            assertEquals("P0", first.get(TapCallbackOffset.KEY_STREAM_OFFSET));
            assertEquals(Collections.singletonList("x"), first.get(TapCallbackOffset.KEY_NODE_IDS));
            assertEquals(10L, first.get(TapCallbackOffset.KEY_SOURCE_TIME));
            assertEquals(10L, first.get(TapCallbackOffset.KEY_EVENT_TIME));
            assertNull(first.get(TapCallbackOffset.KEY_BATCH_OFFSET));
            assertEquals("CDC", acknowledged.get(1).get(TapCallbackOffset.KEY_SYNC_STAGE));
            assertEquals("P1", acknowledged.get(1).get(TapCallbackOffset.KEY_STREAM_OFFSET));

            TapTable table = table("cdc_before_first_control");
            functions.getCreateTableV2Function().createTable(context, new TapCreateTableEvent().table(table));
            functions.getWriteRecordFunction().writeRecord(context,
                    Collections.singletonList(event(table.getName(), 1, "y")), table, result -> {});
            functions.getProcessControlFunction().processControl(context, initialPosition("late-y", "y"));
            functions.getProcessControlFunction().processControl(context, initialPosition("P0-z", "z"));
            assertEquals(3, acknowledged.size());
            assertEquals("P0-z", acknowledged.get(2).get(TapCallbackOffset.KEY_STREAM_OFFSET));
        } finally {
            connector.onStop(context);
        }
    }

    private static HeartbeatEvent initialPosition(String offset, String lane) {
        HeartbeatEvent event = heartbeat(offset, lane, 10L);
        event.addInfo(TapCallbackOffset.KEY_SYNC_STAGE, "INITIAL_SYNC");
        return event;
    }

    @Test
    void initialPositionAfterFullLoadDataWaitsForItsTablePublication() throws Throwable {
        Path warehouse = Files.createDirectories(directory.resolve("warehouse"));
        List<String> callbacks = new ArrayList<>();
        TapConnectorContext context = context(warehouse, callbacks);
        ConnectorFunctions functions = new ConnectorFunctions();
        PaimonConnector connector = new PaimonConnector();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        connector.onStart(context);
        try {
            TapTable table = table("deferred_initial_position");
            functions.getCreateTableV2Function().createTable(context, new TapCreateTableEvent().table(table));
            TapInsertRecordEvent row = event(table.getName(), 1, "x");
            row.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
            functions.getWriteRecordFunction().writeRecord(context, Collections.singletonList(row), table, result -> {});
            functions.getProcessControlFunction().processControl(context, initialPosition("P0", "x"));
            functions.getProcessControlFunction().processControl(context, initialPosition("later-initial", "x"));
            assertTrue(callbacks.isEmpty());

            functions.getAfterInitialSyncFunction().afterInitialSync(context, table);
            assertEquals(Collections.singletonList("P0"), callbacks);
        } finally {
            connector.onStop(context);
        }
    }

    @Test
    void evenAnEmptyPreviouslyBoundNodeRefusesSilentResumeAndReleasesItsDirectory() throws Throwable {
        Path warehouse = Files.createDirectories(directory.resolve("warehouse"));
        List<String> callbacks = new ArrayList<>();
        TapConnectorContext context = context(warehouse, callbacks);
        PaimonConnector initial = new PaimonConnector();
        initial.onStart(context);
        initial.onStop(context);

        for (int attempt = 0; attempt < 2; attempt++) {
            PaimonConnector restarted = new PaimonConnector();
            ConnectorFunctions functions = new ConnectorFunctions();
            restarted.registerCapabilities(functions, new TapCodecsRegistry());
            IllegalStateException failure = assertThrows(IllegalStateException.class, () -> restarted.onStart(context));
            assertTrue(failure.getMessage().contains("PAIMON_RECOVERY_SOURCE_POSITION_UNVERIFIED"), failure::toString);
            functions.getProcessControlFunction().processControl(context, heartbeat("current", "x", 20L));
            assertSame(failure, assertThrows(IllegalStateException.class, () -> functions.getWriteRecordFunction()
                    .writeRecord(context, Collections.emptyList(), table("orders"), result -> {})).getCause());
            restarted.onStop(context);
        }
        assertTrue(callbacks.isEmpty());
    }

    @Test
    void fullLoadWithoutSourceHoldsFutureLanesUntilEachDependentTableIsVisible() throws Throwable {
        Path warehouse = Files.createDirectories(directory.resolve("warehouse"));
        List<String> callbacks = new ArrayList<>();
        TapConnectorContext context = context(warehouse, callbacks);
        ConnectorFunctions functions = new ConnectorFunctions();
        PaimonConnector connector = new PaimonConnector();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        connector.onStart(context);
        try (Catalog observer = CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(
                Collections.singletonMap("warehouse", warehouse.toUri().toString()))))) {
            TapTable a = table("orders_a");
            TapTable b = table("orders_b");
            for (TapTable table : Arrays.asList(a, b)) {
                functions.getCreateTableV2Function().createTable(context, new TapCreateTableEvent().table(table));
            }
            TapInsertRecordEvent initial = event(a.getName(), 1, "x");
            initial.getInfo().remove(TapCallbackOffset.KEY_NODE_IDS);
            initial.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
            functions.getWriteRecordFunction().writeRecord(context, Collections.singletonList(initial), a, result -> {});
            functions.getWriteRecordFunction().writeRecord(context,
                    Collections.singletonList(event(b.getName(), 2, "x")), b, result -> {});
            functions.getProcessControlFunction().processControl(context, heartbeat("x-10", "x", 10L));
            functions.getProcessControlFunction().processControl(context, heartbeat("y-20", "y", 20L));
            FileStoreTable physicalA = (FileStoreTable) observer.getTable(Identifier.create("default", a.getName()));
            FileStoreTable physicalB = (FileStoreTable) observer.getTable(Identifier.create("default", b.getName()));
            assertNull(physicalA.snapshotManager().latestSnapshotIdFromFileSystem());
            assertNull(physicalB.snapshotManager().latestSnapshotIdFromFileSystem());
            assertTrue(callbacks.isEmpty());

            functions.getAfterInitialSyncFunction().afterInitialSync(context, a);
            assertEquals(Collections.singletonMap(1, "v1"), rows(physicalA));
            assertNull(physicalB.snapshotManager().latestSnapshotIdFromFileSystem());
            assertEquals(Collections.singletonList("y-20"), callbacks);

            functions.getWriteRecordFunction().writeRecord(context,
                    Collections.singletonList(event(b.getName(), 3, "x")), b, result -> {});
            Map<Integer, String> expected = new HashMap<>();
            expected.put(2, "v2");
            expected.put(3, "v3");
            assertEquals(expected, rows(physicalB));
            assertEquals(Arrays.asList("y-20", "x-10"), callbacks);
        } finally {
            connector.onStop(context);
        }
    }

    @Test
    void removingTheRecoveryDirectoryFromABoundNodeRefusesWritesAndHeartbeats() throws Throwable {
        assertBoundNodeRejectsDowngrade(true);
    }

    @Test
    void removingBothTheDirectoryAndTheOffsetCapabilityAlsoRefusesDowngrade() throws Throwable {
        assertBoundNodeRejectsDowngrade(false);
    }

    @Test
    void lastPendingHeartbeatReportsScheduledCallbackFailureWithoutAnotherInput() throws Throwable {
        Path warehouse = Files.createDirectories(directory.resolve("warehouse"));
        TapConnectorContext context = context(warehouse, new ArrayList<>());
        context.getNodeConfig().put("commitIntervalMs", 1000);
        context.getNodeConfig().put("cdcCommitTrigger", "time");
        IllegalStateException consumerFailure = new IllegalStateException("offset consumer failed");
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        when(context.getFlushOffsetCallback()).thenReturn(value -> {
            callbackThread.set(Thread.currentThread());
            attempts.incrementAndGet();
            throw consumerFailure;
        });
        CountDownLatch reported = new CountDownLatch(1);
        Log log = context.getLog();
        doAnswer(call -> {
            reported.countDown();
            return null;
        }).when(log).error(eq("Paimon local recovery target failed"), any(Throwable.class));
        ConnectorFunctions functions = new ConnectorFunctions();
        PaimonConnector connector = new PaimonConnector();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        connector.onStart(context);
        try {
            TapTable table = table("last_heartbeat");
            functions.getCreateTableV2Function().createTable(context, new TapCreateTableEvent().table(table));
            functions.getWriteRecordFunction().writeRecord(context,
                    Collections.singletonList(event(table.getName(), 1, "x")), table, result -> {});
            functions.getProcessControlFunction().processControl(context, heartbeat("x-10", "x", 10L));
            assertTrue(reported.await(30, TimeUnit.SECONDS));
            assertNotSame(Thread.currentThread(), callbackThread.get());
            functions.getProcessControlFunction().processControl(context, heartbeat("x-20", "x", 20L));
            assertEquals(1, attempts.get());
            verify(log).error("Paimon local recovery target failed", consumerFailure);
            assertSame(consumerFailure, assertThrows(IllegalStateException.class, () -> functions.getWriteRecordFunction()
                    .writeRecord(context, Collections.singletonList(event(table.getName(), 2, "x")),
                            table, result -> {})).getCause());
        } finally {
            assertThrows(Exception.class, () -> connector.onStop(context));
        }
    }

    private void assertBoundNodeRejectsDowngrade(boolean hasOffsetCapability) throws Throwable {
        Path warehouse = Files.createDirectories(directory.resolve("warehouse"));
        List<String> callbacks = new ArrayList<>();
        TapConnectorContext context = context(warehouse, callbacks);
        TapTable table = table("pending_full_load");
        ConnectorFunctions firstFunctions = new ConnectorFunctions();
        PaimonConnector first = new PaimonConnector();
        first.registerCapabilities(firstFunctions, new TapCodecsRegistry());
        first.onStart(context);
        try {
            firstFunctions.getCreateTableV2Function().createTable(context, new TapCreateTableEvent().table(table));
            TapInsertRecordEvent initial = event(table.getName(), 1, "x");
            initial.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
            firstFunctions.getWriteRecordFunction().writeRecord(context, Collections.singletonList(initial), table, result -> {});
        } finally {
            first.onStop(context);
        }

        when(context.getNodeConfig()).thenReturn(DataMap.create());
        if (!hasOffsetCapability) {
            when(context.getFlushOffsetCallback()).thenReturn(null);
        }
        ConnectorFunctions functions = new ConnectorFunctions();
        PaimonConnector restarted = new PaimonConnector();
        restarted.registerCapabilities(functions, new TapCodecsRegistry());
        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> restarted.onStart(context));
        assertTrue(failure.getMessage().contains("bound to local recovery"));
        functions.getProcessControlFunction().processControl(context, heartbeat("x-20", "x", 20L));
        assertSame(failure, assertThrows(IllegalStateException.class, () ->
                functions.getWriteRecordFunction().writeRecord(context,
                        Collections.singletonList(event(table.getName(), 2, "x")), table, result -> {})).getCause());
        assertTrue(callbacks.isEmpty());
        try (Catalog observer = CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(
                Collections.singletonMap("warehouse", warehouse.toUri().toString()))))) {
            FileStoreTable physical = (FileStoreTable) observer.getTable(Identifier.create("default", table.getName()));
            assertNull(physical.snapshotManager().latestSnapshotIdFromFileSystem());
        }
    }

    private TapConnectorContext context(Path warehouse, List<String> callbacks) throws Exception {
        TapConnectorContext context = connectorContext();
        when(context.getConnectionConfig()).thenReturn(DataMap.create()
                .kv("warehouse", warehouse.toString()).kv("database", "default")
                .kv("diskTmpDir", Files.createDirectories(directory.resolve("spill")).toString())
                .kv("enableAutoCompaction", false).kv("bucketMode", "fixed").kv("bucketCount", 1));
        when(context.getNodeConfig()).thenReturn(DataMap.create()
                .kv("localRecoveryDir", directory.resolve("recovery").toString())
                .kv("cdcCommitTrigger", "timeOrRows").kv("batchAccumulationSize", 2)
                .kv("commitIntervalMs", 3600000).kv("enableDeduplicateUpdateAfterCompletenessCheck", true));
        when(context.getFlushOffsetCallback()).thenReturn(value ->
                callbacks.add((String) ((TapCallbackOffset) value).get(TapCallbackOffset.KEY_STREAM_OFFSET)));
        return context;
    }

    private static Map<Integer, String> rows(FileStoreTable table) throws Exception {
        ReadBuilder read = table.newReadBuilder();
        Map<Integer, String> values = new HashMap<>();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> assertNull(values.put(row.getInt(0), row.getString(1).toString())));
        }
        return values;
    }
}
