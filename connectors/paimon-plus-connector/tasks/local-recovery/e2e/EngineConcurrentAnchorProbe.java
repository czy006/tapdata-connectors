package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataCompleteTableSnapshotEvent;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.connector.paimon.service.PaimonStartupRecoveryIntegrationTest;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.PartitionConcurrentProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.Partitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.PartitionKeySelector;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.core.api.ConnectorNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.EngineAnchorProbe.*;
import static org.mockito.Mockito.*;

// Review of an initially empty source table and the first CDC anchor. Run with --concurrent.
public final class EngineConcurrentAnchorProbe {
    public static void main(String[] args) {
        try {
            runBothIngressPaths(args);
        } catch (Throwable failure) {
            failure.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    private static void runBothIngressPaths(String[] args) throws Throwable {
        Path directory = args.length == 0 ? Files.createTempDirectory("paimon-anchor-review") : Path.of(args[0]);
        run(Files.createDirectory(directory.resolve("concurrent")), true);
        run(Files.createDirectory(directory.resolve("serial")), false);
        System.out.println("BOUNDARY real two-worker watermark, serial filter, Paimon callback and source restart; simulated Manager");
    }

    private static void run(Path directory, boolean concurrentInitial) throws Throwable {
        DelayedManager sink = new DelayedManager();
        TaskDto task = new TaskDto();
        task.setId(sink.id);
        DataProcessorContext processor = mock(DataProcessorContext.class);
        when(processor.getTaskDto()).thenReturn(task);
        TableNode node = mock(TableNode.class);
        when(node.getId()).thenReturn("target");
        doReturn(node).when(processor).getNode();
        HazelcastTargetPdkBaseNode target = mock(HazelcastTargetPdkBaseNode.class);
        configureTarget(target, processor, sink);
        ConnectorNode pdk = target.getConnectorNode();
        TapConnectorContext context = pdk.getConnectorContext();
        KVMap<Object> state = PaimonStartupRecoveryIntegrationTest.taskState(directory.resolve("node.properties"));
        when(context.getStateMap()).thenReturn(state);
        when(context.getLog()).thenReturn(mock(Log.class));
        when(context.getConnectionConfig()).thenReturn(DataMap.create()
                .kv("warehouse", directory.resolve("warehouse").toString()).kv("database", "default")
                .kv("diskTmpDir", directory.resolve("spill").toString())
                .kv("enableAutoCompaction", false).kv("bucketMode", "fixed").kv("bucketCount", 1));
        doReturn(targetNodeConfig(DataMap.create()
                .kv("localRecoveryDir", directory.resolve("recovery").toString())
                .kv("enableDeduplicateUpdateAfterCompletenessCheck", true), SyncTypeEnum.INITIAL_SYNC_CDC)).when(context).getNodeConfig();
        PaimonConnector connector = new PaimonConnector();
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        connector.onStart(context);
        try {
            List<String> delivered = new CopyOnWriteArrayList<>();
            when(pdk.getConnectorFunctions().getProcessControlFunction()).thenReturn((ctx, event) -> {
                delivered.add(String.valueOf((Object) event.getInfo("streamOffset")));
                functions.getProcessControlFunction().processControl(ctx, event);
            });
            verifyWatermarks(target, task, sink, delivered, concurrentInitial);
        } finally {
            connector.onStop(context);
            sink.close();
        }
    }

    private static void verifyWatermarks(HazelcastTargetPdkBaseNode target, TaskDto task,
                                         DelayedManager sink, List<String> delivered, boolean concurrentInitial) throws Exception {
        Map<String, CountDownLatch> seen = Map.of("P0", new CountDownLatch(1), "P1", new CountDownLatch(1),
                "P2", new CountDownLatch(1));
        AtomicReference<Throwable> error = new AtomicReference<>();
        PartitionConcurrentProcessor workers = new PartitionConcurrentProcessor(2, 8,
                mock(Partitioner.class), mock(PartitionKeySelector.class),
                batch -> {}, event -> {
                    target.flushSyncProgressMap(event);
                    seen.get(event.getStreamOffset()).countDown();
                }, (failure, message) -> error.set(failure), () -> true, task);
        workers.start();
        ExecutorService timer = Executors.newSingleThreadExecutor();
        try {
            String initialCursor = concurrentInitial ? "P0" : null;
            if (concurrentInitial) {
                send(workers, SyncStage.INITIAL_SYNC, "P0", seen, error);
                check(delivered.equals(List.of("P0")), "concurrent target received P0");
                SyncProgress atP0 = restore(sink.read(), SyncTypeEnum.INITIAL_SYNC_CDC, "P0");
                check(SyncStage.INITIAL_SYNC.name().equals(atP0.getSyncStage()), "initial anchor retained its stage");
                System.out.println("PASS two workers delivered P0; Paimon callback restored P0 in INITIAL_SYNC");
            } else {
                dispatchSerial(target, heartbeat(SyncStage.INITIAL_SYNC, "P0"));
                check(delivered.isEmpty(), "serial filter withheld P0 from the connector");
            }
            TapdataCompleteTableSnapshotEvent over = new TapdataCompleteTableSnapshotEvent("A");
            over.setSyncStage(SyncStage.INITIAL_SYNC);
            over.setNodeIds(List.of("source"));
            over.setBatchOffset(Map.of(BatchOffsetUtil.BATCH_READ_CONNECTOR_STATUS, "OVER"));
            target.flushSyncProgressMap(over);
            check(target.saveToSnapshot(), "save empty A=OVER");
            SyncProgress restored = restore(sink.read(), SyncTypeEnum.INITIAL_SYNC_CDC, initialCursor);
            check(BatchOffsetUtil.batchIsOverOfTable(restored, "A"), "A=OVER restored");

            // Hold a real periodic-save payload across the later successful callback save.
            DelayedWrite initial = sink.blockNext();
            Future<Boolean> emptySave = timer.submit(target::saveToSnapshot);
            check(initial.entered.await(8, TimeUnit.SECONDS), "older full-load save reached the Manager boundary");
            send(workers, SyncStage.CDC, "P1", seen, error);
            restore(sink.read(), SyncTypeEnum.CDC, "P1");
            System.out.println("CONTROL CDC P1 delivered to same connector and restored");
            initial.release.countDown();
            check(emptySave.get(8, TimeUnit.SECONDS), "older A=OVER save reported success");
            SyncProgress regressed = restore(sink.read(), SyncTypeEnum.INITIAL_SYNC_CDC, initialCursor);
            check(BatchOffsetUtil.batchIsOverOfTable(regressed, "A"), "A stayed complete after the stale progress update");
            check(sink.sourceFlushes.contains("P1"), "source flush had already seen P1");
            System.out.println(concurrentInitial
                    ? "OBSERVED older A=OVER/P0 save replaced persisted P1; source restart decoded P0"
                    : "OBSERVED older serial A=OVER save cleared persisted P1; source restart requested current");

            DelayedWrite first = sink.blockNext();
            Future<Boolean> oldSave = timer.submit(target::saveToSnapshot);
            check(first.entered.await(8, TimeUnit.SECONDS), "older P1 payload reached the Manager boundary");
            send(workers, SyncStage.CDC, "P2", seen, error);
            restore(sink.read(), SyncTypeEnum.CDC, "P2");
            first.release.countDown();
            check(oldSave.get(8, TimeUnit.SECONDS), "both engine save calls reported success");
            restore(sink.read(), SyncTypeEnum.CDC, "P1");
            check(sink.sourceFlushes.contains("P2"), "source flush had already seen P2");
            check(delivered.equals(concurrentInitial ? List.of("P0", "P1", "P2") : List.of("P1", "P2")),
                    "only selected offsets reached the connector, in order");
            System.out.println("OBSERVED older periodic P1 save replaced successfully persisted P2; source restart decoded P1");
        } finally {
            timer.shutdownNow();
            workers.stop();
        }
    }

    private static void send(PartitionConcurrentProcessor workers, SyncStage stage, String cursor,
                             Map<String, CountDownLatch> seen, AtomicReference<Throwable> error)
            throws InterruptedException {
        TapdataEvent event = heartbeat(stage, cursor);
        workers.process(List.of(event), false);
        check(seen.get(cursor).await(8, TimeUnit.SECONDS), "timed out waiting for " + cursor + ", error=" + error.get());
        check(error.get() == null, "unexpected partition worker failure");
    }

    private static final class DelayedManager extends ProgressSink {
        final AtomicReference<DelayedWrite> next = new AtomicReference<>();

        DelayedManager() {
            super(null);
        }

        DelayedWrite blockNext() {
            DelayedWrite delay = new DelayedWrite();
            check(next.compareAndSet(null, delay), "an earlier write is already scheduled for delay");
            return delay;
        }

        @Override
        void write(Map<String, String> payload) {
            DelayedWrite delay = next.getAndSet(null);
            if (delay != null) {
                delay.entered.countDown();
                try {
                    check(delay.release.await(15, TimeUnit.SECONDS), "older save not released");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            super.write(payload);
        }
    }

    private static final class DelayedWrite {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
    }
}
