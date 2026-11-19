package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataCompleteTableSnapshotEvent;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.PartitionConcurrentProcessor;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.mockito.Mockito.*;

// Pinned engine: tapdata@65add935fdee. Run via run-anchor-probe.sh, outside the connector JAR.
public final class EngineAnchorProbe {
    private static final String EDGE = "source,target";
    private static final List<String> SOURCE = List.of("source");

    public static void main(String[] args) throws Exception {
        String uri = System.getenv("PROBE_MONGO_URI");
        if (uri == null || uri.isBlank()) {
            run(null);
            System.out.println("PASS engine + in-memory Manager boundary; real Mongo NOT RUN");
        } else {
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(uri))
                    .applyToClusterSettings(c -> c.serverSelectionTimeout(5, TimeUnit.SECONDS))
                    .build();
            try (MongoClient client = MongoClients.create(settings)) {
                var db = client.getDatabase("paimon_local_recovery_e2e");
                db.runCommand(new Document("ping", 1));
                run(db.getCollection("anchor_probe").withWriteConcern(WriteConcern.MAJORITY));
                System.out.println("PASS engine + real Mongo Manager-shaped progress document");
            }
        }
        // Engine/PDK class initialization may create non-daemon services outside this probe.
        // This entry point is run in its own JVM after the isolated document has been cleaned.
        System.exit(0);
    }

    private static void run(MongoCollection<Document> collection) throws Exception {
        Map<String, Object> settings = Map.of("localRecoveryDir", "/example/target");
        DataMap initialSettings = targetNodeConfig(settings, SyncTypeEnum.INITIAL_SYNC);
        check(initialSettings.equals(targetNodeConfig(settings, SyncTypeEnum.INITIAL_SYNC_CDC))
                        && initialSettings.equals(targetNodeConfig(settings, SyncTypeEnum.CDC)),
                "PDK node configuration is identical across the three task sync types");
        System.out.println("PASS engine produces the same target node configuration for initial, CDC and combined tasks");
        ProgressSink sink = new ProgressSink(collection);
        try {
            TaskDto task = new TaskDto();
            task.setId(sink.id);
            DataProcessorContext processor = mock(DataProcessorContext.class);
            when(processor.getTaskDto()).thenReturn(task);
            TableNode node = mock(TableNode.class);
            when(node.getId()).thenReturn("target");
            doReturn(node).when(processor).getNode();

            HazelcastTargetPdkBaseNode target = mock(HazelcastTargetPdkBaseNode.class);
            configureTarget(target, processor, sink);
            TapdataEvent initial = heartbeat(SyncStage.INITIAL_SYNC, "P0");
            dispatchSerial(target, initial);
            verify(target, never()).flushSyncProgressMap(any());
            PartitionConcurrentProcessor workers = mock(PartitionConcurrentProcessor.class, CALLS_REAL_METHODS);
            Method selectOffset = PartitionConcurrentProcessor.class.getDeclaredMethod(
                    "getOffsetEvent", TapdataEvent.class, List.class);
            selectOffset.setAccessible(true);
            check(selectOffset.invoke(workers, initial, new ArrayList<>()) == initial,
                    "concurrent selector admits stream-only INITIAL heartbeat");

            TapdataCompleteTableSnapshotEvent over = new TapdataCompleteTableSnapshotEvent("A");
            over.setSyncStage(SyncStage.INITIAL_SYNC);
            over.setNodeIds(SOURCE);
            over.setBatchOffset(Map.of(BatchOffsetUtil.BATCH_READ_CONNECTOR_STATUS, "OVER"));
            dispatchSerial(target, over);
            check(target.saveToSnapshot(), "save OVER");
            SyncProgress restarted = restore(sink.read(), SyncTypeEnum.INITIAL_SYNC_CDC, null);
            check(BatchOffsetUtil.batchIsOverOfTable(restarted, "A"), "durable A=OVER");
            check(restarted.getStreamOffset() == null || restarted.getStreamOffset().isEmpty(),
                    "initial CDC anchor missing after OVER");
            restore(sink.read(), SyncTypeEnum.INITIAL_SYNC, null);
            System.out.println("PASS serial empty A=OVER without P0; source recovery requested current");
            System.out.println("PASS initial-only restart needs no stream cursor; concurrent selector selected P0");

            dispatchSerial(target, heartbeat(SyncStage.CDC, "P1"));
            verify(target).flushSyncProgressMap(argThat(event -> event.getSyncStage() == SyncStage.CDC));
            check("P1".equals(target.syncProgressMap.get(EDGE).getStreamOffsetObj()), "engine accepted CDC P1");
            check(sink.lastWriteSucceeded, "safe CDC callback saved");
            restore(sink.read(), SyncTypeEnum.CDC, "P1");

            sink.rejectNext = true;
            dispatchSerial(target, heartbeat(SyncStage.CDC, "P2"));
            check(!sink.lastWriteSucceeded, "manager write failure injected");
            check(sink.sourceFlushes.equals(List.of("P1", "P2")), "source ACK precedes rejected Mongo write");
            restore(sink.read(), SyncTypeEnum.CDC, "P1");
            System.out.println("PASS CDC P1 restored; ACK P2 precedes failed persistence leaving P1 durable");
        } finally {
            sink.close();
        }
    }

    static TapdataEvent heartbeat(SyncStage stage, String offset) {
        TapdataEvent event = TapdataHeartbeatEvent.create(42L, offset);
        event.setSyncStage(stage);
        event.setNodeIds(SOURCE);
        return event;
    }

    static void dispatchSerial(HazelcastTargetPdkBaseNode target, TapdataEvent event) throws Exception {
        Method ingress = HazelcastTargetPdkBaseNode.class.getDeclaredMethod(
                "flushOffsetByTapdataEventForNoConcurrent", AtomicReference.class);
        ingress.setAccessible(true);
        ingress.invoke(target, new AtomicReference<>(event));
    }

    static DataMap targetNodeConfig(Map<String, Object> settings, SyncTypeEnum type) {
        TableNode node = new TableNode();
        node.setNodeConfig(new HashMap<>(settings));
        TaskDto task = new TaskDto();
        task.setSyncType(type.getSyncType());
        HazelcastTargetPdkBaseNode target = mock(HazelcastTargetPdkBaseNode.class);
        doCallRealMethod().when(target).generateNodeConfig(any(), any());
        return DataMap.create(target.generateNodeConfig(node, task));
    }

    static void configureTarget(HazelcastTargetPdkBaseNode target, DataProcessorContext processor,
                                ProgressSink sink) throws Exception {
        configureTarget(target, processor, sink, sink.sourceFlushes::add);
    }

    static void configureTarget(HazelcastTargetPdkBaseNode target, DataProcessorContext processor,
                                ProgressSink sink, java.util.function.Consumer<Object> sourceFlush) throws Exception {
        setField(target, "fromConcurrentProcessor", ThreadLocal.withInitial(() -> false));
        setField(target, "processorBaseContext", processor);
        setField(target, "dataProcessorContext", processor);
        setField(target, "saveSnapshotLock", new Object());
        setField(target, "flushOffset", new AtomicBoolean());
        setField(target, "obsLogger", mock(ObsLogger.class));
        target.syncProgressMap = new HashMap<>();
        target.uploadDagService = new AtomicBoolean();
        target.offsetCallbackEnable = true;

        ConnectorNode targetPdk = mock(ConnectorNode.class, RETURNS_DEEP_STUBS);
        TapConnectorContext context = mock(TapConnectorContext.class, CALLS_REAL_METHODS);
        when(targetPdk.getConnectorContext()).thenReturn(context);
        when(target.getConnectorNode()).thenReturn(targetPdk);
        when(targetPdk.getConnectorFunctions().getProcessControlFunction()).thenReturn((ctx, event) -> {
            TapCallbackOffset payload = new TapCallbackOffset();
            for (String field : List.of("nodeIds", "streamOffset", "syncStage", "sourceTime")) {
                Object value = event.getInfo(field);
                payload.put(field, "syncStage".equals(field) ? String.valueOf(value) : value);
            }
            ctx.getFlushOffsetCallback().accept(payload);
        });
        doCallRealMethod().when(target).initFlushOffsetConsumer();
        doCallRealMethod().when(target).parseSyncStageSafely(anyString(), any());
        doCallRealMethod().when(target).flushSyncProgressMap(any());
        doCallRealMethod().when(target).flushOffsetCallback(any(), any());
        doCallRealMethod().when(target).saveToSnapshot();
        doAnswer(invocation -> { throw new AssertionError("unexpected engine error", (Throwable) invocation.getArgument(0)); })
                .when(target).errorHandle(any(Throwable.class));
        target.initFlushOffsetConsumer();

        ConnectorNode sourcePdk = mock(ConnectorNode.class, RETURNS_DEEP_STUBS);
        when(sourcePdk.getConnectorFunctions().getFlushOffsetFunction())
                .thenReturn((ctx, pos) -> sourceFlush.accept(pos));
        setField(target, "sourceConnectorNodeMap", Map.of("source", sourcePdk));
        ClientMongoOperator manager = mock(ClientMongoOperator.class);
        doAnswer(invocation -> {
            String url = invocation.getArgument(1);
            check(url.endsWith("/syncProgress/" + sink.id), "isolated task ID");
            sink.write(invocation.getArgument(0));
            return null;
        }).when(manager).insertOne(any(), anyString());
        setField(target, "clientMongoOperator", manager);
    }

    static SyncProgress restore(Map<String, Object> attrs, SyncTypeEnum type, String expected)
            throws Exception {
        HazelcastSourcePdkDataNode source = mock(HazelcastSourcePdkDataNode.class, CALLS_REAL_METHODS);
        doNothing().when(source).initStreamOffsetFromTime(any());
        setField(source, "syncType", type);
        doReturn(mock(ConnectorNode.class)).when(source).getConnectorNode();
        SyncProgress progress = source.foundAllSyncProgress(attrs).get(EDGE);
        check(progress != null, "edge decoded through engine restart method");
        source.syncProgress = progress;
        source.readBatchOffset(progress);
        source.readNormalAndLogCollectorTaskStreamOffset(progress.getStreamOffset());
        if (expected == null) {
            if (type == SyncTypeEnum.INITIAL_SYNC) {
                verify(source, never()).initStreamOffsetFromTime(any());
            } else {
                verify(source).initStreamOffsetFromTime(null);
            }
        } else {
            verify(source, never()).initStreamOffsetFromTime(any());
            check(expected.equals(progress.getStreamOffsetObj()), "source decoded durable stream cursor");
        }
        return progress;
    }

    private static void setField(Object instance, String name, Object value) throws Exception {
        for (Class<?> c = instance.getClass(); c != null; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (f.getName().equals(name)) {
                    f.setAccessible(true);
                    f.set(instance, value);
                    return;
                }
            }
        }
        throw new NoSuchFieldException(name);
    }

    static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }

    // TaskServiceImpl#updateSyncProgress stores each edge JSON under attrs.syncProgress.<edge>.
    // Only a fresh ObjectId in a fixed test namespace is written, and only that ID is cleaned.
    static class ProgressSink {
        final ObjectId id;
        final List<Object> sourceFlushes = new java.util.concurrent.CopyOnWriteArrayList<>();
        final Map<String, String> memory = new HashMap<>();
        final MongoCollection<Document> collection;
        volatile boolean rejectNext;
        volatile boolean lastWriteSucceeded;

        ProgressSink(MongoCollection<Document> collection) {
            this(collection, new ObjectId(), true);
        }

        ProgressSink(MongoCollection<Document> collection, ObjectId id, boolean create) {
            this.id = id;
            this.collection = collection;
            if (collection != null && create) collection.insertOne(new Document("_id", id));
        }

        void write(Map<String, String> payload) {
            lastWriteSucceeded = false;
            if (rejectNext) {
                rejectNext = false;
                throw new IllegalStateException("injected Manager write failure");
            }
            if (collection == null) {
                memory.putAll(payload);
            } else {
                payload.forEach((edge, json) -> check(collection.updateOne(eq("_id", id),
                        set("attrs.syncProgress." + edge, json)).getMatchedCount() == 1, "isolated document"));
            }
            lastWriteSucceeded = true;
        }

        Map<String, Object> read() {
            if (collection == null) return Map.of("syncProgress", new HashMap<>(memory));
            Document row = collection.find(eq("_id", id)).first();
            check(row != null, "isolated progress record present");
            return row.get("attrs", Document.class);
        }

        void close() {
            if (collection != null) collection.deleteOne(eq("_id", id));
        }
    }
}
