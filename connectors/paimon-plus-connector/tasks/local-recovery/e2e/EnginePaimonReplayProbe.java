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
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.connector.paimon.service.PaimonStartupRecoveryIntegrationTest;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.EngineAnchorProbe.check;
import static org.mockito.Mockito.*;

/** Opt-in coupled probe. The shell supplies an isolated warehouse, progress record, task ID and JVM per phase. */
public final class EnginePaimonReplayProbe {
    private static final String A = "orders_a", B = "orders_b";
    private static final List<String> SOURCE = List.of("source");
    private final EngineAnchorProbe.ProgressSink progress;
    private final HazelcastTargetPdkBaseNode target;
    private final TapConnectorContext context;
    private final PaimonConnector connector = new PaimonConnector();
    private final ConnectorFunctions functions = new ConnectorFunctions();
    private final Catalog observer;
    private volatile Long bBeforeP2;
    private final AtomicReference<Throwable> callbackReadFailure = new AtomicReference<>();

    public static void main(String[] args) {
        int exit = 0;
        try {
            Path base = Path.of(args[0]);
            String phase = args[1];
            ObjectId id = new ObjectId(args[2]);
            String uri = System.getenv("PROBE_MONGO_URI");
            if (uri != null && !uri.isBlank()) {
                MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
                        .applyToClusterSettings(c -> c.serverSelectionTimeout(4, TimeUnit.SECONDS)).build();
                try (MongoClient client = MongoClients.create(settings)) {
                    var db = client.getDatabase("paimon_local_recovery_e2e");
                    db.runCommand(new Document("ping", 1));
                    var collection = db.getCollection("engine_paimon_probe").withWriteConcern(WriteConcern.MAJORITY);
                    exit = run(base, phase, id, collection);
                }
            } else {
                exit = run(base, phase, id, null);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            exit = 1;
        }
        System.exit(exit);
    }

    private static int run(Path base, String phase, ObjectId id, MongoCollection<Document> collection) throws Throwable {
        var sink = collection == null ? new FileProgress(base.resolve("progress.properties"), id)
                : new EngineAnchorProbe.ProgressSink(collection, id, phase.equals("initial") || phase.equals("continuous"));
        if (phase.equals("cleanup")) { sink.close(); return 0; }
        var probe = new EnginePaimonReplayProbe(base, sink);
        switch (phase) {
            case "initial" -> {
                probe.connector.onStart(probe.context);
                probe.stageInitial();
                System.out.println("PASS A=OVER; stream anchor absent; both tables unpublished; halt=42");
                System.out.flush();
                Runtime.getRuntime().halt(42);
            }
            case "guard-initial" -> {
                probe.assertRestartRefused();
                probe.verifyInitial();
                System.out.println("PASS refused unverified source restart after silent target recovery of all four rows");
            }
            case "continuous" -> {
                probe.connector.onStart(probe.context);
                probe.stageInitial();
                for (String table : List.of(A, B)) {
                    probe.functions.getAfterInitialSyncFunction().afterInitialSync(probe.context, schema(table));
                }
                probe.verifyInitial();
                probe.sendOffset(EngineAnchorProbe.heartbeat(SyncStage.CDC, "P1"));
                EngineAnchorProbe.restore(sink.read(), SyncTypeEnum.CDC, "P1");
                sink.rejectNext = true;
                probe.replay(true);
                // Deliver the same source segment again while the original target is still running.
                // A decoded cursor in the fixture does not authorize a production target restart.
                probe.replay(false);
                System.out.flush();
                Runtime.getRuntime().halt(43);
            }
            case "guard-cdc" -> {
                EngineAnchorProbe.restore(sink.read(), SyncTypeEnum.CDC, "P2");
                probe.assertRestartRefused();
                probe.verifyAtP2();
                System.out.println("PASS refused unverified restart even with P2 in Manager; target data still matches P2");
            }
            default -> throw new IllegalArgumentException("Unknown phase " + phase);
        }
        probe.connector.onStop(probe.context);
        probe.observer.close();
        return 0;
    }

    private EnginePaimonReplayProbe(Path base, EngineAnchorProbe.ProgressSink sink) throws Throwable {
        this.progress = sink;
        TaskDto task = new TaskDto();
        task.setId(sink.id);
        DataProcessorContext processor = mock(DataProcessorContext.class);
        when(processor.getTaskDto()).thenReturn(task);
        TableNode node = mock(TableNode.class);
        when(node.getId()).thenReturn("target");
        doReturn(node).when(processor).getNode();

        target = mock(HazelcastTargetPdkBaseNode.class);
        EngineAnchorProbe.configureTarget(target, processor, sink, this::sourceFlushed);
        target.writeStrategy = "updateOrInsert";
        doCallRealMethod().when(target).handleTapdataRecordEvent(any());
        doCallRealMethod().when(target).handleTapdataEventDML(anyList(), any(), anyList(), any(), any());
        context = target.getConnectorNode().getConnectorContext();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        when(target.getConnectorNode().getConnectorFunctions().getProcessControlFunction())
                .thenReturn(functions.getProcessControlFunction());
        Path warehouse = Files.createDirectories(base.resolve("warehouse"));
        doReturn(DataMap.create().kv("warehouse", warehouse.toString()).kv("database", "default")
                .kv("diskTmpDir", Files.createDirectories(base.resolve("spill")).toString())
                .kv("bucketMode", "fixed").kv("bucketCount", 2).kv("enableAutoCompaction", false))
                .when(context).getConnectionConfig();
        doReturn(EngineAnchorProbe.targetNodeConfig(DataMap.create().kv("localRecoveryDir", base.resolve("recovery").toString())
                .kv("cdcCommitTrigger", "timeOrRows").kv("batchAccumulationSize", 2).kv("commitIntervalMs", 4000)
                .kv("enablePrimaryKeyUpdate", true), SyncTypeEnum.INITIAL_SYNC_CDC)).when(context).getNodeConfig();
        doReturn(PaimonStartupRecoveryIntegrationTest.taskState(base.resolve("task.properties"))).when(context).getStateMap();
        doReturn(mock(Log.class)).when(context).getLog();
        observer = CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(Map.of("warehouse", warehouse.toUri().toString()))));
    }

    private void stageInitial() throws Throwable {
        for (String name : List.of(A, B)) {
            functions.getCreateTableV2Function().createTable(context, new TapCreateTableEvent().table(schema(name)));
        }
        sendOffset(EngineAnchorProbe.heartbeat(SyncStage.INITIAL_SYNC, "P0"));
        write(A, "INITIAL_SYNC", null, List.of(insert(A, 1, "alpha"), insert(A, 2, "drop")));
        write(B, "INITIAL_SYNC", null, List.of(insert(B, 7, "old"), insert(B, 8, "keep")));
        TapdataCompleteTableSnapshotEvent over = new TapdataCompleteTableSnapshotEvent(A);
        over.setSyncStage(SyncStage.INITIAL_SYNC);
        over.setNodeIds(SOURCE);
        over.setBatchOffset(Map.of(BatchOffsetUtil.BATCH_READ_CONNECTOR_STATUS, "OVER"));
        sendOffset(over);
        check(target.saveToSnapshot(), "OVER saved");
        check(BatchOffsetUtil.batchIsOverOfTable(EngineAnchorProbe.restore(progress.read(),
                SyncTypeEnum.INITIAL_SYNC_CDC, null), A), "A durable OVER without CDC anchor");
        check(head(A) == null && head(B) == null, "both snapshots unpublished at crash");
    }

    private void verifyInitial() throws Exception {
        check(BatchOffsetUtil.batchIsOverOfTable(EngineAnchorProbe.restore(progress.read(),
                SyncTypeEnum.INITIAL_SYNC_CDC, null), A), "durable A preserved after restart");
        check(read(A).equals(Map.of(1, "alpha", 2, "drop")) && read(B).equals(Map.of(7, "old", 8, "keep")),
                "quiet recovery of complete A and partial B before new source input");
        check(Long.valueOf(1).equals(head(A)) && Long.valueOf(1).equals(head(B)), "one initial snapshot per table");
    }

    private void assertRestartRefused() throws Throwable {
        IllegalStateException startupFailure;
        try {
            connector.onStart(context);
            throw new AssertionError("accepted an existing binding without an authoritative source restart position");
        } catch (IllegalStateException expected) {
            check(expected.getMessage().contains("PAIMON_RECOVERY_SOURCE_POSITION_UNVERIFIED"),
                    "unexpected rejection: " + expected.getMessage());
            startupFailure = expected;
        }
        Map<String, Object> before = progress.read();
        try {
            write(A, "CDC", "unverified", List.of(insert(A, 99, "must not appear")));
            throw new AssertionError("DML accepted after the rejected restart");
        } catch (IllegalStateException expected) {
            check(expected.getCause() == startupFailure, "late DML carries the failed startup diagnosis");
        }
        sendOffset(EngineAnchorProbe.heartbeat(SyncStage.CDC, "unverified"));
        check(progress.sourceFlushes.isEmpty() && before.equals(progress.read()),
                "late heartbeat advanced the source or Manager after the rejected restart");
    }

    private long countP2Flushes() {
        return progress.sourceFlushes.stream().filter("P2"::equals).count();
    }

    private void replay(boolean failedPersistence) throws Throwable {
        long previousFlushes = countP2Flushes();
        write(A, "CDC", "P2", List.of(new TapUpdateRecordEvent().init().table(A).before(row(1,"alpha")).after(row(3,"moved")),
                new TapDeleteRecordEvent().init().table(A).before(row(2,"drop"))));
        bBeforeP2 = head(B);
        write(B, "CDC", "P2", List.of(new TapUpdateRecordEvent().init().table(B).before(row(7,"old")).after(row(7,"changed"))));
        check(read(A).equals(Map.of(3,"moved")), "A committed at the size threshold");
        check(Objects.equals(bBeforeP2, head(B)), "B remains unpublished before H");
        if (failedPersistence) check(read(B).equals(Map.of(7,"old",8,"keep")), "B still at previous source state");
        sendOffset(EngineAnchorProbe.heartbeat(SyncStage.CDC, "P2"));
        check(countP2Flushes() == previousFlushes, "held heartbeat produced no early source flush");
        EngineAnchorProbe.restore(progress.read(), SyncTypeEnum.CDC, "P1");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (countP2Flushes() == previousFlushes || !Boolean.valueOf(!failedPersistence).equals(progress.lastWriteSucceeded)) {
            if (System.nanoTime() >= deadline) throw new AssertionError("timed out awaiting real engine progress write");
            Thread.sleep(25);
        }
        if (callbackReadFailure.get() != null) {
            throw new AssertionError("target rows or snapshot were incorrect inside the source flush", callbackReadFailure.get());
        }
        verifyAtP2();
        EngineAnchorProbe.restore(progress.read(), SyncTypeEnum.CDC, failedPersistence ? "P1" : "P2");
        System.out.println("PASS P2: A={3=moved} B={7=changed,8=keep}; heads=" + head(A) + "/" + head(B)
                + "; sourceFlush=P2; durable=" + (failedPersistence ? "P1" : "P2; same-process replay complete; halt=43"));
    }

    // Read at the actual external-side-effect boundary. Polling only after the callback would
    // miss an implementation that acknowledged the source first and then published the table.
    private void sourceFlushed(Object offset) {
        if ("P2".equals(offset)) {
            try {
                check(!Objects.equals(bBeforeP2, head(B)), "B snapshot visible at P2 source flush");
                verifyAtP2();
            } catch (Throwable failure) {
                callbackReadFailure.compareAndSet(null, failure);
            }
        }
        progress.sourceFlushes.add(offset);
    }

    private void verifyAtP2() throws Exception {
        check(read(A).equals(Map.of(3,"moved")) && read(B).equals(Map.of(7,"changed",8,"keep")),
                "ordinary Paimon batch read matches all expected keys, values and deletes at P2");
    }

    private void sendOffset(TapdataEvent event) throws Exception { EngineAnchorProbe.dispatchSerial(target, event); }

    private void write(String name, String stage, String offset, List<TapRecordEvent> rows) throws Throwable {
        List<TapEvent> staged = new ArrayList<>();
        AtomicReference<TapdataEvent> last = new AtomicReference<>();
        for (TapRecordEvent row : rows) {
            TapdataEvent input = new TapdataEvent();
            input.setTapEvent(row);
            input.setSyncStage(SyncStage.valueOf(stage));
            input.setNodeIds(SOURCE);
            input.setStreamOffset(offset);
            input.setSourceTime(42L);
            target.handleTapdataEventDML(staged, new AtomicBoolean(), new ArrayList<>(), last, input);
        }
        List<TapRecordEvent> converted = staged.stream().map(TapRecordEvent.class::cast).toList();
        check(converted.size() == rows.size(), "engine forwarded each DML to the PDK");
        functions.getWriteRecordFunction().writeRecord(context, converted, schema(name), result -> {});
    }

    private static TapTable schema(String name) { return new TapTable(name).add(new TapField("id","INT").primaryKeyPos(1)).add(new TapField("value","STRING")); }
    private static Map<String,Object> row(int id,String val) { return Map.of("id",id,"value",val); }
    private static TapRecordEvent insert(String table,int id,String val) { return new TapInsertRecordEvent().init().table(table).after(row(id,val)); }
    private FileStoreTable physical(String table) throws Exception { return (FileStoreTable) observer.getTable(Identifier.create("default",table)); }
    private Long head(String table) throws Exception { return physical(table).snapshotManager().latestSnapshotIdFromFileSystem(); }
    private Map<Integer,String> read(String table) throws Exception {
        ReadBuilder builder = physical(table).newReadBuilder();
        Map<Integer,String> rows = new TreeMap<>();
        try (RecordReader<InternalRow> reader = builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> check(rows.put(row.getInt(0),row.getString(1).toString()) == null, "duplicate physical key"));
        }
        return rows;
    }

    private static final class FileProgress extends EngineAnchorProbe.ProgressSink {
        private final Path file;
        FileProgress(Path file, ObjectId id) throws IOException {
            super(null,id,false);
            this.file = file;
            if (Files.exists(file)) {
                Properties data = new Properties();
                try (var in = Files.newInputStream(file)) { data.load(in); }
                for (String edge : data.stringPropertyNames()) memory.put(edge,data.getProperty(edge));
            }
        }
        @Override synchronized void write(Map<String,String> payload) {
            super.write(payload);
            Properties data = new Properties();
            data.putAll(memory);
            try {
                Path tmp = file.resolveSibling("progress.new");
                try (var out = Files.newOutputStream(tmp)) { data.store(out,null); }
                Files.move(tmp,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) { throw new UncheckedIOException(e); }
        }
        @Override synchronized Map<String,Object> read() { return super.read(); }
    }
}
