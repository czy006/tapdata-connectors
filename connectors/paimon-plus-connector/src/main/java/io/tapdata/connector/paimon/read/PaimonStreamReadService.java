package io.tapdata.connector.paimon.read;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.service.PaimonStopController;
import io.tapdata.connector.paimon.service.PaimonStopResources;
import io.tapdata.connector.paimon.util.PaimonFailures;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan.Plan;
import org.apache.paimon.types.RowKind;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Owns stream state for one PaimonService lifecycle: initial offsets, scans, protection and active sessions.
 * Paimon still plans splits and reads rows; this class coordinates TapData adaptation without a Flink runtime.
 *
 * <p>The host supplies ingress admission, catalog lookup and managed readers through Runtime; it closes the Catalog.
 * The shared stop lock, controller and resource ledger serialize session registration against host shutdown.
 * The retained root remains the host bound to stopController, preserving ownership of all shared resources.
 */
public final class PaimonStreamReadService {
    /** Exposes only the host capabilities needed by stream reads, without exposing PaimonService internals. */
    public interface Runtime {
        AutoCloseable enterIngress(String action) throws Exception;
        Table loadTable(Identifier identifier) throws Exception;
        RecordReader<InternalRow> createReader(
                PaimonStopController.CheckedSupplier<RecordReader<InternalRow>> factory) throws Exception;
    }

    private final PaimonConfig config;
    private final Runtime runtime;
    private final PaimonStopController stopController;
    private final PaimonStopResources stopResources;
    private final Object closeCoordinationLock;
    private final AtomicReference<PaimonStreamReadSession> sourceSession = new AtomicReference<>();
    private final AtomicReference<Throwable> sourceProtectionFailure = new AtomicReference<>();
    private volatile SourceProtection sourceProtection;
    private static final class SourceProtection {
        final PaimonReadOffset offset;
        final Set<String> names;
        final PaimonConsumerLease lease;
        final PaimonStopResources.Slot slot;
        final long created = System.nanoTime();
        SourceProtection(PaimonReadOffset offset, Set<String> names, PaimonConsumerLease lease, PaimonStopResources.Slot slot) {
            this.offset=offset;this.names=new HashSet<>(names);this.lease=lease;this.slot=slot;
        }
        boolean matches(PaimonReadOffset other, Set<String> requested) {
            if(!offset.readEpoch().equals(other.readEpoch()) || !names.equals(requested))return false;
            for(String name:names) {
                PaimonReadOffset.Position first=offset.position(name), next=other.position(name);
                if(first.startSnapshot!=next.startSnapshot || !first.physicalTableHash.equals(next.physicalTableHash))return false;
            }
            return true;
        }
    }

    public PaimonStreamReadService(PaimonConfig config, Runtime runtime,
            PaimonStopController stopController, PaimonStopResources stopResources, Object closeCoordinationLock) {
        this.config = Objects.requireNonNull(config, "config");
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.stopController = Objects.requireNonNull(stopController, "stopController");
        this.stopResources = Objects.requireNonNull(stopResources, "stopResources");
        this.closeCoordinationLock = Objects.requireNonNull(closeCoordinationLock, "closeCoordinationLock");
    }

    /** Called under the shared lock when host shutdown starts; requests cancellation without waiting under the lock. */
    public void cancelActiveSession() {
        PaimonStreamReadSession active = sourceSession.get();
        if (active != null) { active.cancel(); }
    }

    /**
     * Resolve one shared timestamp into a versioned, next-to-consume envelope for every table.
     * Metadata failures fail the entire initialization; protected mode registers all leases before returning.
     */
    public Object timestampToStreamOffset(List<String> tables, Long timestamp, Runnable threadContext) throws Exception {
        try (AutoCloseable ignored = runtime.enterIngress("timestampToStreamOffset")) {
            return timestampToStreamOffsetAdmitted(tables, timestamp, threadContext);
        }
    }

    private Object timestampToStreamOffsetAdmitted(List<String> tables, Long timestamp, Runnable threadContext) throws Exception {
        // Use one time boundary for all tables; this connector policy does not create a cross-table snapshot transaction.
        // See PaimonReadPositionResolver#atTimestamp for equality handling and the difference from native TimeTravelUtil.
        long now = System.currentTimeMillis();
        long boundary = timestamp == null ? now : timestamp;
        Map<String, FileStoreTable> resolved = resolveSourceTables(tables);
        Map<String, PaimonReadOffset.Position> positions = new LinkedHashMap<>();
        for (Map.Entry<String, FileStoreTable> entry : resolved.entrySet()) {
            PaimonReadPositionResolver resolver = new PaimonReadPositionResolver(entry.getValue(), entry.getKey(),
                    () -> !stopController.isStarted());
            long next = resolver.atTimestamp(boundary, now);
            resolver.validate(next);
            positions.put(entry.getKey(), new PaimonReadOffset.Position(
                    PaimonReadPositionResolver.identity(entry.getValue()), next, next));
        }
        PaimonReadOffset offset = new PaimonReadOffset(UUID.randomUUID().toString(), positions);
        ensureSourceProtection(offset, resolved, threadContext);
        return offset.toMap();
    }

    private Map<String, FileStoreTable> resolveSourceTables(List<String> names) throws Exception {
        if (names == null || names.isEmpty()) { throw new IllegalArgumentException("Stream read requires tables"); }
        Map<String, FileStoreTable> result = new LinkedHashMap<>();
        Set<String> identities = new HashSet<>();
        for (String name : names) {
            if (name == null || name.trim().isEmpty() || result.containsKey(name)) {
                throw new IllegalArgumentException("Empty or duplicate stream table: " + name);
            }
            Table table = runtime.loadTable(Identifier.create(config.getDatabase(), name));
            if (!(table instanceof FileStoreTable)) { throw new IllegalArgumentException("Unsupported source table: " + name); }
            FileStoreTable file = (FileStoreTable) table;
            if (!identities.add(PaimonReadPositionResolver.identity(file))) {
                throw new IllegalArgumentException("Duplicate physical source table: " + name);
            }
            result.put(name, file);
        }
        return result;
    }

    public void checkSourceProtection() {
        Throwable failure = sourceProtectionFailure.get();
        if (failure != null) { throw new IllegalStateException("Paimon source protection failed; restart required", failure); }
        SourceProtection protection = sourceProtection;
        if (protection != null) { protection.lease.check(); }
    }

    private void ensureSourceProtection(PaimonReadOffset offset, Map<String, FileStoreTable> tables,
            Runnable threadContext) throws Exception {
        // Register fixed-start protection when creating the initial offset so it also covers the full-read phase.
        // Stream reads reuse protection for the same epoch; waiting until the full read ends would leave history unprotected.
        checkSourceProtection();
        if ("none".equals(config.getStreamReadConsumerProtection())) { return; }
        SourceProtection protection;
        synchronized (closeCoordinationLock) {
            if (stopController.isStarted()) { throw new IllegalStateException("Source protection is stopping"); }
            protection = sourceProtection;
            if (protection == null) {
                String attempt = UUID.randomUUID().toString();
                List<PaimonConsumerRegistration> registrations = new ArrayList<>();
                for (Map.Entry<String, FileStoreTable> entry : tables.entrySet()) {
                    registrations.add(new PaimonConsumerRegistration(entry.getValue(), entry.getKey(), offset,
                            attempt, () -> sourceProtectionFailure.get() == null));
                }
                PaimonConsumerLease lease = PaimonConsumerLease.create(registrations,
                        TimeUnit.SECONDS.toMillis(config.getStopTimeoutSeconds()), () -> true, failure -> {
                            sourceProtectionFailure.compareAndSet(null, failure);
                            PaimonStreamReadSession session = sourceSession.get();
                            if (session != null) { session.fail(failure); }
                        }, threadContext);
                PaimonStopResources.Slot slot = stopResources.reserve("source Consumer protection");
                protection = new SourceProtection(offset, tables.keySet(), lease, slot);
                slot.bind(protection); sourceProtection = protection;
            } else if (!protection.matches(offset, tables.keySet())) {
                throw new IllegalStateException("Another source read epoch is protected; restart Service to initialize a new epoch");
            }
        }
        try {
            protection.lease.initialize(() -> !stopController.isStarted());
            checkSourceProtection();
        } catch (Exception | Error failure) {
            sourceProtectionFailure.compareAndSet(null, failure);
            try { closeSourceProtection(protection.created + TimeUnit.SECONDS.toNanos(config.getStopTimeoutSeconds())); }
            catch (Throwable closeFailure) { failure.addSuppressed(closeFailure); }
            throw failure;
        }
    }

    public void closeSourceProtection(long deadline) throws Exception {
        SourceProtection protection = sourceProtection;
        if (protection == null) { return; }
        try {
            protection.lease.close(deadline);
            synchronized (closeCoordinationLock) {
                protection.slot.released();
                if (sourceProtection == protection) { sourceProtection = null; }
            }
        } catch (Exception | Error failure) {
            stopController.retain(failure, this);
            throw failure;
        }
    }

    /** Synchronous PDK invocation; only completed plans can publish a next-snapshot offset. */
    public void streamRead(List<String> tables, Object offsetState, int eventBatchSize,
            StreamReadConsumer consumer, TapConnectorContext context, Supplier<Boolean> running) throws Exception {
        if (!Boolean.TRUE.equals(running.get())) { return; }
        PaimonReadOffset offset = PaimonReadOffset.parse(offsetState);
        PaimonStreamReadSession session = new PaimonStreamReadSession(offset, eventBatchSize,
                config.getStreamReadFlushIntervalMs(), () -> Boolean.TRUE.equals(running.get()), (events, state) -> {
                    checkSourceProtection(); consumer.accept(events, state);
                });
        executeSession(session, () -> {
            Map<String, FileStoreTable> resolved = resolveSourceTables(tables);
            List<PaimonStreamReadCoordinator.Source> sources = new ArrayList<>();
            for (Map.Entry<String, FileStoreTable> entry : resolved.entrySet()) {
                session.checkActive();
                String name = entry.getKey();
                FileStoreTable physical = entry.getValue();
                PaimonReadOffset.Position position = offset.position(name);
                if (!position.physicalTableHash.equals(PaimonReadPositionResolver.identity(physical))) {
                    throw new IllegalArgumentException("Paimon offset belongs to another physical table: " + name);
                }
                PaimonReadPositionResolver resolver = new PaimonReadPositionResolver(physical, name, session::isActive);
                resolver.validate(position.nextSnapshot);
                TapTable tap = context.getTableMap().get(name);
                PaimonReadSchemaGuard guard = new PaimonReadSchemaGuard(physical, tap, resolver);
                // Paimon release-1.3 @ 5c59e6cb01ed0b29563371f56e14fcade4597a2e:
                // DataTableStreamScan#checkpoint/restore use the same nextSnapshotId; do not add one during recovery.
                // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java#L307
                // consumer.ignore-progress keeps the Engine offset authoritative over native Consumer progress.
                // Native createFollowUpScanner selects delta or changelog according to changelog-producer.
                // The connector does not assemble file deltas or promise full CDC images for tables without changelogs.
                // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java#L266
                FileStoreTable table = physical.copy(Collections.singletonMap("consumer.ignore-progress", "true"));
                ReadBuilder builder = table.newReadBuilder();
                StreamTableScan scan = builder.newStreamScan();
                scan.restore(position.nextSnapshot);
                TableRead read = builder.newRead();
                PaimonSourceRowDecoder decoder = new PaimonSourceRowDecoder(table.rowType().getFields());
                sources.add(new PaimonStreamReadCoordinator.Source() {
                    public String name() { return name; }
                    public Long checkpoint() { return scan.checkpoint(); }
                    public void validatePosition(long next) throws Exception {
                        try (PaimonStreamReadSession.Admission ignored = session.admit()) { checkSourceProtection(); resolver.validate(next); }
                    }
                    public void beforePlan() {
                        try (PaimonStreamReadSession.Admission ignored = session.admit()) { checkSourceProtection(); guard.beforePlan(); }
                    }
                    public Plan plan() {
                        try (PaimonStreamReadSession.Admission ignored = session.admit()) { checkSourceProtection(); return scan.plan(); }
                    }
                    public void beforeRead(Plan plan) throws Exception {
                        try (PaimonStreamReadSession.Admission ignored = session.admit()) { guard.beforeRead(plan); }
                    }
                    public RecordReader<InternalRow> reader(Split split) throws Exception {
                        try (PaimonStreamReadSession.Admission ignored = session.admit()) {
                            return new PaimonSourceReader(runtime.createReader(() -> read.createReader(split)), session, PaimonStreamReadService.this::checkSourceProtection);
                        }
                    }
                    public io.tapdata.entity.event.TapEvent convert(InternalRow row) {
                        return createEventFromRowKind(row, decoder.decodeForEvent(row), name);
                    }
                });
            }
            ensureSourceProtection(offset, resolved, context::configContext);
            session.checkActive();
            // PDK API 2.0.8-SNAPSHOT io/tapdata/pdk/apis/consumer/StreamReadConsumer.java:29-50.
            // State notifications only; do not opt into asyncMethodAndNoRetry or infer durable ack.
            boolean started = false;
            Throwable failure = null;
            try {
                started = true;
                consumer.streamReadStarted();
                session.run(sources, config.getStreamReadPollIntervalMs(),
                        TimeUnit.SECONDS.toMillis(config.getStopTimeoutSeconds()), context::configContext);
            } catch (Throwable error) { failure = error; }
            finally {
                if (started) {
                    try { consumer.streamReadEnded(); }
                    catch (Throwable endFailure) { failure = PaimonFailures.append(failure, endFailure); }
                }
            }
            if (failure != null) { rethrow(failure); }
        });
    }

    /**
     * Maps native changes to TapData events: +I/+U become inserts and -D/-U become deletes.
     * Does not pair images or create cross-event transactions; target keys and conflict policies determine replay behavior.
     * Paimon permits UPDATE_AFTER as a standalone idempotent update, so paired images cannot be assumed.
     * referenceTime is the local conversion time, not the snapshot commit time or source business time.
     * Source: RowKind semantics at the pinned release-1.3 revision:
     * https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-api/src/main/java/org/apache/paimon/types/RowKind.java#L29
     *
     * @param row       internal row
     * @param data      converted data map
     * @param tableName table name
     * @return TapInsertRecordEvent or TapDeleteRecordEvent; an unknown kind returns null and fails the stream caller
     */
    private io.tapdata.entity.event.TapEvent createEventFromRowKind(InternalRow row, Map<String, Object> data, String tableName) {
        RowKind rowKind = row.getRowKind();

        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                // For INSERT and UPDATE_AFTER, create insert event
                io.tapdata.entity.event.dml.TapInsertRecordEvent insertEvent =
                        new io.tapdata.entity.event.dml.TapInsertRecordEvent().init();
                insertEvent.setTableId(tableName);
                insertEvent.setAfter(data);
                insertEvent.setReferenceTime(System.currentTimeMillis());
                return insertEvent;

            case DELETE:
            case UPDATE_BEFORE:
                // For DELETE and UPDATE_BEFORE, create delete event
                io.tapdata.entity.event.dml.TapDeleteRecordEvent deleteEvent =
                        new io.tapdata.entity.event.dml.TapDeleteRecordEvent().init();
                deleteEvent.setTableId(tableName);
                deleteEvent.setBefore(data);
                deleteEvent.setReferenceTime(System.currentTimeMillis());
                return deleteEvent;

            default:
                // The stream Coordinator rejects null instead of silently skipping a row and advancing the offset.
                return null;
        }
    }

    /** Source ownership uses the same ledger and stop barrier as finite reads and writers. */
    public void executeSession(PaimonStreamReadSession session,
            PaimonStopController.CheckedRunnable body) throws Exception {
        try (AutoCloseable ignored = runtime.enterIngress("streamRead")) {
            PaimonStopResources.Slot registration;
            synchronized (closeCoordinationLock) {
                if (stopController.isStarted() || !sourceSession.compareAndSet(null, session)) {
                    throw new IllegalStateException("Paimon source is stopping or another stream invocation is active");
                }
                registration = stopResources.reserve("stream source session");
                registration.bind(session);
            }
            Throwable failure = null;
            try { body.run(); }
            catch (Throwable error) { failure = error; }
            finally {
                session.cancel();
                try { closeSourceProtection(session.cancelledAtNanos() + TimeUnit.SECONDS.toNanos(config.getStopTimeoutSeconds())); }
                catch (Throwable closeFailure) { failure = PaimonFailures.append(failure, closeFailure); }
                if (session.resourcesTerminated()) {
                    registration.released();
                    sourceSession.compareAndSet(session, null);
                } else {
                    Throwable unclosed = new IllegalStateException("Paimon source termination not proved; shared resources retained");
                    failure = PaimonFailures.append(failure, unclosed);
                    stopController.retain(failure, this);
                }
            }
            if (failure != null) { rethrow(failure); }
        }
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IllegalStateException("Paimon service failed", failure);
    }

}
