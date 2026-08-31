package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonCommitStateStore;

import io.tapdata.connector.paimon.exception.PaimonDynamicBucketPollutedException;
import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import io.tapdata.connector.paimon.write.bucket.DefaultPaimonBucketWriterRuntimeFactory;
import io.tapdata.entity.utils.cache.KVMap;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One-time exact-primary-key pollution preflight for legacy HASH_DYNAMIC tables. */
public final class PaimonDynamicBucketPreflight {

    private static final String MARKER_PREFIX = "paimon.hash-dynamic-preflight-v1.";
    private static final PreflightRuntimeFactory PRODUCTION_RUNTIME =
            new PreflightRuntimeFactory() {
                @Override
                public FileStoreTable createRuntimeTable(FileStoreTable source) {
                    return PaimonRuntimeTableFactory.create(source);
                }

                @Override
                public PreflightIoOwner createIoOwner(String configuredTmpDirs) {
                    PaimonSpillDirCleaner.IOManagerBuildResult built =
                            PaimonSpillDirCleaner.resolveAndCreateIOManager(configuredTmpDirs);
                    return new PreflightIoOwner(built.ioManager(), built.spillDirs());
                }

                @Override
                public GlobalIndexAssigner createAssigner(FileStoreTable runtimeTable) {
                    return DefaultPaimonBucketWriterRuntimeFactory.INSTANCE
                            .createGlobalIndexAssigner(runtimeTable);
                }

                @Override
                public RecordReader<InternalRow> createBootstrapReader(
                        FileStoreTable runtimeTable) throws Exception {
                    return DefaultPaimonBucketWriterRuntimeFactory.INSTANCE
                            .createIndexBootstrapReader(runtimeTable);
                }
            };

    private PaimonDynamicBucketPreflight() {
    }

    public static void ensureHashDynamicValidated(
            KVMap<Object> stateMap,
            String warehouse,
            String tableKey,
            FileStoreTable table,
            String configuredTmpDirs) throws Exception {
        String markerKey = MARKER_PREFIX + PaimonCommitStateStore.stateKey(
                warehouse, table.location().toUri().toString());
        // FileStorePathFactory.uuid() is intentionally a per-instance random file-name prefix in
        // Paimon 1.3.1. It changes whenever the table/store is reloaded and therefore must never be
        // used as a durable table identity. Table.uuid() is the metastore UUID (or the filesystem
        // table's stable creation identity) and changes when the table is recreated.
        String expectedMarker = Objects.requireNonNull(
                table.uuid(), "Paimon table UUID is required for HASH_DYNAMIC preflight");
        Object existing = stateMap.get(markerKey);
        if (expectedMarker.equals(existing)) {
            return;
        }
        if (existing != null && !(existing instanceof String)) {
            throw new IllegalStateException("Invalid HASH_DYNAMIC preflight marker type");
        }

        validateExactPrimaryKeyUniqueness(tableKey, table, configuredTmpDirs);

        if (existing == null) {
            Object raced = stateMap.putIfAbsent(markerKey, expectedMarker);
            if (raced != null && !expectedMarker.equals(raced)) {
                throw new IllegalStateException(
                        "HASH_DYNAMIC table identity changed during pollution preflight");
            }
        } else {
            // The physical path was recreated with a new Paimon table UUID. The single-writer
            // lifecycle lock permits replacing only this validation marker after a fresh scan.
            stateMap.put(markerKey, expectedMarker);
        }
        if (!expectedMarker.equals(stateMap.get(markerKey))) {
            throw new IllegalStateException("HASH_DYNAMIC preflight marker was not durably observable");
        }
    }

    private static void validateExactPrimaryKeyUniqueness(
            String tableKey, FileStoreTable table, String configuredTmpDirs) throws Exception {
        validateExactPrimaryKeyUniqueness(
                tableKey, table, configuredTmpDirs, PRODUCTION_RUNTIME);
    }

    static void validateExactPrimaryKeyUniqueness(
            String tableKey,
            FileStoreTable table,
            String configuredTmpDirs,
            PreflightRuntimeFactory runtimeFactory) throws Exception {
        Objects.requireNonNull(runtimeFactory, "runtimeFactory");
        PreflightIoOwner ioOwner = null;
        GlobalIndexAssigner staged = null;
        BootstrapDrainState bootstrapDrain = new BootstrapDrainState();
        Throwable failure = null;
        try {
            FileStoreTable runtimeTable = Objects.requireNonNull(
                    runtimeFactory.createRuntimeTable(table), "runtime table");
            requireAsyncDisabled(runtimeTable);
            FileStoreTable validationTable = withoutIndexTtl(runtimeTable);
            requireAsyncDisabled(validationTable);

            ioOwner = Objects.requireNonNull(
                    runtimeFactory.createIoOwner(configuredTmpDirs), "preflight IO owner");
            staged = Objects.requireNonNull(
                    runtimeFactory.createAssigner(validationTable), "preflight assigner");
            Long snapshotBefore =
                    validationTable.snapshotManager().latestSnapshotIdFromFileSystem();
            staged.open(0L, ioOwner.ioManager(), 1, 0, (row, bucket) -> { });
            bootstrap(
                    staged,
                    runtimeFactory.createBootstrapReader(validationTable),
                    bootstrapDrain);
            staged.endBoostrap(false);
            Long snapshotAfter =
                    validationTable.snapshotManager().latestSnapshotIdFromFileSystem();
            if (!Objects.equals(snapshotBefore, snapshotAfter)) {
                throw new IllegalStateException(
                        "Paimon table changed during HASH_DYNAMIC pollution preflight; "
                                + "only one write job per physical table is supported");
            }
        } catch (Throwable operationFailure) {
            failure = wrapIfPolluted(tableKey, operationFailure);
        }

        boolean checkerClosed = false;
        if (staged != null) {
            Throwable closeFailure = close(staged);
            checkerClosed = closeFailure == null;
            failure = merge(failure, closeFailure);
        } else {
            checkerClosed = true;
        }
        if (ioOwner != null && checkerClosed && bootstrapDrain.cleanupSucceeded()) {
            Throwable closeFailure = close(ioOwner.ioManager());
            if (closeFailure == null) {
                closeFailure = unregister(ioOwner.spillDirs());
            }
            failure = merge(failure, closeFailure);
        }

        if (failure != null) {
            rethrow(failure);
        }
    }

    private static void bootstrap(
            GlobalIndexAssigner checker,
            RecordReader<InternalRow> bootstrapReader,
            BootstrapDrainState drainState)
            throws Exception {
        RecordReader<InternalRow> reader =
                Objects.requireNonNull(bootstrapReader, "index bootstrap reader");
        Throwable failure = null;
        try {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                Throwable batchFailure = null;
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        checker.bootstrapKey(row);
                    }
                } catch (Throwable iterationFailure) {
                    batchFailure = iterationFailure;
                }
                try {
                    batch.releaseBatch();
                } catch (Throwable releaseFailure) {
                    drainState.cleanupFailed();
                    batchFailure = merge(batchFailure, releaseFailure);
                }
                if (batchFailure != null) {
                    rethrow(batchFailure);
                }
            }
        } catch (Throwable operationFailure) {
            failure = operationFailure;
        }
        Throwable readerCloseFailure = close(reader);
        if (readerCloseFailure != null) {
            drainState.cleanupFailed();
            failure = merge(failure, readerCloseFailure);
        }
        if (failure != null) {
            rethrow(failure);
        }
    }

    /**
     * IndexBootstrap applies cross-partition-upsert.index-ttl by dropping old splits. That behavior
     * is correct for a live KEY_DYNAMIC index but invalid for an exact historical-pollution scan.
     * Use a read-only dynamic table copy with the TTL removed so every latest-snapshot split is
     * examined; the persisted table options are not changed.
     */
    public static FileStoreTable withoutIndexTtl(FileStoreTable table) {
        if (!table.options().containsKey(CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key())) {
            return table;
        }
        return table.copy(Collections.singletonMap(
                CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key(), null));
    }

    private static void requireAsyncDisabled(FileStoreTable table) {
        if (table.coreOptions().fileReaderAsyncEnabled()) {
            throw new IllegalArgumentException(
                    "HASH_DYNAMIC preflight requires file-reader-async-enabled=false");
        }
    }

    private static Throwable wrapIfPolluted(String tableKey, Throwable failure) {
        if (failure instanceof Exception) {
            return PaimonDynamicBucketPollutedException.wrapIfPolluted(tableKey, failure);
        }
        return failure;
    }

    private static Throwable close(AutoCloseable closeable) {
        try {
            closeable.close();
            return null;
        } catch (Throwable closeFailure) {
            return closeFailure;
        }
    }

    private static Throwable unregister(List<String> spillDirs) {
        try {
            PaimonSpillDirCleaner.unregisterLiveDirs(spillDirs);
            return null;
        } catch (Throwable unregisterFailure) {
            return unregisterFailure;
        }
    }

    private static Throwable merge(Throwable primary, Throwable secondary) {
        if (primary == null) {
            return secondary;
        }
        if (secondary != null && primary != secondary) {
            primary.addSuppressed(secondary);
        }
        return primary;
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    interface PreflightRuntimeFactory {
        FileStoreTable createRuntimeTable(FileStoreTable source) throws Exception;

        PreflightIoOwner createIoOwner(String configuredTmpDirs) throws Exception;

        GlobalIndexAssigner createAssigner(FileStoreTable runtimeTable) throws Exception;

        RecordReader<InternalRow> createBootstrapReader(FileStoreTable runtimeTable)
                throws Exception;
    }

    static final class PreflightIoOwner {
        private final IOManager ioManager;
        private final List<String> spillDirs;

        PreflightIoOwner(IOManager ioManager, List<String> spillDirs) {
            this.ioManager = Objects.requireNonNull(ioManager, "ioManager");
            this.spillDirs = Objects.requireNonNull(spillDirs, "spillDirs");
        }

        IOManager ioManager() {
            return ioManager;
        }

        List<String> spillDirs() {
            return spillDirs;
        }
    }

    private static final class BootstrapDrainState {
        private boolean cleanupSucceeded = true;

        private void cleanupFailed() {
            cleanupSucceeded = false;
        }

        private boolean cleanupSucceeded() {
            return cleanupSucceeded;
        }
    }
}
