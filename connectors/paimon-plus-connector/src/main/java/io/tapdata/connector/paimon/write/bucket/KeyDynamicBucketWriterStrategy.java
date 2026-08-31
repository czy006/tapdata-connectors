package io.tapdata.connector.paimon.write.bucket;

import io.tapdata.connector.paimon.schema.PaimonRowKindField;
import io.tapdata.connector.paimon.service.PaimonRuntimeTableFactory;

import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Global key-index routing strategy for cross-partition primary-key updates.
 *
 * <p>Open, bootstrap, emitted DELETE/INSERT ordering and snapshot fencing follow Paimon 1.3.2:
 * https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java
 * and
 * https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/crosspartition/IndexBootstrap.java
 */
public final class KeyDynamicBucketWriterStrategy extends AbstractPaimonBucketWriterStrategy {

    private final GlobalIndexAssigner assigner;
    private final List<BucketedRow> emittedRows = new ArrayList<>();

    KeyDynamicBucketWriterStrategy(
            PaimonBucketWriterStrategyContext context,
            PaimonBucketWriterRuntimeFactory runtimeFactory)
            throws Exception {
        super(
                requireIoManager(context),
                BucketMode.KEY_DYNAMIC,
                HashDynamicBucketWriterStrategy.requiredPrimaryKeyFields(context.table()));
        PaimonBucketWriterRuntimeFactory runtime =
                Objects.requireNonNull(runtimeFactory, "runtimeFactory");
        this.assigner = createReadyAssigner(context.ioManager(), runtime);
    }

    private GlobalIndexAssigner createReadyAssigner(
            org.apache.paimon.disk.IOManager ioManager,
            PaimonBucketWriterRuntimeFactory runtime) throws Exception {
        FileStoreTable runtimeTable = PaimonRuntimeTableFactory.create(table);
        GlobalIndexAssigner staged = null;
        try {
            staged = Objects.requireNonNull(
                    runtime.createGlobalIndexAssigner(runtimeTable), "globalIndexAssigner");
            Long snapshotBefore =
                    runtimeTable.snapshotManager().latestSnapshotIdFromFileSystem();
            GlobalIndexAssigner active = staged;
            active.open(
                    0L,
                    ioManager,
                    1,
                    0,
                    (row, bucket) -> emittedRows.add(new BucketedRow(row, bucket)));
            bootstrap(active, runtime.createIndexBootstrapReader(runtimeTable));
            active.endBoostrap(false);
            Long snapshotAfter =
                    runtimeTable.snapshotManager().latestSnapshotIdFromFileSystem();
            if (!Objects.equals(snapshotBefore, snapshotAfter)) {
                throw new IllegalStateException(
                        "Paimon table changed while bootstrapping KEY_DYNAMIC index; "
                                + "only one write job per physical table is supported");
            }
            emittedRows.clear();
            return active;
        } catch (Throwable failure) {
            try {
                if (staged != null) {
                    staged.close();
                }
            } catch (Throwable closeFailure) {
                addSuppressed(failure, closeFailure);
            }
            rethrow(failure);
            throw new AssertionError("unreachable");
        }
    }

    private static PaimonBucketWriterStrategyContext requireIoManager(
            PaimonBucketWriterStrategyContext context) {
        Objects.requireNonNull(context, "context");
        if (context.ioManager() == null) {
            throw new IllegalStateException("KEY_DYNAMIC bucket mode requires an IOManager");
        }
        return context;
    }

    private static void bootstrap(
            GlobalIndexAssigner staged, RecordReader<InternalRow> bootstrapReader) throws Exception {
        try (RecordReader<InternalRow> reader =
                     Objects.requireNonNull(bootstrapReader, "indexBootstrapReader")) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                Throwable failure = null;
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        staged.bootstrapKey(row);
                    }
                } catch (Throwable iterationFailure) {
                    failure = iterationFailure;
                }
                try {
                    batch.releaseBatch();
                } catch (Throwable releaseFailure) {
                    if (failure == null) {
                        failure = releaseFailure;
                    } else {
                        addSuppressed(failure, releaseFailure);
                    }
                }
                if (failure != null) {
                    rethrow(failure);
                }
            }
        }
    }

    private static void addSuppressed(Throwable primary, Throwable suppressed) {
        if (primary != suppressed) {
            primary.addSuppressed(suppressed);
        }
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

    @Override
    protected void doWrite(InternalRow row) throws Exception {
        emittedRows.clear();
        try {
            assigner.processInput(row);
            for (BucketedRow emitted : emittedRows) {
                // GlobalIndexAssigner synthesizes cross-partition DELETE rows by copying the
                // incoming row and changing only InternalRow.RowKind. RowKindGenerator later
                // trusts the configured rowkind field, so keep both representations consistent.
                PaimonRowKindField.apply(
                        writeSemanticContract(), emitted.row, emitted.row.getRowKind());
                delegate.write(emitted.row, emitted.bucket);
            }
        } finally {
            emittedRows.clear();
        }
    }

    @Override
    protected void closeModeResources() throws Exception {
        assigner.close();
    }

    private static final class BucketedRow {
        private final InternalRow row;
        private final int bucket;

        private BucketedRow(InternalRow row, int bucket) {
            this.row = row;
            this.bucket = bucket;
        }
    }
}
