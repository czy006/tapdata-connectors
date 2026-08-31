package io.tapdata.connector.paimon.write.bucket;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.StartupMode.LATEST;

/**
 * Version-locked, caller-thread implementation of Paimon 1.3.2 index bootstrap planning.
 *
 * <p>This class intentionally reproduces {@code IndexBootstrap#bootstrap} through the point where
 * that implementation delegates to {@code SplitsParallelReadUtil.parallelExecute}. It replaces
 * only that final parallel fan-out with a structured composite reader which owns at most one split
 * reader and one batch at a time. Any Paimon upgrade must diff the release-1.3.2 implementation and
 * update the golden test before this adapter is considered compatible.
 */
final class PaimonSequentialIndexBootstrap {

    private final FileStoreTable table;
    private final LongSupplier currentTimeMillis;

    PaimonSequentialIndexBootstrap(FileStoreTable table) {
        this(table, System::currentTimeMillis);
    }

    PaimonSequentialIndexBootstrap(FileStoreTable table, LongSupplier currentTimeMillis) {
        this.table = Objects.requireNonNull(table, "table");
        this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis, "currentTimeMillis");
    }

    RecordReader<InternalRow> bootstrap(int numAssigners, int assignId) throws IOException {
        RowType rowType = table.rowType();
        List<String> fieldNames = rowType.getFieldNames();
        int[] keyProjection =
                table.schema().trimmedPrimaryKeys().stream()
                        .map(fieldNames::indexOf)
                        .mapToInt(Integer::intValue)
                        .toArray();

        ReadBuilder readBuilder =
                table.copy(Collections.singletonMap(SCAN_MODE.key(), LATEST.toString()))
                        .newReadBuilder()
                        .withProjection(keyProjection);
        DataTableScan tableScan = (DataTableScan) readBuilder.newScan();
        List<Split> splits =
                tableScan
                        .withBucketFilter(bucket -> bucket % numAssigners == assignId)
                        .plan()
                        .splits();

        CoreOptions options = CoreOptions.fromMap(table.options());
        Duration indexTtl = options.crossPartitionUpsertIndexTtl();
        if (indexTtl != null) {
            long indexTtlMillis = indexTtl.toMillis();
            long currentTime = currentTimeMillis.getAsLong();
            splits =
                    splits.stream()
                            .filter(split -> filterSplit(split, indexTtlMillis, currentTime))
                            .collect(Collectors.toList());
        }

        RowDataToObjectArrayConverter partBucketConverter =
                new RowDataToObjectArrayConverter(
                        TypeUtils.concat(
                                TypeUtils.project(rowType, table.partitionKeys()),
                                RowType.of(DataTypes.INT())));
        List<PlannedSplit> plannedSplits = new ArrayList<>(splits.size());
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            InternalRow partitionAndBucket =
                    partBucketConverter.toGenericRow(
                            new JoinedRow(
                                    dataSplit.partition(), GenericRow.of(dataSplit.bucket())));
            plannedSplits.add(new PlannedSplit(split, partitionAndBucket));
        }

        return new SequentialCompositeReader(readBuilder, plannedSplits);
    }

    private static boolean filterSplit(Split split, long indexTtl, long currentTime) {
        for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
            if (currentTime <= file.creationTimeEpochMillis() + indexTtl) {
                return true;
            }
        }
        return false;
    }

    private static final class PlannedSplit {
        private final Split split;
        private final InternalRow partitionAndBucket;

        private PlannedSplit(Split split, InternalRow partitionAndBucket) {
            this.split = split;
            this.partitionAndBucket = partitionAndBucket;
        }
    }

    private static final class SequentialCompositeReader implements RecordReader<InternalRow> {

        private final ReadBuilder readBuilder;
        private final List<PlannedSplit> splits;

        private int nextSplit;
        private RecordReader<InternalRow> currentReader;
        private InternalRow currentPartitionAndBucket;
        private ManagedBatch activeBatch;
        private boolean terminal;
        private boolean closeAttempted;
        private Throwable stickyCleanupFailure;

        private SequentialCompositeReader(ReadBuilder readBuilder, List<PlannedSplit> splits) {
            this.readBuilder = readBuilder;
            this.splits = splits;
        }

        @Override
        public synchronized RecordIterator<InternalRow> readBatch() throws IOException {
            if (terminal) {
                replayCleanupFailureIfPresent();
                return null;
            }

            if (activeBatch != null) {
                activeBatch.releaseBatch();
            }

            while (true) {
                if (currentReader == null) {
                    if (nextSplit >= splits.size()) {
                        terminal = true;
                        return null;
                    }
                    openNextReader();
                }

                final RecordIterator<InternalRow> splitBatch;
                try {
                    splitBatch = currentReader.readBatch();
                } catch (Throwable readFailure) {
                    failAfterRead(readFailure, null);
                    return null;
                }

                if (splitBatch != null) {
                    activeBatch =
                            new ManagedBatch(this, splitBatch, currentPartitionAndBucket);
                    return activeBatch;
                }

                Throwable closeFailure = closeCurrentReader(null);
                if (closeFailure != null) {
                    terminal = true;
                    stickyCleanupFailure = closeFailure;
                    rethrow(closeFailure);
                }
            }
        }

        private void openNextReader() throws IOException {
            PlannedSplit plannedSplit = splits.get(nextSplit++);
            try {
                currentReader = readBuilder.newRead().createReader(plannedSplit.split);
                currentPartitionAndBucket = plannedSplit.partitionAndBucket;
            } catch (Throwable creationFailure) {
                terminal = true;
                rethrow(creationFailure);
            }
        }

        private synchronized InternalRow next(ManagedBatch batch) throws IOException {
            if (batch.released) {
                batch.replayReleaseFailureIfPresent();
                return null;
            }
            final InternalRow row;
            try {
                row = batch.delegate.next();
            } catch (Throwable readFailure) {
                failAfterRead(readFailure, batch);
                return null;
            }
            if (row == null) {
                release(batch);
                return null;
            }
            return new JoinedRow().replace(row, batch.partitionAndBucket);
        }

        private synchronized void release(ManagedBatch batch) {
            if (batch.released) {
                batch.replayReleaseFailureIfPresent();
                return;
            }
            batch.released = true;
            if (activeBatch == batch) {
                activeBatch = null;
            }
            try {
                batch.delegate.releaseBatch();
            } catch (Throwable releaseFailure) {
                batch.releaseFailure = releaseFailure;
                terminal = true;
                stickyCleanupFailure = releaseFailure;
                closeCurrentReader(releaseFailure);
                rethrowUnchecked(releaseFailure);
            }
        }

        private void failAfterRead(Throwable readFailure, ManagedBatch batch) throws IOException {
            Throwable cleanupFailure = null;
            if (batch != null && !batch.released) {
                batch.released = true;
                if (activeBatch == batch) {
                    activeBatch = null;
                }
                try {
                    batch.delegate.releaseBatch();
                } catch (Throwable releaseFailure) {
                    addSuppressed(readFailure, releaseFailure);
                    cleanupFailure = releaseFailure;
                }
            }

            RecordReader<InternalRow> reader = currentReader;
            currentReader = null;
            currentPartitionAndBucket = null;
            if (reader != null) {
                try {
                    reader.close();
                } catch (Throwable closeFailure) {
                    addSuppressed(readFailure, closeFailure);
                    if (cleanupFailure == null) {
                        cleanupFailure = closeFailure;
                    }
                }
            }
            terminal = true;
            stickyCleanupFailure = cleanupFailure;
            rethrow(readFailure);
        }

        private Throwable closeCurrentReader(Throwable primary) {
            RecordReader<InternalRow> reader = currentReader;
            currentReader = null;
            currentPartitionAndBucket = null;
            if (reader == null) {
                return primary;
            }
            try {
                reader.close();
            } catch (Throwable closeFailure) {
                if (primary == null) {
                    return closeFailure;
                }
                addSuppressed(primary, closeFailure);
            }
            return primary;
        }

        @Override
        public synchronized void close() throws IOException {
            if (terminal || closeAttempted) {
                replayCleanupFailureIfPresent();
                return;
            }
            closeAttempted = true;

            Throwable failure = null;
            if (activeBatch != null) {
                try {
                    activeBatch.releaseBatch();
                } catch (Throwable releaseFailure) {
                    failure = releaseFailure;
                }
            }
            if (currentReader != null) {
                failure = closeCurrentReader(failure);
            }
            terminal = true;
            if (failure != null) {
                stickyCleanupFailure = failure;
                rethrow(failure);
            }
        }

        private void replayCleanupFailureIfPresent() throws IOException {
            if (stickyCleanupFailure != null) {
                throw new IOException(
                        "Sequential index bootstrap resource cleanup previously failed",
                        stickyCleanupFailure);
            }
        }
    }

    private static final class ManagedBatch implements RecordReader.RecordIterator<InternalRow> {

        private final SequentialCompositeReader owner;
        private final RecordReader.RecordIterator<InternalRow> delegate;
        private final InternalRow partitionAndBucket;

        private boolean released;
        private Throwable releaseFailure;

        private ManagedBatch(
                SequentialCompositeReader owner,
                RecordReader.RecordIterator<InternalRow> delegate,
                InternalRow partitionAndBucket) {
            this.owner = owner;
            this.delegate = delegate;
            this.partitionAndBucket = partitionAndBucket;
        }

        @Override
        public InternalRow next() throws IOException {
            return owner.next(this);
        }

        @Override
        public void releaseBatch() {
            owner.release(this);
        }

        private void replayReleaseFailureIfPresent() {
            if (releaseFailure != null) {
                throw new IllegalStateException(
                        "Sequential index bootstrap batch release previously failed",
                        releaseFailure);
            }
        }
    }

    private static void addSuppressed(Throwable primary, Throwable suppressed) {
        if (primary != suppressed) {
            primary.addSuppressed(suppressed);
        }
    }

    private static void rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException(failure);
    }

    private static void rethrowUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IllegalStateException(failure);
    }
}
