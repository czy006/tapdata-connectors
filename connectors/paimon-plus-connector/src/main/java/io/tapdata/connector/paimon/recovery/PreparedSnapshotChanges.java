package io.tapdata.connector.paimon.recovery;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.HashIndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CompactedChangelogPathResolver;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.IndexFilePathFactories;
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Expected, separately verifiable APPEND/OVERWRITE and COMPACT effects of one sealed interval. */
final class PreparedSnapshotChanges {
    private final FileStoreTable table;
    private final Changes append = new Changes(false);
    private final Changes compact = new Changes(true);
    private final ManifestList lists;
    private final ManifestFile manifests;
    private final IndexManifestFile indices;
    private final DataFilePathFactories dataPaths;
    private final IndexFilePathFactories indexPaths;

    PreparedSnapshotChanges(FileStoreTable table, List<CommitMessage> messages) throws IOException {
        this.table = table;
        lists = table.store().manifestListFactory().create();
        manifests = table.store().manifestFileFactory().create();
        indices = table.store().indexManifestFileFactory().create();
        dataPaths = new DataFilePathFactories(table.store().pathFactory());
        indexPaths = new IndexFilePathFactories(table.store().pathFactory());
        // Match FileStoreCommitImpl.collectChanges, preserving index ADD order across prepares.
        // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L703-L792
        for (CommitMessage message : messages) {
            if (!(message instanceof CommitMessageImpl)) {
                throw new IOException("Unexpected Paimon commit message implementation");
            }
            CommitMessageImpl msg = (CommitMessageImpl) message;
            DataIncrement data = msg.newFilesIncrement();
            append.add(table, msg, data.newFiles(), data.deletedFiles(), data.changelogFiles(),
                    data.newIndexFiles(), data.deletedIndexFiles());
            CompactIncrement compaction = msg.compactIncrement();
            compact.add(table, msg, compaction.compactAfter(), compaction.compactBefore(),
                    compaction.changelogFiles(), compaction.newIndexFiles(), compaction.deletedIndexFiles());
        }
    }

    int snapshotCount() {
        // StreamWriteBuilderImpl.newCommit() explicitly sets ignoreEmptyCommit(false) in 1.3.2.
        return compact.isEmpty() ? 1 : 2;
    }

    void checkFiles(boolean includeRemovalInputs) throws IOException {
        for (Changes phase : new Changes[]{append, compact}) {
            checkData(phase.data, includeRemovalInputs);
            checkData(phase.changelog, includeRemovalInputs);
            for (IndexManifestEntry entry : phase.index) {
                if (includeRemovalInputs || entry.kind() == FileKind.ADD) {
                    Path path = indexPaths.get(entry.partition(), entry.bucket()).toPath(entry.indexFile());
                    checkFile(path, entry.indexFile().fileSize());
                }
            }
        }
    }

    /**
     * Before native APPEND, prove that every data file to remove is active at writer open or added
     * earlier in this sealed interval. Native COMPACT can discover this conflict after APPEND.
     * FileEntry.Identifier covers partition, bucket, level, sidecars, embedded index and location.
     */
    void checkDeleteDependencies(Snapshot baseline) throws IOException {
        if (!append.deletesData && !compact.deletesData) {
            return;
        }
        Set<FileEntry.Identifier> staged = new HashSet<>();
        Set<FileEntry.Identifier> required = new HashSet<>();
        for (Changes phase : new Changes[]{append, compact}) {
            for (ManifestEntry entry : phase.data) {
                FileEntry.Identifier key = entry.identifier();
                if (entry.kind() == FileKind.ADD) {
                    if (!staged.add(key)) {
                        throw new IOException("Prepared duplicate file addition: " + key.fileName);
                    }
                } else if (!staged.remove(key) && !required.add(key)) {
                    throw new IOException("Prepared duplicate file removal: " + key.fileName);
                }
            }
        }
        if (required.isEmpty()) {
            return;
        }
        if (baseline != null) {
            Set<BinaryRow> partitions = new HashSet<>();
            Set<String> names = new HashSet<>();
            for (FileEntry.Identifier key : required) {
                partitions.add(key.partition);
                names.add(key.fileName);
            }
            Iterator<ManifestEntry> active = table.newSnapshotReader().withSnapshot(baseline)
                    .withPartitionFilter(new ArrayList<>(partitions)).withDataFileNameFilter(names::contains)
                    .readFileIterator();
            while (active.hasNext()) {
                required.remove(active.next().identifier());
            }
        }
        if (!required.isEmpty()) {
            throw new IOException("File to remove is outside the recovery baseline and interval: "
                    + required.iterator().next().fileName);
        }
    }

    private void checkData(List<ManifestEntry> entries, boolean includeRemovalInputs) throws IOException {
        for (ManifestEntry entry : entries) {
            if (!includeRemovalInputs && entry.kind() != FileKind.ADD) {
                continue;
            }
            DataFileMeta file = entry.file();
            DataFilePathFactory paths = dataPaths.get(entry.partition(), entry.bucket());
            checkFile(paths.toPath(file), file.fileSize());
            for (String extra : file.extraFiles()) {
                // DataFileMeta stores the names of sidecars but has no individual size for them.
                checkFile(paths.toAlignedPath(extra, file), -1L);
            }
        }
    }

    private void checkFile(Path path, long expectedSize) throws IOException {
        if (CompactedChangelogPathResolver.isCompactedChangelogPath(path)) {
            // Compacted changelog paths can refer to slices of a shared physical file. This mode
            // has no recovery admission until its slice lengths and reader semantics are verified.
            throw new IOException("Compacted changelog files are not supported for local recovery");
        }
        FileStatus status = table.fileIO().getFileStatus(path);
        if (status.isDir() || (expectedSize >= 0 && status.getLen() != expectedSize)) {
            throw new IOException("Prepared file length/type mismatch for " + path.getName()
                    + ": expected " + expectedSize + ", observed " + status.getLen());
        }
    }

    void verify(Snapshot previous, Snapshot current, boolean isCompact) throws IOException {
        Changes phase = isCompact ? compact : append;
        Snapshot.CommitKind expectedKind = isCompact ? Snapshot.CommitKind.COMPACT : append.dataKind();
        if (current.commitKind() != expectedKind) {
            throw new IOException("Unexpected snapshot kind at " + current.id() + ": expected " + expectedKind);
        }
        verifyManifests(current, lists.readDeltaManifests(current), phase.data);
        verifyManifests(current, lists.readChangelogManifests(current), phase.changelog);
        String previousName = previous == null ? null : previous.indexManifest();
        if (phase.index.isEmpty()) {
            if (!Objects.equals(previousName, current.indexManifest())) {
                throw new IOException("Unexpected index manifest change at snapshot " + current.id());
            }
            return;
        }
        // 1.3.2 first removes deleted keys, then folds ADDs in order, retaining the final
        // cumulative hash image for each (partition,bucket). DV and other types are not admitted.
        // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestFileHandler.java#L133-L159
        Map<Pair<BinaryRow, Integer>, IndexFileMeta> expected = indexImage(previousName);
        for (IndexManifestEntry entry : phase.index) {
            if (entry.kind() == FileKind.DELETE) {
                expected.remove(Pair.of(entry.partition(), entry.bucket()));
            }
        }
        for (IndexManifestEntry entry : phase.index) {
            if (entry.kind() == FileKind.ADD) {
                expected.put(Pair.of(entry.partition(), entry.bucket()), entry.indexFile());
            }
        }
        if (!expected.equals(indexImage(current.indexManifest()))) {
            throw new IOException("Hash index effects differ at snapshot " + current.id());
        }
    }

    private Map<Pair<BinaryRow, Integer>, IndexFileMeta> indexImage(String manifest) throws IOException {
        Map<Pair<BinaryRow, Integer>, IndexFileMeta> image = new HashMap<>();
        if (manifest != null) {
            for (IndexManifestEntry entry : indices.read(manifest)) {
                requireHashIndex(entry.indexFile());
                if (entry.kind() != FileKind.ADD
                        || image.put(Pair.of(entry.partition(), entry.bucket()), entry.indexFile()) != null) {
                    throw new IOException("Invalid cumulative hash index manifest " + manifest);
                }
            }
        }
        return image;
    }

    private void verifyManifests(Snapshot snapshot, List<ManifestFileMeta> delta, List<ManifestEntry> expected)
            throws IOException {
        Map<ManifestEntry, Integer> remaining = new HashMap<>();
        for (ManifestEntry entry : expected) {
            remaining.merge(entry, 1, Integer::sum);
        }
        for (ManifestFileMeta manifest : delta) {
            for (ManifestEntry actual : manifests.read(manifest.fileName(), manifest.fileSize())) {
                Integer count = remaining.get(actual);
                if (count == null) {
                    throw new IOException("Unexpected file in snapshot " + snapshot.id() + ": " + actual.file().fileName());
                }
                if (count == 1) {
                    remaining.remove(actual);
                } else {
                    remaining.put(actual, count - 1);
                }
            }
        }
        if (!remaining.isEmpty()) {
            throw new IOException("Missing expected manifest entries at snapshot " + snapshot.id());
        }
    }

    private static void requireHashIndex(IndexFileMeta file) throws IOException {
        if (!HashIndexFile.HASH_INDEX.equals(file.indexType())) {
            throw new IOException("Unsupported recovery index type " + file.indexType());
        }
    }

    private static final class Changes {
        private final boolean isCompaction;
        private final List<ManifestEntry> data = new ArrayList<>();
        private final List<ManifestEntry> changelog = new ArrayList<>();
        private final List<IndexManifestEntry> index = new ArrayList<>();
        private boolean deletesData;

        private Changes(boolean isCompaction) {
            this.isCompaction = isCompaction;
        }

        void add(FileStoreTable table, CommitMessageImpl msg, List<DataFileMeta> added,
                List<DataFileMeta> deleted, List<DataFileMeta> log, List<IndexFileMeta> addedIndex,
                List<IndexFileMeta> deletedIndex) throws IOException {
            int totalBuckets = msg.totalBuckets() == null ? table.schema().numBuckets() : msg.totalBuckets();
            if (isCompaction) {
                addData(data, msg, totalBuckets, FileKind.DELETE, deleted);
                addData(data, msg, totalBuckets, FileKind.ADD, added);
            } else {
                addData(data, msg, totalBuckets, FileKind.ADD, added);
                addData(data, msg, totalBuckets, FileKind.DELETE, deleted);
            }
            addData(changelog, msg, totalBuckets, FileKind.ADD, log);
            deletesData |= !deleted.isEmpty();
            addIndex(msg, FileKind.ADD, addedIndex);
            addIndex(msg, FileKind.DELETE, deletedIndex);
        }

        private void addIndex(CommitMessageImpl msg, FileKind kind, List<IndexFileMeta> files) throws IOException {
            for (IndexFileMeta file : files) {
                requireHashIndex(file);
                index.add(new IndexManifestEntry(kind, msg.partition(), msg.bucket(), file));
            }
        }

        private static void addData(List<ManifestEntry> entries, CommitMessageImpl msg, int buckets,
                FileKind kind, List<DataFileMeta> files) {
            for (DataFileMeta file : files) {
                entries.add(ManifestEntry.create(kind, msg.partition(), msg.bucket(), buckets, file));
            }
        }

        private Snapshot.CommitKind dataKind() {
            return deletesData ? Snapshot.CommitKind.OVERWRITE : Snapshot.CommitKind.APPEND;
        }

        private boolean isEmpty() {
            return data.isEmpty() && index.isEmpty() && changelog.isEmpty();
        }
    }
}
