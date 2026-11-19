package io.tapdata.connector.paimon.recovery;

import io.tapdata.connector.paimon.service.PaimonStopResources;
import io.tapdata.connector.paimon.write.PaimonNativeCommitterClose;
import io.tapdata.connector.paimon.write.PaimonTableCommitter;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * Verifies a sealed, ordered interval for an exclusively owned table/branch. Its input files and
 * snapshot history must be protected from external reclamation until the caller persists success.
 * For a nonempty source interval the caller must establish that prepare returned business effects.
 */
public final class PublicationVerifier {
    private PublicationVerifier() {}

    /** Capture before opening the writer. Read snapshot files directly to detect rollback/ID reuse. */
    public static String fingerprint(FileStoreTable table) throws IOException {
        long head = latestId(table);
        return fingerprintAt(table, head);
    }

    public static String publish(FileStoreTable table, String commitUser, long identifier,
            List<CommitMessage> messages, String baselineFingerprint) throws Exception {
        StreamTableCommit nativeCommit = table.newStreamWriteBuilder().withCommitUser(commitUser).newCommit();
        String confirmed;
        try (PaimonNativeCommitterClose ignored = new PaimonNativeCommitterClose(nativeCommit,
                PaimonStopResources.Scope.standalone("recover prepared files"))) {
            confirmed = publishChecked(table, commitUser, identifier, messages, baselineFingerprint,
                    nativeCommit::filterAndCommit);
        }
        // The native committer may run retention after filterAndCommit returns.
        requireFingerprint(table, baselineId(baselineFingerprint), baselineFingerprint);
        requireHead(table, confirmed);
        return confirmed;
    }

    /** Use the writer's existing committer when its lifetime is already managed by the stop scope. */
    public static String publish(FileStoreTable table, String commitUser, long identifier,
            List<CommitMessage> messages, String baselineFingerprint, PaimonTableCommitter committer)
            throws Exception {
        return publishChecked(table, commitUser, identifier, messages, baselineFingerprint,
                committer::filterAndCommit);
    }

    private static String publishChecked(FileStoreTable table, String user, long id,
            List<CommitMessage> messages, String baseline,
            ToIntFunction<Map<Long, List<CommitMessage>>> commit) throws IOException {
        Objects.requireNonNull(user, "commitUser");
        if (id < 0 || id == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid recovery commit identifier " + id);
        }
        long baseId = baselineId(baseline);
        requireFingerprint(table, baseId, baseline);
        PreparedSnapshotChanges changes = new PreparedSnapshotChanges(table, messages);
        long head = latestId(table);
        boolean fresh = head == baseId;
        changes.checkFiles(fresh);
        if (fresh) {
            CoreOptions options = CoreOptions.fromMap(table.options());
            int retain = changes.snapshotCount() + (baseId == 0 ? 0 : 1);
            if (!options.writeOnly() && options.snapshotNumRetainMin() < retain) {
                throw new IOException("Snapshot retention cannot protect the baseline and " +
                        "pending publication. Minimum required for this interval: " + retain);
            }
            changes.checkDeleteDependencies(snapshot(table, baseId));
            requireHead(table, baseline);
            // Both fresh and ambiguous submissions need native positive-file checks.
            // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L265-L303
            commit.applyAsInt(Collections.singletonMap(id, messages));
            head = latestId(table);
        }
        // FileStoreCommitImpl may first publish APPEND/OVERWRITE, then COMPACT with the same id.
        // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L323-L410
        if (head < baseId || head - baseId != changes.snapshotCount()) {
            throw new IOException("Incomplete or divergent publication for " + table.name()
                    + ", commit " + id + ", baseline " + baseId + ", head " + head);
        }
        Snapshot previous = snapshot(table, baseId);
        for (int offset = 1; offset <= changes.snapshotCount(); offset++) {
            Snapshot current = snapshot(table, baseId + offset);
            if (!user.equals(current.commitUser()) || id != current.commitIdentifier()
                    || current.schemaId() != table.schema().id()) {
                throw new IOException("Unexpected owner, commit identifier or schema in snapshot " + current.id());
            }
            changes.verify(previous, current, offset == 2);
            previous = current;
        }
        String result = token(table, previous);
        requireFingerprint(table, baseId, baseline);
        requireHead(table, result);
        if (!fresh) {
            // Let native retry callbacks observe an already published interval only after proving
            // that the entire interval is visible. A partial APPEND must never reach this point.
            commit.applyAsInt(Collections.singletonMap(id, messages));
            requireHead(table, result);
        }
        return result;
    }

    private static void requireHead(FileStoreTable table, String expected) throws IOException {
        if (!expected.equals(fingerprint(table))) {
            throw new IOException("Table head changed during publication of " + table.name());
        }
    }

    private static void requireFingerprint(FileStoreTable table, long id, String expected) throws IOException {
        if (!expected.equals(fingerprintAt(table, id))) {
            throw new IOException("Recovery baseline has changed for " + table.name());
        }
    }

    private static long latestId(FileStoreTable table) {
        Long id = table.snapshotManager().latestSnapshotIdFromFileSystem();
        return id == null ? 0 : id;
    }

    /** The first field is the snapshot ID, zero designates a never-published table. */
    private static long baselineId(String baseline) throws IOException {
        if (baseline != null && baseline.length() <= 128) {
            int separator = baseline.indexOf(':');
            if (separator > 0) {
                try {
                    long id = Long.parseLong(baseline.substring(0, separator));
                    if (id >= 0) {
                        return id;
                    }
                } catch (NumberFormatException invalid) {
                    throw new IOException("Invalid recovery snapshot identifier", invalid);
                }
            }
        }
        throw new IOException("Invalid recovery baseline fingerprint");
    }

    private static String fingerprintAt(FileStoreTable table, long id) throws IOException {
        return token(table, snapshot(table, id));
    }

    private static Snapshot snapshot(FileStoreTable table, long id) throws IOException {
        if (id == 0) {
            return null;
        }
        SnapshotManager manager = table.snapshotManager();
        Snapshot snapshot = Snapshot.fromJson(table.fileIO().readFileUtf8(manager.snapshotPath(id)));
        if (snapshot.id() != id) {
            throw new IOException("Snapshot ID differs from its file name at " + id);
        }
        // Snapshot JSON may survive deletion of the manifest lists it names. Check these directly,
        // including when recovering a clean local directory that no longer has staged messages.
        FileStorePathFactory paths = table.store().pathFactory();
        checkMetadata(table, paths.toManifestListPath(snapshot.baseManifestList()), snapshot.baseManifestListSize());
        checkMetadata(table, paths.toManifestListPath(snapshot.deltaManifestList()), snapshot.deltaManifestListSize());
        if (snapshot.changelogManifestList() != null) {
            checkMetadata(table, paths.toManifestListPath(snapshot.changelogManifestList()),
                    snapshot.changelogManifestListSize());
        }
        if (snapshot.indexManifest() != null) {
            checkMetadata(table, paths.indexManifestFileFactory().toPath(snapshot.indexManifest()), null);
        }
        return snapshot;
    }

    private static void checkMetadata(FileStoreTable table, Path path, Long expectedLength) throws IOException {
        FileStatus status = table.fileIO().getFileStatus(path);
        if (status.isDir() || (expectedLength != null && expectedLength != status.getLen())) {
            throw new IOException("Snapshot metadata length/type mismatch for " + path.getName());
        }
    }

    private static String token(FileStoreTable table, Snapshot snapshot) throws IOException {
        TableSchema schema = table.schemaManager().latest()
                .orElseThrow(() -> new IOException("No persisted schema for " + table.name()));
        if (schema.id() != table.schema().id()) {
            throw new IOException("Writer schema is stale for " + table.name());
        }
        // The JSON contains the snapshot's immutable manifest references. Including the scope
        // and persisted schema also binds an empty table to its own generation and branch.
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String field : new String[]{table.location().toUri().normalize().toString(),
                    table.snapshotManager().snapshotDirectory().toString(), table.uuid(), schema.toString(),
                    snapshot == null ? "" : snapshot.toJson()}) {
                digest.update(field.getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
            }
            return (snapshot == null ? 0 : snapshot.id()) + ":"
                    + Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
