package io.tapdata.connector.paimon.read;

import io.tapdata.exception.TapPdkOffsetOutOfLogEx;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

import java.util.function.BooleanSupplier;

/**
 * Resolves a shared time boundary into next-to-consume positions and validates history using metadata only.
 * Paimon defines snapshot/changelog semantics; bounded retries, gap rejection and no latest fallback are connector policy.
 */
public final class PaimonReadPositionResolver {
    private final SnapshotManager snapshots;
    private final ChangelogManager changelogs;
    private final boolean decoupled;
    private final String tableName;
    private final BooleanSupplier active;
    private final long retryMillis;

    public PaimonReadPositionResolver(FileStoreTable table, String name, BooleanSupplier active) {
        this(table.snapshotManager(), table.changelogManager(),
                new CoreOptions(table.options()).changelogLifecycleDecoupled(), name, active, 1000);
    }

    PaimonReadPositionResolver(SnapshotManager snapshots, ChangelogManager changelogs,
            boolean decoupled, String name, BooleanSupplier active, long retryMillis) {
        this.snapshots = snapshots; this.changelogs = changelogs; this.decoupled = decoupled;
        this.tableName = name; this.active = active; this.retryMillis = retryMillis;
    }

    public static String identity(FileStoreTable table) {
        return PaimonReadOffset.physicalTableHash(table.location().toUri(), CoreOptions.branch(table.options()));
    }

    // Paimon release-1.3 @ 5c59e6cb01ed0b29563371f56e14fcade4597a2e:
    // paimon-core/src/main/java/org/apache/paimon/utils/NextSnapshotFetcher.java,
    // getNextSnapshot (52-67) prefers snapshot, then decoupled changelog. rangeCheck (70-99)
    // does NOT report every decoupled history gap, so the connector validates the actual id.
    // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/utils/NextSnapshotFetcher.java#L52
    public Snapshot metadata(long id) {
        if (snapshots.snapshotExists(id)) {
            try { return snapshots.snapshot(id); }
            catch (RuntimeException failure) {
                // Snapshot expiration can move metadata to changelog between exists and read.
                if (decoupled && changelogs.longLivedChangelogExists(id)) {
                    return changelogs.changelog(id);
                }
                throw failure;
            }
        }
        return decoupled && changelogs.longLivedChangelogExists(id) ? changelogs.changelog(id) : null;
    }

    // Native NextSnapshotFetcher#rangeCheck allows waiting for a future snapshot but misses some decoupled history gaps.
    // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/utils/NextSnapshotFetcher.java#L70
    // Validate the exact id: allow existing history, latest+1, or 1 for an empty table; never wait indefinitely at a gap.
    // Three attempts are connector policy. Preserve the final storage error instead of misclassifying it as expired history.
    public void validate(long next) throws InterruptedException {
        RuntimeException cause = null;
        RuntimeException metadataFailure = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            checkActive();
            try {
                metadataFailure = null;
                if (next <= 0) { throw new IllegalArgumentException("Non-positive snapshot"); }
                if (metadata(next) != null) { return; }
                Long latest = snapshots.latestSnapshotIdFromFileSystem();
                if (latest == null) {
                    if (next == 1 && (!decoupled || changelogs.latestLongLivedChangelogId() == null)) { return; }
                } else if (latest < Long.MAX_VALUE && next == latest + 1) { return; }
                cause = new IllegalStateException("Requested next=" + next + ", latest=" + latest
                        + ", earliest=" + snapshots.earliestSnapshotId() + ", decoupled=" + decoupled);
            } catch (RuntimeException failure) { cause = failure; metadataFailure = failure; }
            if (attempt < 2) { pause(); }
        }
        if (metadataFailure != null) { throw metadataFailure; }
        throw outOfHistory(next, cause);
    }

    // Paimon SnapshotManager.earlierOrEqualTimeMills (287-314) stops on equality;
    // TimeTravelUtil.earlierThanTimeMills uses strict <T. Neither implements our highest-id <=T.
    // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java#L287
    // Connector policy: freeze latest, descend metadata, tolerate non-monotonic writer clocks.
    // Unlike the native strict-before binary search, this method selects the highest id at or before T.
    // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/TimeTravelUtil.java#L160
    public long atTimestamp(long timestamp, long initializationTime) throws InterruptedException {
        if (timestamp < 0 || timestamp > initializationTime) {
            throw new IllegalArgumentException("Paimon stream timestamp must be between epoch and initialization time");
        }
        checkActive();
        Long latest = snapshots.latestSnapshotIdFromFileSystem();
        if (latest == null) { validate(1); return 1; }
        Long earliest = snapshots.earliestSnapshotId();
        if (decoupled) {
            Long firstChange = changelogs.earliestLongLivedChangelogId();
            if (firstChange != null && (earliest == null || firstChange < earliest)) { earliest = firstChange; }
        }
        if (earliest == null) { throw outOfHistory(latest, new IllegalStateException("Missing history boundary")); }
        for (long id = latest; id >= earliest; id--) {
            checkActive();
            Snapshot snapshot = requiredMetadata(id);
            if (snapshot.timeMillis() <= timestamp) { return Math.addExact(id, 1); }
        }
        if (earliest == 1) { return 1; }
        throw outOfHistory(earliest, new IllegalStateException("Timestamp precedes available Paimon history"));
    }

    // Unlike validate, time resolution and plan schema checks require actual metadata, not a future snapshot position.
    // Never return null to allow progress; preserve storage errors and use out-of-log only for missing history.
    public Snapshot requiredMetadata(long id) throws InterruptedException {
        RuntimeException cause = null;
        RuntimeException metadataFailure = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            checkActive();
            try {
                metadataFailure = null;
                Snapshot snapshot = metadata(id);
                if (snapshot != null) { return snapshot; }
                cause = new IllegalStateException("Missing snapshot/changelog metadata " + id);
            } catch (RuntimeException failure) { cause = failure; metadataFailure = failure; }
            if (attempt < 2) { pause(); }
        }
        if (metadataFailure != null) { throw metadataFailure; }
        throw outOfHistory(id, cause);
    }

    private TapPdkOffsetOutOfLogEx outOfHistory(long next, Throwable cause) {
        // Verified signature: pdk-error-code 2.8-SNAPSHOT TapPdkOffsetOutOfLogEx(String,Object,Throwable).
        return new TapPdkOffsetOutOfLogEx("paimon", tableName + ":next=" + next + "; decoupled=" + decoupled + "; required history unavailable; reinitialize explicitly", cause);
    }
    private void pause() throws InterruptedException {
        long remaining = retryMillis;
        while (remaining > 0) {
            checkActive();
            long step = Math.min(remaining, 50);
            Thread.sleep(step); remaining -= step;
        }
    }
    private void checkActive() throws InterruptedException {
        if (!active.getAsBoolean() || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Paimon position resolution cancelled");
        }
    }
}
