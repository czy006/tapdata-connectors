package io.tapdata.connector.paimon.read;

import io.tapdata.exception.TapPdkOffsetOutOfLogEx;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Changelog;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.ChangelogManager;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaimonReadPositionResolverTest {
    final SnapshotManager snapshots = mock(SnapshotManager.class);
    final ChangelogManager changes = mock(ChangelogManager.class);
    final PaimonReadPositionResolver resolver = new PaimonReadPositionResolver(snapshots, changes, true, "orders", () -> true, 0);
    void snapshot(long id, long time) {
        Snapshot s = mock(Snapshot.class);
        when(s.id()).thenReturn(id); when(s.timeMillis()).thenReturn(time);
        when(snapshots.snapshotExists(id)).thenReturn(true); when(snapshots.snapshot(id)).thenReturn(s);
    }
    @Test void storageFailureIsNotMisclassifiedAsExpiredHistory() {
        RuntimeException denied=new java.io.UncheckedIOException(new java.io.IOException("storage unavailable"));
        when(snapshots.snapshotExists(1)).thenThrow(denied);
        assertSame(denied,assertThrows(RuntimeException.class,()->resolver.validate(1)));
        assertSame(denied,assertThrows(RuntimeException.class,()->resolver.requiredMetadata(1)));
    }
    @Test void emptyAndLatestSuccessorWaitButHolesFail() throws Exception {
        resolver.validate(1);
        assertThrows(TapPdkOffsetOutOfLogEx.class, () -> resolver.validate(2));
        when(snapshots.latestSnapshotIdFromFileSystem()).thenReturn(5L);
        resolver.validate(6);
        assertThrows(TapPdkOffsetOutOfLogEx.class, () -> resolver.validate(4));
        assertThrows(TapPdkOffsetOutOfLogEx.class, () -> resolver.validate(7));
    }
    @Test void decoupledMetadataBelowEarliestSnapshotRemainsReadable() throws Exception {
        when(snapshots.latestSnapshotIdFromFileSystem()).thenReturn(10L);
        when(snapshots.earliestSnapshotId()).thenReturn(8L);
        when(changes.longLivedChangelogExists(5L)).thenReturn(true);
        when(changes.changelog(5L)).thenReturn(mock(Changelog.class));
        resolver.validate(5);
        assertThrows(TapPdkOffsetOutOfLogEx.class, () -> resolver.validate(6));
    }
    @Test void timestampUsesHighestEqualIdAndDoesNotAssumeMonotonicTime() throws Exception {
        when(snapshots.latestSnapshotIdFromFileSystem()).thenReturn(4L);
        when(snapshots.earliestSnapshotId()).thenReturn(1L);
        snapshot(1, 10); snapshot(2, 20); snapshot(3, 20); snapshot(4, 30);
        assertEquals(4, resolver.atTimestamp(20, 100));
        snapshot(4, 15); // Clock rollback: highest qualifying snapshot remains id 4.
        assertEquals(5, resolver.atTimestamp(20, 100));
        assertThrows(IllegalArgumentException.class, () -> resolver.atTimestamp(101, 100));
    }
    @Test void earlierThanRetainedHistoryFailsInsteadOfClamping() {
        when(snapshots.latestSnapshotIdFromFileSystem()).thenReturn(5L);
        when(snapshots.earliestSnapshotId()).thenReturn(5L);
        snapshot(5, 50);
        assertThrows(TapPdkOffsetOutOfLogEx.class, () -> resolver.atTimestamp(20, 100));
    }
    @Test void gapAndCancellationNeverProduceNewOffset() {
        when(snapshots.latestSnapshotIdFromFileSystem()).thenReturn(5L);
        when(snapshots.earliestSnapshotId()).thenReturn(1L);
        assertThrows(TapPdkOffsetOutOfLogEx.class, () -> resolver.atTimestamp(10, 100));
        PaimonReadPositionResolver stopped = new PaimonReadPositionResolver(snapshots, changes, true, "orders", () -> false, 0);
        assertThrows(InterruptedException.class, () -> stopped.validate(1));
    }
}
