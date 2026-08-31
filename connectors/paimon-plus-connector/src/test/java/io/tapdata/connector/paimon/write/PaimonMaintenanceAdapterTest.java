package io.tapdata.connector.paimon.write;

import org.apache.paimon.table.sink.TableCommitMaintenance;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonMaintenanceAdapterTest {

    @Test
    void waitingCanRejoinTheSameOpaqueOperation() {
        TableCommitMaintenance lifecycle = mock(TableCommitMaintenance.class);
        TableCommitMaintenance.Outcome waiting =
                outcome(TableCommitMaintenance.Status.WAITING, null, null);
        TableCommitMaintenance.Outcome success =
                outcome(TableCommitMaintenance.Status.SUCCESS, null, null);
        when(lifecycle.shutdownAndAwait(1L, TimeUnit.SECONDS))
                .thenReturn(waiting)
                .thenReturn(success);
        PaimonMaintenanceAdapter adapter = new PaimonMaintenanceAdapter(lifecycle);

        assertEquals(
                PaimonMaintenanceAdapter.Status.WAITING,
                adapter.shutdownAndAwait(1L, TimeUnit.SECONDS).status());
        PaimonMaintenanceAdapter.Outcome terminal =
                adapter.shutdownAndAwait(1L, TimeUnit.SECONDS);

        assertEquals(PaimonMaintenanceAdapter.Status.SUCCESS, terminal.status());
        assertNull(terminal.failure());
        verify(lifecycle, times(2)).shutdownAndAwait(1L, TimeUnit.SECONDS);
    }

    @Test
    void failedDrainedRetainsTheOriginalThrowable() {
        Throwable failure = new IllegalStateException("maintenance");
        TableCommitMaintenance lifecycle = mock(TableCommitMaintenance.class);
        TableCommitMaintenance.Outcome failed =
                outcome(TableCommitMaintenance.Status.FAILED_DRAINED, failure, null);
        when(lifecycle.closeAndDrain()).thenReturn(failed);

        PaimonMaintenanceAdapter.Outcome actual =
                new PaimonMaintenanceAdapter(lifecycle).closeAndDrain();

        assertEquals(PaimonMaintenanceAdapter.Status.FAILED_DRAINED, actual.status());
        assertSame(failure, actual.failure());
    }

    @Test
    void waitingMustRetainTheExactPaimonInterruption() {
        InterruptedException interruption = new InterruptedException("maintenance join");
        TableCommitMaintenance lifecycle = mock(TableCommitMaintenance.class);
        TableCommitMaintenance.Outcome waiting =
                outcome(
                        TableCommitMaintenance.Status.WAITING,
                        null,
                        interruption);
        when(lifecycle.shutdownAndAwait(1L, TimeUnit.SECONDS))
                .thenReturn(waiting);

        PaimonMaintenanceAdapter.Outcome actual =
                new PaimonMaintenanceAdapter(lifecycle)
                        .shutdownAndAwait(1L, TimeUnit.SECONDS);

        assertEquals(PaimonMaintenanceAdapter.Status.WAITING, actual.status());
        assertNull(actual.failure());
        assertSame(interruption, actual.interruption());
    }

    @Test
    void inconsistentOutcomeFailsClosed() {
        TableCommitMaintenance lifecycle = mock(TableCommitMaintenance.class);
        TableCommitMaintenance.Outcome inconsistent =
                outcome(TableCommitMaintenance.Status.FAILED_DRAINED, null, null);
        when(lifecycle.closeAndDrain()).thenReturn(inconsistent);

        assertThrows(
                IllegalStateException.class,
                () -> new PaimonMaintenanceAdapter(lifecycle).closeAndDrain());
    }

    private static TableCommitMaintenance.Outcome outcome(
            TableCommitMaintenance.Status status,
            Throwable failure,
            InterruptedException interruption) {
        TableCommitMaintenance.Outcome outcome = mock(TableCommitMaintenance.Outcome.class);
        when(outcome.status()).thenReturn(status);
        when(outcome.failure()).thenReturn(failure);
        when(outcome.interruption()).thenReturn(interruption);
        return outcome;
    }
}
