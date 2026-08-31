package io.tapdata.connector.paimon.service;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonWriteCloseModelTest {

    @Test
    void proofAuthorityIsLimitedToStopDdlAndFactoryRollback() {
        PhysicalTableWriterLease lease = lease();
        Object tableLock = new Object();

        PaimonWriteCloseModel.QuiescenceProof stop =
                PaimonWriteCloseModel.QuiescenceProof.stop(
                        lease,
                        1L,
                        PaimonWriteCloseModel.FailureOrigin.WRITE_PATH,
                        true,
                        true);
        PaimonWriteCloseModel.QuiescenceProof ddl =
                PaimonWriteCloseModel.QuiescenceProof.ddl(
                        lease,
                        2L,
                        PaimonWriteCloseModel.FailureOrigin.DDL_PATH,
                        tableLock,
                        true);
        PaimonWriteCloseModel.QuiescenceProof factory =
                PaimonWriteCloseModel.QuiescenceProof.factoryRollback(
                        lease,
                        3L,
                        PaimonWriteCloseModel.FailureOrigin.FACTORY_CONSTRUCTION,
                        true,
                        false,
                        false);

        assertEquals(PaimonWriteCloseModel.InitiatingReason.STOP, stop.initiatingReason());
        assertEquals(
                PaimonWriteCloseModel.FailureOrigin.WRITE_PATH, stop.failureOrigin());
        assertEquals(PaimonWriteCloseModel.InitiatingReason.DDL, ddl.initiatingReason());
        assertEquals(
                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                factory.initiatingReason());
        assertSame(lease, factory.lease());
        assertEquals(3L, factory.proofEpoch());
    }

    @Test
    void incompleteReasonSpecificEvidenceCannotMintProof() {
        PhysicalTableWriterLease lease = lease();

        assertThrows(
                IllegalStateException.class,
                () ->
                        PaimonWriteCloseModel.QuiescenceProof.stop(
                                lease,
                                1L,
                                PaimonWriteCloseModel.FailureOrigin.NONE,
                                false,
                                true));
        assertThrows(
                IllegalStateException.class,
                () ->
                        PaimonWriteCloseModel.QuiescenceProof.ddl(
                                lease,
                                1L,
                                PaimonWriteCloseModel.FailureOrigin.NONE,
                                new Object(),
                                false));
        assertThrows(
                IllegalStateException.class,
                () ->
                        PaimonWriteCloseModel.QuiescenceProof.factoryRollback(
                                lease,
                                1L,
                                PaimonWriteCloseModel.FailureOrigin.NONE,
                                true,
                                true,
                                false));
    }

    @Test
    void activeWaitingAndTerminalRetainedStatesAreDisjoint() {
        assertTrue(
                PaimonWriteCloseModel.CloseState.WAITING_COMPACTION.isActiveWaiting());
        assertTrue(
                PaimonWriteCloseModel.CloseState.WAITING_COMMIT_MAINTENANCE
                        .isActiveWaiting());
        assertTrue(
                PaimonWriteCloseModel.CloseState.CLOSE_DEFERRED_TERMINATION
                        .isActiveWaiting());
        assertFalse(
                PaimonWriteCloseModel.CloseState.WAITING_COMPACTION.isTerminal());

        assertTrue(
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED
                        .isTerminal());
        assertTrue(
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED
                        .isRetained());
        assertFalse(
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED
                        .isActiveWaiting());
        assertTrue(
                PaimonWriteCloseModel.CloseState.IO_CLOSE_FAILED_RETAINED.isTerminal());
        assertFalse(PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS.isRetained());
    }

    @Test
    void failedDrainedMaintenanceNeverAuthorizesIoRelease() {
        AtomicInteger ioCloseCount = new AtomicInteger();
        PaimonWriteCloseModel.DelegateCloseProgress progress =
                new PaimonWriteCloseModel.DelegateCloseProgress();
        assertTrue(progress.closeWriterOnce(() -> {}).succeeded());
        assertTrue(progress.closeCommitterOnce(() -> {}).succeeded());
        IOException maintenanceFailure = new IOException("maintenance failed");
        PaimonWriteCloseModel.MaintenanceOutcome maintenance =
                PaimonWriteCloseModel.MaintenanceOutcome.failedDrained(
                        maintenanceFailure);

        PaimonWriteCloseModel.DelegateCloseOutcome io =
                progress.closeIoOnceIfSafe(true, maintenance, ioCloseCount::incrementAndGet);

        assertEquals(PaimonWriteCloseModel.MaintenanceStatus.FAILED_DRAINED,
                maintenance.status());
        assertSame(maintenanceFailure, maintenance.failure());
        assertEquals(PaimonWriteCloseModel.DelegateCloseStatus.NOT_ATTEMPTED, io.status());
        assertEquals(0, ioCloseCount.get());
        assertEquals(
                PaimonWriteCloseModel.DelegateTerminalEvidence.DEPENDENCY_FAILED_RETAINED,
                progress.terminalEvidence(true, maintenance));
    }

    @Test
    void failedDelegateCloseIsStickyAndNeverRetriedInProcess() {
        AtomicInteger writerCloseCount = new AtomicInteger();
        IOException writerFailure = new IOException("writer close failed");
        PaimonWriteCloseModel.DelegateCloseProgress progress =
                new PaimonWriteCloseModel.DelegateCloseProgress();
        PaimonWriteCloseModel.CloseAction writerClose =
                () -> {
                    writerCloseCount.incrementAndGet();
                    throw writerFailure;
                };

        PaimonWriteCloseModel.DelegateCloseOutcome first =
                progress.closeWriterOnce(writerClose);
        PaimonWriteCloseModel.DelegateCloseOutcome second =
                progress.closeWriterOnce(writerClose);

        assertSame(first, second);
        assertSame(writerFailure, second.failure());
        assertEquals(PaimonWriteCloseModel.DelegateCloseStatus.FAILED_RETAINED,
                second.status());
        assertEquals(1, writerCloseCount.get());
    }

    @Test
    void ioCloseFailureIsStickyAndCannotBeHiddenByASecondNoOp() {
        AtomicInteger ioCloseCount = new AtomicInteger();
        IOException ioFailure = new IOException("partial delete");
        PaimonWriteCloseModel.DelegateCloseProgress progress =
                new PaimonWriteCloseModel.DelegateCloseProgress();
        progress.closeWriterOnce(() -> {});
        progress.closeCommitterOnce(() -> {});
        PaimonWriteCloseModel.CloseAction ioClose =
                () -> {
                    ioCloseCount.incrementAndGet();
                    throw ioFailure;
                };

        PaimonWriteCloseModel.DelegateCloseOutcome first =
                progress.closeIoOnceIfSafe(
                        true, PaimonWriteCloseModel.MaintenanceOutcome.success(), ioClose);
        PaimonWriteCloseModel.DelegateCloseOutcome second =
                progress.closeIoOnceIfSafe(
                        true, PaimonWriteCloseModel.MaintenanceOutcome.success(), ioClose);

        assertSame(first, second);
        assertSame(ioFailure, second.failure());
        assertEquals(1, ioCloseCount.get());
        assertEquals(
                PaimonWriteCloseModel.DelegateTerminalEvidence.IO_FAILED_RETAINED,
                progress.terminalEvidence(
                        true, PaimonWriteCloseModel.MaintenanceOutcome.success()));
    }

    @Test
    void waitingMaintenanceCannotBeCollapsedIntoATerminalOutcome() {
        PaimonWriteCloseModel.DelegateCloseProgress progress =
                new PaimonWriteCloseModel.DelegateCloseProgress();

        assertThrows(
                IllegalStateException.class,
                () ->
                        progress.terminalEvidence(
                                true,
                                PaimonWriteCloseModel.MaintenanceOutcome.waiting()));
    }

    @Test
    void delegateEvidenceCannotMintResourceClosedSuccessBeforeSpillCleanup() {
        PaimonWriteCloseModel.DelegateCloseProgress progress =
                new PaimonWriteCloseModel.DelegateCloseProgress();
        progress.closeWriterOnce(() -> {});
        progress.closeCommitterOnce(() -> {});
        progress.closeIoOnceIfSafe(
                true, PaimonWriteCloseModel.MaintenanceOutcome.success(), () -> {});

        assertEquals(
                PaimonWriteCloseModel.DelegateTerminalEvidence.IO_CLOSED,
                progress.terminalEvidence(
                        true, PaimonWriteCloseModel.MaintenanceOutcome.success()));
        for (Method method :
                PaimonWriteCloseModel.DelegateCloseProgress.class.getDeclaredMethods()) {
            assertFalse(
                    method.getReturnType() == PaimonWriteCloseModel.CloseState.class,
                    "dependency-only helper must not mint a resource CloseState: " + method);
        }
    }

    private static PhysicalTableWriterLease lease() {
        return PhysicalTableWriterLease.of(
                "physical-hash",
                "database.table",
                "service",
                "generation",
                PhysicalTableWriterLease.Purpose.WRITER);
    }
}
