package io.tapdata.connector.paimon.service;

import java.util.Objects;

/** Proof, state, and exactly-once dependency-close model for one writer generation. */
final class PaimonWriteCloseModel {

    private PaimonWriteCloseModel() {}

    enum InitiatingReason {
        STOP,
        DDL,
        FACTORY_ROLLBACK
    }

    enum FailureOrigin {
        NONE,
        WRITE_PATH,
        DDL_PATH,
        STOP_DRAIN,
        FACTORY_CONSTRUCTION,
        DEPENDENCY_CLOSE
    }

    enum CloseState {
        WAITING_COMPACTION(true, false, false),
        WAITING_COMMIT_MAINTENANCE(true, false, false),
        CLOSE_DEFERRED_TERMINATION(true, false, false),
        FACTORY_ROLLBACK_RETAINED(false, true, true),
        DEPENDENCY_CLOSE_FAILED_RETAINED(false, true, true),
        IO_CLOSE_FAILED_RETAINED(false, true, true),
        CLOSED_SUCCESS(false, true, false);

        private final boolean activeWaiting;
        private final boolean terminal;
        private final boolean retained;

        CloseState(boolean activeWaiting, boolean terminal, boolean retained) {
            this.activeWaiting = activeWaiting;
            this.terminal = terminal;
            this.retained = retained;
        }

        boolean isActiveWaiting() {
            return activeWaiting;
        }

        boolean isTerminal() {
            return terminal;
        }

        boolean isRetained() {
            return retained;
        }
    }

    enum MaintenanceStatus {
        SUCCESS,
        FAILED_DRAINED,
        WAITING
    }

    enum DelegateCloseStatus {
        NOT_ATTEMPTED,
        SUCCEEDED,
        FAILED_RETAINED
    }

    interface CloseAction {
        void close() throws Exception;
    }

    /** Immutable proof minted only after the caller has established a reason-specific barrier. */
    static final class QuiescenceProof {
        private final PhysicalTableWriterLease lease;
        private final InitiatingReason initiatingReason;
        private final FailureOrigin failureOrigin;
        private final long proofEpoch;

        private QuiescenceProof(
                PhysicalTableWriterLease lease,
                InitiatingReason initiatingReason,
                FailureOrigin failureOrigin,
                long proofEpoch) {
            this.lease = Objects.requireNonNull(lease, "lease");
            this.initiatingReason =
                    Objects.requireNonNull(initiatingReason, "initiatingReason");
            this.failureOrigin = Objects.requireNonNull(failureOrigin, "failureOrigin");
            if (proofEpoch < 0L) {
                throw new IllegalArgumentException("proofEpoch must not be negative");
            }
            this.proofEpoch = proofEpoch;
        }

        static QuiescenceProof stop(
                PhysicalTableWriterLease lease,
                long proofEpoch,
                FailureOrigin failureOrigin,
                boolean schedulerTerminated,
                boolean ingressQuiescent) {
            if (!schedulerTerminated || !ingressQuiescent) {
                throw new IllegalStateException(
                        "STOP proof requires terminated scheduler and quiescent ingress");
            }
            return new QuiescenceProof(
                    lease, InitiatingReason.STOP, failureOrigin, proofEpoch);
        }

        static QuiescenceProof ddl(
                PhysicalTableWriterLease lease,
                long proofEpoch,
                FailureOrigin failureOrigin,
                Object tableLockIdentity,
                boolean drainingGuardHeld) {
            Objects.requireNonNull(tableLockIdentity, "tableLockIdentity");
            if (!drainingGuardHeld) {
                throw new IllegalStateException("DDL proof requires the table draining guard");
            }
            return new QuiescenceProof(
                    lease, InitiatingReason.DDL, failureOrigin, proofEpoch);
        }

        static QuiescenceProof factoryRollback(
                PhysicalTableWriterLease lease,
                long proofEpoch,
                FailureOrigin failureOrigin,
                boolean unpublished,
                boolean writerFirstUseObserved,
                boolean compactionSubmitted) {
            if (!unpublished || writerFirstUseObserved || compactionSubmitted) {
                throw new IllegalStateException(
                        "FACTORY_ROLLBACK proof requires unpublished and unused writer resources");
            }
            return new QuiescenceProof(
                    lease,
                    InitiatingReason.FACTORY_ROLLBACK,
                    failureOrigin,
                    proofEpoch);
        }

        PhysicalTableWriterLease lease() {
            return lease;
        }

        InitiatingReason initiatingReason() {
            return initiatingReason;
        }

        FailureOrigin failureOrigin() {
            return failureOrigin;
        }

        long proofEpoch() {
            return proofEpoch;
        }
    }

    static final class MaintenanceOutcome {
        private static final MaintenanceOutcome SUCCESS =
                new MaintenanceOutcome(MaintenanceStatus.SUCCESS, null);
        private static final MaintenanceOutcome WAITING =
                new MaintenanceOutcome(MaintenanceStatus.WAITING, null);

        private final MaintenanceStatus status;
        private final Throwable failure;

        private MaintenanceOutcome(MaintenanceStatus status, Throwable failure) {
            this.status = Objects.requireNonNull(status, "status");
            this.failure = failure;
        }

        static MaintenanceOutcome success() {
            return SUCCESS;
        }

        static MaintenanceOutcome failedDrained(Throwable failure) {
            return new MaintenanceOutcome(
                    MaintenanceStatus.FAILED_DRAINED,
                    Objects.requireNonNull(failure, "failure"));
        }

        static MaintenanceOutcome waiting() {
            return WAITING;
        }

        MaintenanceStatus status() {
            return status;
        }

        Throwable failure() {
            return failure;
        }
    }

    /**
     * Monotonic state holder for one close operation.
     *
     * <p>A DDL caller may stop waiting without terminating the operation. Only a valid STOP proof
     * for the same exact generation can resume that deferred operation. Once retained, the first
     * terminal state and failure are sticky for the rest of the process.
     */
    static final class CloseOperation {
        private final QuiescenceProof initiatingProof;
        private CloseState state = CloseState.WAITING_COMPACTION;
        private Throwable terminalFailure;

        CloseOperation(QuiescenceProof initiatingProof) {
            this.initiatingProof = Objects.requireNonNull(initiatingProof, "initiatingProof");
        }

        synchronized CloseState state() {
            return state;
        }

        synchronized Throwable terminalFailure() {
            return terminalFailure;
        }

        synchronized void recordCompactionTerminated() {
            requireState(CloseState.WAITING_COMPACTION);
            state = CloseState.WAITING_COMMIT_MAINTENANCE;
        }

        synchronized void deferTermination() {
            if (initiatingProof.initiatingReason() != InitiatingReason.DDL) {
                throw new IllegalStateException(
                        "Only a DDL caller may defer an active close operation");
            }
            if (state != CloseState.WAITING_COMPACTION
                    && state != CloseState.WAITING_COMMIT_MAINTENANCE) {
                throw new IllegalStateException("Close operation is not actively waiting");
            }
            state = CloseState.CLOSE_DEFERRED_TERMINATION;
        }

        synchronized void resumeForStop(
                QuiescenceProof stopProof, CloseState waitingPhase) {
            Objects.requireNonNull(stopProof, "stopProof");
            if (state != CloseState.CLOSE_DEFERRED_TERMINATION) {
                throw new IllegalStateException("Only a deferred operation can be joined by STOP");
            }
            if (stopProof.initiatingReason() != InitiatingReason.STOP
                    || !initiatingProof.lease().equals(stopProof.lease())) {
                throw new IllegalStateException(
                        "STOP join proof must match the exact deferred generation");
            }
            if (waitingPhase != CloseState.WAITING_COMPACTION
                    && waitingPhase != CloseState.WAITING_COMMIT_MAINTENANCE) {
                throw new IllegalArgumentException("waitingPhase must be an active wait phase");
            }
            state = waitingPhase;
        }

        synchronized CloseState retain(CloseState retainedState, Throwable failure) {
            Objects.requireNonNull(retainedState, "retainedState");
            Objects.requireNonNull(failure, "failure");
            if (state.isTerminal()) {
                return state;
            }
            if (!retainedState.isRetained()) {
                throw new IllegalArgumentException("The requested state is not retained");
            }
            state = retainedState;
            terminalFailure = failure;
            return state;
        }

        synchronized void recordClosedSuccess(
                boolean compactionTerminated,
                MaintenanceOutcome maintenance,
                DelegateCloseProgress progress) {
            requireState(CloseState.WAITING_COMMIT_MAINTENANCE);
            CloseState proven =
                    Objects.requireNonNull(progress, "progress")
                            .retainedOrClosedState(compactionTerminated, maintenance);
            if (proven != CloseState.CLOSED_SUCCESS) {
                throw new IllegalStateException(
                        "A close operation needs complete successful dependency evidence");
            }
            state = proven;
        }

        private void requireState(CloseState expected) {
            if (state != expected) {
                throw new IllegalStateException(
                        "Expected close state " + expected + " but was " + state);
            }
        }
    }

    static final class DelegateCloseOutcome {
        private static final DelegateCloseOutcome NOT_ATTEMPTED =
                new DelegateCloseOutcome(DelegateCloseStatus.NOT_ATTEMPTED, null);
        private static final DelegateCloseOutcome SUCCEEDED =
                new DelegateCloseOutcome(DelegateCloseStatus.SUCCEEDED, null);

        private final DelegateCloseStatus status;
        private final Throwable failure;

        private DelegateCloseOutcome(DelegateCloseStatus status, Throwable failure) {
            this.status = Objects.requireNonNull(status, "status");
            this.failure = failure;
        }

        private static DelegateCloseOutcome failed(Throwable failure) {
            return new DelegateCloseOutcome(
                    DelegateCloseStatus.FAILED_RETAINED,
                    Objects.requireNonNull(failure, "failure"));
        }

        DelegateCloseStatus status() {
            return status;
        }

        Throwable failure() {
            return failure;
        }

        boolean succeeded() {
            return status == DelegateCloseStatus.SUCCEEDED;
        }
    }

    /** Per-dependency monotonic progress; a delegate that threw is never invoked again. */
    static final class DelegateCloseProgress {
        private DelegateCloseOutcome writer = DelegateCloseOutcome.NOT_ATTEMPTED;
        private DelegateCloseOutcome committer = DelegateCloseOutcome.NOT_ATTEMPTED;
        private DelegateCloseOutcome io = DelegateCloseOutcome.NOT_ATTEMPTED;

        synchronized DelegateCloseOutcome closeWriterOnce(CloseAction closeAction) {
            if (writer.status() == DelegateCloseStatus.NOT_ATTEMPTED) {
                writer = invoke(closeAction);
            }
            return writer;
        }

        synchronized DelegateCloseOutcome closeCommitterOnce(CloseAction closeAction) {
            if (committer.status() == DelegateCloseStatus.NOT_ATTEMPTED) {
                committer = invoke(closeAction);
            }
            return committer;
        }

        synchronized DelegateCloseOutcome closeIoOnceIfSafe(
                boolean compactionTerminated,
                MaintenanceOutcome maintenance,
                CloseAction closeAction) {
            Objects.requireNonNull(maintenance, "maintenance");
            if (!canReleaseIo(compactionTerminated, writer, maintenance, committer)) {
                return io;
            }
            if (io.status() == DelegateCloseStatus.NOT_ATTEMPTED) {
                io = invoke(closeAction);
            }
            return io;
        }

        synchronized DelegateCloseOutcome writerOutcome() {
            return writer;
        }

        synchronized DelegateCloseOutcome committerOutcome() {
            return committer;
        }

        synchronized DelegateCloseOutcome ioOutcome() {
            return io;
        }

        synchronized CloseState retainedOrClosedState(
                boolean compactionTerminated, MaintenanceOutcome maintenance) {
            Objects.requireNonNull(maintenance, "maintenance");
            if (!compactionTerminated || maintenance.status() == MaintenanceStatus.WAITING) {
                throw new IllegalStateException("Termination is still active; no terminal state exists");
            }
            if (!writer.succeeded()
                    || !committer.succeeded()
                    || maintenance.status() != MaintenanceStatus.SUCCESS) {
                return CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED;
            }
            if (io.status() == DelegateCloseStatus.FAILED_RETAINED) {
                return CloseState.IO_CLOSE_FAILED_RETAINED;
            }
            if (io.status() == DelegateCloseStatus.SUCCEEDED) {
                return CloseState.CLOSED_SUCCESS;
            }
            throw new IllegalStateException("IO close has not completed");
        }

        private static DelegateCloseOutcome invoke(CloseAction closeAction) {
            Objects.requireNonNull(closeAction, "closeAction");
            try {
                closeAction.close();
                return DelegateCloseOutcome.SUCCEEDED;
            } catch (Throwable failure) {
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                return DelegateCloseOutcome.failed(failure);
            }
        }
    }

    static boolean canReleaseIo(
            boolean compactionTerminated,
            DelegateCloseOutcome writer,
            MaintenanceOutcome maintenance,
            DelegateCloseOutcome committer) {
        return compactionTerminated
                && Objects.requireNonNull(writer, "writer").succeeded()
                && Objects.requireNonNull(maintenance, "maintenance").status()
                        == MaintenanceStatus.SUCCESS
                && maintenance.failure() == null
                && Objects.requireNonNull(committer, "committer").succeeded();
    }
}
