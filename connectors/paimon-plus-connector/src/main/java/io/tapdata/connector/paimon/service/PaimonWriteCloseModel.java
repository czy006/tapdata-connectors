package io.tapdata.connector.paimon.service;

import java.util.Objects;

/** Proof, state, and exactly-once dependency-close model for one writer generation. */
public final class PaimonWriteCloseModel {

    private PaimonWriteCloseModel() {}

    public enum InitiatingReason {
        STOP,
        DDL,
        FACTORY_ROLLBACK
    }

    public enum FailureOrigin {
        NONE,
        WRITE_PATH,
        DDL_PATH,
        STOP_DRAIN,
        FACTORY_CONSTRUCTION,
        DEPENDENCY_CLOSE
    }

    public enum CloseState {
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

        public boolean isActiveWaiting() {
            return activeWaiting;
        }

        public boolean isTerminal() {
            return terminal;
        }

        public boolean isRetained() {
            return retained;
        }
    }

    public enum MaintenanceStatus {
        SUCCESS,
        FAILED_DRAINED,
        WAITING
    }

    public enum DelegateCloseStatus {
        NOT_ATTEMPTED,
        IN_PROGRESS,
        SUCCEEDED,
        FAILED_RETAINED
    }

    public interface CloseAction {
        void close() throws Exception;
    }

    /** Immutable proof minted only after the caller has established a reason-specific barrier. */
    public static final class QuiescenceProof {
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

        /**
         * Returns the opaque, immutable lease capability without exposing its package-private
         * implementation type across the service/write boundary.
         */
        public Object generationCapability() {
            return lease;
        }

        public boolean matchesGenerationCapability(Object expectedCapability) {
            return lease.equals(expectedCapability);
        }

        public InitiatingReason initiatingReason() {
            return initiatingReason;
        }

        public FailureOrigin failureOrigin() {
            return failureOrigin;
        }

        public long proofEpoch() {
            return proofEpoch;
        }
    }

    public static final class MaintenanceOutcome {
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

        public static MaintenanceOutcome success() {
            return SUCCESS;
        }

        public static MaintenanceOutcome failedDrained(Throwable failure) {
            return new MaintenanceOutcome(
                    MaintenanceStatus.FAILED_DRAINED,
                    Objects.requireNonNull(failure, "failure"));
        }

        public static MaintenanceOutcome waiting() {
            return WAITING;
        }

        public MaintenanceStatus status() {
            return status;
        }

        public Throwable failure() {
            return failure;
        }
    }

    public static final class DelegateCloseOutcome {
        private static final DelegateCloseOutcome NOT_ATTEMPTED =
                new DelegateCloseOutcome(DelegateCloseStatus.NOT_ATTEMPTED, null);
        private static final DelegateCloseOutcome IN_PROGRESS =
                new DelegateCloseOutcome(DelegateCloseStatus.IN_PROGRESS, null);
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

        public DelegateCloseStatus status() {
            return status;
        }

        public Throwable failure() {
            return failure;
        }

        public boolean succeeded() {
            return status == DelegateCloseStatus.SUCCEEDED;
        }
    }

    /** Per-dependency monotonic progress; a delegate that threw is never invoked again. */
    public static final class DelegateCloseProgress {
        private DelegateCloseOutcome writer = DelegateCloseOutcome.NOT_ATTEMPTED;
        private DelegateCloseOutcome committer = DelegateCloseOutcome.NOT_ATTEMPTED;
        private DelegateCloseOutcome io = DelegateCloseOutcome.NOT_ATTEMPTED;

        public DelegateCloseOutcome closeWriterOnce(CloseAction closeAction) {
            return closeOnce(Resource.WRITER, closeAction);
        }

        public DelegateCloseOutcome closeCommitterOnce(CloseAction closeAction) {
            return closeOnce(Resource.COMMITTER, closeAction);
        }

        public DelegateCloseOutcome closeIoOnceIfSafe(
                boolean compactionTerminated,
                MaintenanceOutcome maintenance,
                CloseAction closeAction) {
            Objects.requireNonNull(maintenance, "maintenance");
            synchronized (this) {
                if (!canReleaseIo(compactionTerminated, writer, maintenance, committer)) {
                    return io;
                }
            }
            return closeOnce(Resource.IO, closeAction);
        }

        public synchronized DelegateCloseOutcome writerOutcome() {
            return writer;
        }

        public synchronized DelegateCloseOutcome committerOutcome() {
            return committer;
        }

        public synchronized DelegateCloseOutcome ioOutcome() {
            return io;
        }

        public synchronized CloseState retainedOrClosedState(
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

        private DelegateCloseOutcome closeOnce(Resource resource, CloseAction closeAction) {
            Objects.requireNonNull(closeAction, "closeAction");
            synchronized (this) {
                DelegateCloseOutcome current = outcome(resource);
                if (current.status() != DelegateCloseStatus.NOT_ATTEMPTED) {
                    return current;
                }
                setOutcome(resource, DelegateCloseOutcome.IN_PROGRESS);
            }

            DelegateCloseOutcome completed = invoke(closeAction);
            synchronized (this) {
                setOutcome(resource, completed);
                return completed;
            }
        }

        private DelegateCloseOutcome outcome(Resource resource) {
            switch (resource) {
                case WRITER:
                    return writer;
                case COMMITTER:
                    return committer;
                case IO:
                    return io;
                default:
                    throw new IllegalStateException("Unknown close resource " + resource);
            }
        }

        private void setOutcome(Resource resource, DelegateCloseOutcome outcome) {
            switch (resource) {
                case WRITER:
                    writer = outcome;
                    return;
                case COMMITTER:
                    committer = outcome;
                    return;
                case IO:
                    io = outcome;
                    return;
                default:
                    throw new IllegalStateException("Unknown close resource " + resource);
            }
        }

        private enum Resource {
            WRITER,
            COMMITTER,
            IO
        }
    }

    public static boolean canReleaseIo(
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
