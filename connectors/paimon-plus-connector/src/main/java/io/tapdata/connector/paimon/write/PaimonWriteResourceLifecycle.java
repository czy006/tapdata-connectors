package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.service.PaimonWriteCloseModel;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Single structured close operation for every resource owned by one writer generation.
 *
 * <p>The safety order is fixed: exact quiescence proof, graceful compaction shutdown, actual
 * executor termination, writer, structured commit maintenance, committer, IO manager, spill
 * registration. Physical generation lease ownership is deliberately outside this class: the
 * coordinator or factory construction envelope releases or transfers that exact lease only after
 * this lifecycle publishes {@link PaimonWriteCloseModel.CloseState#CLOSED_SUCCESS}. A dependency
 * failure is terminal and retained; it never authorizes a later in-process retry of a partially
 * closed resource or release of its physical lease.
 *
 * <p>Paimon source contract: a terminal result from {@code
 * TableCommitMaintenance#shutdownAndAwait} is published only after its executor, every accepted
 * maintenance runnable and its resource closer have returned. {@code FAILED_DRAINED} proves
 * drain, but explicitly does not authorize external IO release. See patched Paimon 1.3.2 {@code
 * paimon-core/.../TableCommitMaintenance.java}, lines 34-170.
 */
public final class PaimonWriteResourceLifecycle {

    enum StepState {
        NOT_STARTED,
        IN_PROGRESS,
        WAITING,
        SUCCEEDED,
        FAILED
    }

    /** Opaque identity and monotonic state for the one close operation of this generation. */
    static final class CloseOperation {
        private final UUID operationId = UUID.randomUUID();
        private final PaimonWriteCloseModel.QuiescenceProof initiatingProof;
        private PaimonWriteCloseModel.InitiatingReason activeJoinReason;
        private PaimonWriteCloseModel.CloseState state =
                PaimonWriteCloseModel.CloseState.WAITING_COMPACTION;
        private PaimonWriteCloseModel.CloseState deferredPhase;
        private Throwable terminalFailure;
        private boolean finalizationClaimed;

        private CloseOperation(PaimonWriteCloseModel.QuiescenceProof initiatingProof) {
            this.initiatingProof = Objects.requireNonNull(initiatingProof, "initiatingProof");
            this.activeJoinReason = initiatingProof.initiatingReason();
        }

        synchronized PaimonWriteCloseModel.CloseState state() {
            return state;
        }

        synchronized Throwable terminalFailure() {
            return terminalFailure;
        }

        private synchronized CloseOutcome snapshot(
                PaimonWriteResourceLifecycle ownerIdentity,
                Object exactGenerationCapability) {
            return new CloseOutcome(
                    ownerIdentity,
                    this,
                    exactGenerationCapability,
                    initiatingReason(),
                    activeJoinReason,
                    state,
                    terminalFailure);
        }

        private PaimonWriteCloseModel.InitiatingReason initiatingReason() {
            return initiatingProof.initiatingReason();
        }

        private synchronized void joinForStop() {
            if (initiatingReason() != PaimonWriteCloseModel.InitiatingReason.DDL) {
                throw new IllegalStateException("Only a DDL operation can be joined by STOP");
            }
            if (finalizationClaimed) {
                throw new IllegalStateException(
                        "DDL resource outcome was already claimed by its scenario finalizer");
            }
            activeJoinReason = PaimonWriteCloseModel.InitiatingReason.STOP;
            if (state == PaimonWriteCloseModel.CloseState.CLOSE_DEFERRED_TERMINATION) {
                resumeForStop();
            }
        }

        private synchronized void recordCompactionTerminated() {
            requireState(PaimonWriteCloseModel.CloseState.WAITING_COMPACTION);
            state = PaimonWriteCloseModel.CloseState.WAITING_COMMIT_MAINTENANCE;
        }

        private synchronized void deferForDdl() {
            if (initiatingReason() != PaimonWriteCloseModel.InitiatingReason.DDL) {
                throw new IllegalStateException("Only a DDL operation can be deferred");
            }
            if (state != PaimonWriteCloseModel.CloseState.WAITING_COMPACTION
                    && state
                            != PaimonWriteCloseModel.CloseState.WAITING_COMMIT_MAINTENANCE) {
                throw new IllegalStateException("Close operation is not actively waiting");
            }
            deferredPhase = state;
            state = PaimonWriteCloseModel.CloseState.CLOSE_DEFERRED_TERMINATION;
        }

        /**
         * Atomically resolves a timed wait against a concurrent DDL-to-STOP join.
         *
         * <p>If STOP wins the monitor first this method observes STOP and keeps the operation
         * active. If DDL wins first it publishes the deferred phase, which {@link #joinForStop()}
         * immediately resumes while holding this same monitor.
         */
        private synchronized void transitionOnWaiting(Throwable factoryFailure) {
            switch (activeJoinReason) {
                case STOP:
                    return;
                case DDL:
                    deferForDdl();
                    return;
                case FACTORY_ROLLBACK:
                    retain(
                            PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                            Objects.requireNonNull(factoryFailure, "factoryFailure"));
                    return;
                default:
                    throw new IllegalStateException("Unknown close reason " + activeJoinReason);
            }
        }

        private synchronized void transitionOnInterruptedWait(
                InterruptedException interrupted) {
            switch (activeJoinReason) {
                case STOP:
                    return;
                case DDL:
                    deferForDdl();
                    return;
                case FACTORY_ROLLBACK:
                    retain(
                            PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                            interrupted);
                    return;
                default:
                    throw new IllegalStateException("Unknown close reason " + activeJoinReason);
            }
        }

        private synchronized void resumeForStop() {
            requireState(PaimonWriteCloseModel.CloseState.CLOSE_DEFERRED_TERMINATION);
            if (initiatingReason() != PaimonWriteCloseModel.InitiatingReason.DDL
                    || deferredPhase == null) {
                throw new IllegalStateException("Only a deferred DDL operation can be joined");
            }
            state = deferredPhase;
            deferredPhase = null;
        }

        private synchronized void retain(
                PaimonWriteCloseModel.CloseState retainedState, Throwable failure) {
            Objects.requireNonNull(retainedState, "retainedState");
            Objects.requireNonNull(failure, "failure");
            if (state.isTerminal()) {
                return;
            }
            if (!retainedState.isRetained()) {
                throw new IllegalArgumentException("Requested state is not retained");
            }
            state = retainedState;
            terminalFailure = failure;
        }

        private synchronized void recordClosedSuccess() {
            requireState(PaimonWriteCloseModel.CloseState.WAITING_COMMIT_MAINTENANCE);
            state = PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS;
        }

        private synchronized void claimFinalization(
                CloseOutcome evidence,
                PaimonWriteCloseModel.InitiatingReason expectedReason,
                boolean requireClosedSuccess) {
            if (evidence.operationIdentity != this
                    || evidence.state != state
                    || evidence.failure != terminalFailure) {
                throw new IllegalArgumentException(
                        "Resource outcome is stale or belongs to another close operation");
            }
            if (activeJoinReason != expectedReason
                    || evidence.effectiveReason != activeJoinReason) {
                throw new IllegalArgumentException(
                        "Resource outcome no longer owns the requested finalization reason");
            }
            if (requireClosedSuccess) {
                if (state != PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS
                        || terminalFailure != null) {
                    throw new IllegalStateException(
                            "Finalization requires a current CLOSED_SUCCESS resource outcome");
                }
            } else if (!state.isRetained() || terminalFailure == null) {
                throw new IllegalStateException(
                        "Retained finalization requires a current terminal retained outcome");
            }
            if (finalizationClaimed) {
                throw new IllegalStateException(
                        "Resource finalization authority was already claimed");
            }
            finalizationClaimed = true;
        }

        private void requireState(PaimonWriteCloseModel.CloseState expected) {
            if (state != expected) {
                throw new IllegalStateException(
                        "Expected close state " + expected + " but was " + state);
            }
        }

        @Override
        public boolean equals(Object other) {
            return this == other;
        }

        @Override
        public int hashCode() {
            return operationId.hashCode();
        }
    }

    public static final class CloseOutcome {
        private final PaimonWriteResourceLifecycle ownerIdentity;
        private final CloseOperation operationIdentity;
        private final Object exactGenerationCapability;
        private final PaimonWriteCloseModel.InitiatingReason initiatingReason;
        private final PaimonWriteCloseModel.InitiatingReason effectiveReason;
        private final PaimonWriteCloseModel.CloseState state;
        private final Throwable failure;

        private CloseOutcome(
                PaimonWriteResourceLifecycle ownerIdentity,
                CloseOperation operationIdentity,
                Object exactGenerationCapability,
                PaimonWriteCloseModel.InitiatingReason initiatingReason,
                PaimonWriteCloseModel.InitiatingReason effectiveReason,
                PaimonWriteCloseModel.CloseState state,
                Throwable failure) {
            this.ownerIdentity = Objects.requireNonNull(ownerIdentity, "ownerIdentity");
            this.operationIdentity =
                    Objects.requireNonNull(operationIdentity, "operationIdentity");
            this.exactGenerationCapability =
                    Objects.requireNonNull(
                            exactGenerationCapability, "exactGenerationCapability");
            this.initiatingReason =
                    Objects.requireNonNull(initiatingReason, "initiatingReason");
            this.effectiveReason =
                    Objects.requireNonNull(effectiveReason, "effectiveReason");
            this.state = Objects.requireNonNull(state, "state");
            this.failure = failure;
        }

        public PaimonWriteCloseModel.CloseState state() {
            return state;
        }

        public Throwable failure() {
            return failure;
        }

        public boolean terminal() {
            return state.isTerminal();
        }

        public PaimonWriteCloseModel.InitiatingReason initiatingReason() {
            return initiatingReason;
        }

        /** Finalization authority after applying the only legal cross-scenario join: DDL to STOP. */
        public PaimonWriteCloseModel.InitiatingReason effectiveReason() {
            return effectiveReason;
        }

        public boolean belongsTo(PaimonWriteResourceLifecycle expectedOwner) {
            return ownerIdentity == expectedOwner;
        }

        public boolean matchesGenerationCapability(Object expectedCapability) {
            return exactGenerationCapability == expectedCapability;
        }
    }

    static final class CloseSnapshot {
        private final CloseOutcome outcome;
        private final boolean operationPresent;
        private final boolean compactionShutdownStarted;
        private final boolean compactionTerminated;
        private final PaimonWriteCloseModel.DelegateCloseStatus writer;
        private final StepState maintenance;
        private final PaimonWriteCloseModel.DelegateCloseStatus committer;
        private final PaimonWriteCloseModel.DelegateCloseStatus io;
        private final StepState spill;

        private CloseSnapshot(
                CloseOutcome outcome,
                boolean operationPresent,
                boolean compactionShutdownStarted,
                boolean compactionTerminated,
                PaimonWriteCloseModel.DelegateCloseStatus writer,
                StepState maintenance,
                PaimonWriteCloseModel.DelegateCloseStatus committer,
                PaimonWriteCloseModel.DelegateCloseStatus io,
                StepState spill) {
            this.outcome = outcome;
            this.operationPresent = operationPresent;
            this.compactionShutdownStarted = compactionShutdownStarted;
            this.compactionTerminated = compactionTerminated;
            this.writer = writer;
            this.maintenance = maintenance;
            this.committer = committer;
            this.io = io;
            this.spill = spill;
        }

        CloseOutcome outcome() {
            return outcome;
        }

        boolean operationPresent() {
            return operationPresent;
        }

        boolean compactionShutdownStarted() {
            return compactionShutdownStarted;
        }

        boolean compactionTerminated() {
            return compactionTerminated;
        }

        PaimonWriteCloseModel.DelegateCloseStatus writer() {
            return writer;
        }

        StepState maintenance() {
            return maintenance;
        }

        PaimonWriteCloseModel.DelegateCloseStatus committer() {
            return committer;
        }

        PaimonWriteCloseModel.DelegateCloseStatus io() {
            return io;
        }

        StepState spill() {
            return spill;
        }

    }

    private final Object coordination = new Object();
    private final Object exactGenerationCapability;
    private final PaimonCompactionRuntime compactionRuntime;
    private final PaimonWriteCloseModel.CloseAction writerClose;
    private final PaimonMaintenanceAdapter maintenance;
    private final PaimonWriteCloseModel.CloseAction committerClose;
    private final PaimonWriteCloseModel.CloseAction ioClose;
    private final OnceCloseStep spillUnregister;
    private final PaimonWriteCloseModel.DelegateCloseProgress dependencyProgress =
            new PaimonWriteCloseModel.DelegateCloseProgress();
    private final MaintenanceProgress maintenanceProgress = new MaintenanceProgress();
    private final OnceCloseStep compactionShutdown;
    private volatile boolean submissionFreeAtShutdownFence;

    private CloseOperation closeOperation;
    private boolean finishRoundRunning;

    PaimonWriteResourceLifecycle(
            Object exactGenerationCapability,
            PaimonCompactionRuntime compactionRuntime,
            PaimonWriteCloseModel.CloseAction writerClose,
            PaimonMaintenanceAdapter maintenance,
            PaimonWriteCloseModel.CloseAction committerClose,
            PaimonWriteCloseModel.CloseAction ioClose,
            PaimonWriteCloseModel.CloseAction spillUnregister) {
        this.exactGenerationCapability =
                Objects.requireNonNull(
                        exactGenerationCapability, "exactGenerationCapability");
        this.compactionRuntime = Objects.requireNonNull(compactionRuntime, "compactionRuntime");
        this.compactionShutdown =
                new OnceCloseStep(
                        () ->
                                submissionFreeAtShutdownFence =
                                        compactionRuntime.beginShutdown());
        this.writerClose = Objects.requireNonNull(writerClose, "writerClose");
        this.maintenance = Objects.requireNonNull(maintenance, "maintenance");
        this.committerClose = Objects.requireNonNull(committerClose, "committerClose");
        this.ioClose = Objects.requireNonNull(ioClose, "ioClose");
        this.spillUnregister =
                new OnceCloseStep(
                        Objects.requireNonNull(spillUnregister, "spillUnregister"));
    }

    /** Used only by the one-shot ownership binding created by the service coordinator. */
    public boolean matchesGenerationCapability(Object expectedCapability) {
        return exactGenerationCapability == expectedCapability;
    }

    /** Atomically consumes current success authority before an outer ownership transition. */
    public void claimClosedSuccess(
            CloseOutcome evidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason) {
        claimFinalization(evidence, expectedReason, true);
    }

    /** Atomically consumes current retained authority before publishing a retained carrier. */
    public void claimRetained(
            CloseOutcome evidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason) {
        claimFinalization(evidence, expectedReason, false);
    }

    CloseOperation beginClose(
            PaimonWriteCloseModel.QuiescenceProof proof) {
        PaimonWriteCloseModel.QuiescenceProof checked =
                Objects.requireNonNull(proof, "proof");
        if (!checked.matchesGenerationCapability(exactGenerationCapability)) {
            throw new IllegalArgumentException(
                    "Quiescence proof does not match the exact writer generation");
        }
        synchronized (coordination) {
            if (closeOperation == null) {
                closeOperation = new CloseOperation(checked);
                return closeOperation;
            }
            PaimonWriteCloseModel.InitiatingReason initiating =
                    closeOperation.initiatingReason();
            PaimonWriteCloseModel.InitiatingReason joining = checked.initiatingReason();
            if (initiating == PaimonWriteCloseModel.InitiatingReason.STOP
                    && joining == PaimonWriteCloseModel.InitiatingReason.STOP) {
                return closeOperation;
            }
            if (initiating == PaimonWriteCloseModel.InitiatingReason.DDL
                    && joining == PaimonWriteCloseModel.InitiatingReason.STOP) {
                closeOperation.joinForStop();
                return closeOperation;
            }
            throw new IllegalStateException(
                    "Paimon close join is forbidden for " + initiating + " -> " + joining);
        }
    }

    void beginCompactionShutdown(CloseOperation operation) {
        requireOwnedOperation(operation);
        compactionShutdown.start();
    }

    CloseOutcome awaitAndFinish(
            CloseOperation operation,
            long absoluteDeadlineNanos)
            throws InterruptedException {
        requireOwnedOperation(operation);
        beginCompactionShutdown(operation);
        OnceCloseStep.Attempt shutdownAttempt;
        try {
            shutdownAttempt = compactionShutdown.awaitCompletion(absoluteDeadlineNanos);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            transitionOnInterruptedWait(operation, interrupted);
            throw interrupted;
        }
        if (!shutdownAttempt.completed()) {
            return transitionOnWaiting(
                    operation,
                    new IllegalStateException(
                            "Paimon compaction shutdown did not complete before the join deadline"));
        }
        Throwable shutdownFailure = shutdownAttempt.failure();
        if (shutdownFailure != null) {
            operation.retain(retainedDependencyState(operation), shutdownFailure);
            return outcome(operation);
        }
        if (operation.initiatingReason()
                        == PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK
                && !submissionFreeAtShutdownFence) {
            operation.retain(
                    PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                    new IllegalStateException(
                            "FACTORY_ROLLBACK proof conflicts with a pre-fence compaction submission"));
            return outcome(operation);
        }
        if (!claimFinishRound(operation, absoluteDeadlineNanos)) {
            return outcome(operation);
        }
        try {
            return finishRound(operation, absoluteDeadlineNanos);
        } finally {
            synchronized (coordination) {
                finishRoundRunning = false;
                coordination.notifyAll();
            }
        }
    }

    CloseSnapshot closeSnapshot() {
        CloseOperation operation;
        synchronized (coordination) {
            operation = closeOperation;
        }
        CloseOutcome outcome = operation == null ? null : outcome(operation);
        return new CloseSnapshot(
                outcome,
                operation != null,
                compactionShutdown.state() != StepState.NOT_STARTED,
                compactionRuntime.terminated(),
                dependencyProgress.writerOutcome().status(),
                maintenanceProgress.state(),
                dependencyProgress.committerOutcome().status(),
                dependencyProgress.ioOutcome().status(),
                spillUnregister.state());
    }

    private CloseOutcome finishRound(
            CloseOperation operation,
            long absoluteDeadlineNanos)
            throws InterruptedException {
        PaimonWriteCloseModel.CloseState state = operation.state();
        if (state.isTerminal() || state == PaimonWriteCloseModel.CloseState.CLOSE_DEFERRED_TERMINATION) {
            return outcome(operation);
        }

        if (state == PaimonWriteCloseModel.CloseState.WAITING_COMPACTION) {
            final boolean terminated;
            try {
                terminated = compactionRuntime.awaitTermination(absoluteDeadlineNanos);
            } catch (InterruptedException interrupted) {
                transitionOnInterruptedWait(operation, interrupted);
                throw interrupted;
            }
            if (!terminated) {
                return transitionOnWaiting(
                        operation,
                        new IllegalStateException(
                                "Paimon compaction did not terminate before the join deadline"));
            }
            if (!compactionRuntime.terminated()) {
                operation.retain(
                        retainedDependencyState(operation),
                        new IllegalStateException(
                                "Paimon compaction termination was not positively observed"));
                return outcome(operation);
            }
            operation.recordCompactionTerminated();
        }

        CloseOutcome expired =
                stopBeforeNewPhaseIfDeadlineExpired(operation, absoluteDeadlineNanos);
        if (expired != null) {
            return expired;
        }
        boolean writerAttemptedThisRound =
                dependencyProgress.writerOutcome().status()
                        == PaimonWriteCloseModel.DelegateCloseStatus.NOT_ATTEMPTED;
        PaimonWriteCloseModel.DelegateCloseOutcome writer =
                dependencyProgress.closeWriterOnce(writerClose);
        InterruptedException pendingInterruption =
                writerAttemptedThisRound ? interruption(writer.failure()) : null;
        if (pendingInterruption != null) {
            // Delegate progress restores the flag when it captures the exact exception. Clear it
            // only while completing the mandatory maintenance/committer safety tail; restore it
            // immediately before propagating the same object to the caller.
            Thread.interrupted();
        }
        if (writer.failure() == null) {
            expired = stopBeforeNewPhaseIfDeadlineExpired(operation, absoluteDeadlineNanos);
            if (expired != null) {
                return expired;
            }
        }
        MaintenanceAttempt maintenanceAttempt =
                maintenanceProgress.shutdownAndAwait(
                        maintenance,
                        remainingNanos(absoluteDeadlineNanos, System.nanoTime()));
        if (maintenanceAttempt.invocationFailure() != null) {
            Throwable failure =
                    append(writer.failure(), maintenanceAttempt.invocationFailure());
            operation.retain(retainedDependencyState(operation), failure);
            rethrowInterruption(pendingInterruption);
            return outcome(operation);
        }
        PaimonWriteCloseModel.MaintenanceOutcome maintenanceOutcome =
                maintenanceAttempt.outcome();
        if (maintenanceOutcome.status()
                == PaimonWriteCloseModel.MaintenanceStatus.WAITING) {
            InterruptedException maintenanceInterruption =
                    maintenanceAttempt.interruption();
            Throwable waitingFailure =
                    maintenanceInterruption != null
                            ? maintenanceInterruption
                            : new IllegalStateException(
                                    "Paimon commit maintenance did not drain before the join deadline");
            if (operation.initiatingReason()
                            == PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK
                    && writer.failure() != null) {
                operation.retain(
                        PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                        append(writer.failure(), waitingFailure));
                rethrowInterruption(
                        pendingInterruption != null
                                ? pendingInterruption
                                : maintenanceInterruption);
                return outcome(operation);
            }
            if (maintenanceInterruption != null) {
                pendingInterruption =
                        preferFirstInterruption(
                                pendingInterruption, maintenanceInterruption);
                transitionOnInterruptedWait(operation, pendingInterruption);
                rethrowInterruption(pendingInterruption);
            }
            if (pendingInterruption != null) {
                transitionOnInterruptedWait(operation, pendingInterruption);
                rethrowInterruption(pendingInterruption);
            }
            return transitionOnWaiting(operation, waitingFailure);
        }
        if (writer.failure() == null
                && maintenanceOutcome.failure() == null
                && pendingInterruption == null) {
            expired = stopBeforeNewPhaseIfDeadlineExpired(operation, absoluteDeadlineNanos);
            if (expired != null) {
                return expired;
            }
        }
        PaimonWriteCloseModel.DelegateCloseOutcome committer =
                dependencyProgress.closeCommitterOnce(committerClose);
        if (pendingInterruption == null) {
            pendingInterruption = interruption(committer.failure());
        }

        Throwable dependencyFailure = null;
        dependencyFailure = append(dependencyFailure, writer.failure());
        dependencyFailure = append(dependencyFailure, maintenanceOutcome.failure());
        dependencyFailure = append(dependencyFailure, committer.failure());
        if (dependencyFailure != null) {
            operation.retain(retainedDependencyState(operation), dependencyFailure);
            rethrowInterruption(pendingInterruption);
            return outcome(operation);
        }

        expired = stopBeforeNewPhaseIfDeadlineExpired(operation, absoluteDeadlineNanos);
        if (expired != null) {
            return expired;
        }
        PaimonWriteCloseModel.DelegateCloseOutcome io =
                dependencyProgress.closeIoOnceIfSafe(
                        compactionRuntime.terminated(), maintenanceOutcome, ioClose);
        if (io.failure() != null) {
            operation.retain(
                    retainedIoState(operation),
                    io.failure());
            rethrowInterruption(interruption(io.failure()));
            return outcome(operation);
        }

        expired = stopBeforeNewPhaseIfDeadlineExpired(operation, absoluteDeadlineNanos);
        if (expired != null) {
            return expired;
        }
        Throwable spillFailure = spillUnregister.run();
        if (spillFailure != null) {
            operation.retain(retainedDependencyState(operation), spillFailure);
            rethrowInterruption(interruption(spillFailure));
            return outcome(operation);
        }

        if (!compactionRuntime.terminated()
                || dependencyProgress.terminalEvidence(
                                compactionRuntime.terminated(), maintenanceOutcome)
                        != PaimonWriteCloseModel.DelegateTerminalEvidence.IO_CLOSED) {
            operation.retain(
                    retainedDependencyState(operation),
                    new IllegalStateException(
                            "Paimon dependency evidence changed before close publication"));
            return outcome(operation);
        }
        operation.recordClosedSuccess();
        return outcome(operation);
    }

    private boolean claimFinishRound(
            CloseOperation operation,
            long absoluteDeadlineNanos)
            throws InterruptedException {
        synchronized (coordination) {
            while (finishRoundRunning && !operation.state().isTerminal()) {
                long remaining = remainingNanos(absoluteDeadlineNanos, System.nanoTime());
                if (remaining == 0L) {
                    return false;
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(coordination, remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    transitionOnInterruptedWait(operation, interrupted);
                    throw interrupted;
                }
            }
            if (operation.state().isTerminal()) {
                return false;
            }
            if (remainingNanos(absoluteDeadlineNanos, System.nanoTime()) == 0L) {
                transitionOnWaiting(
                        operation,
                        new IllegalStateException(
                                "Paimon close deadline elapsed before a new cleanup phase"));
                return false;
            }
            finishRoundRunning = true;
            return true;
        }
    }

    private void requireOwnedOperation(CloseOperation operation) {
        Objects.requireNonNull(operation, "operation");
        synchronized (coordination) {
            if (operation != closeOperation) {
                throw new IllegalArgumentException(
                        "Close operation is not owned by this writer generation");
            }
        }
    }

    private CloseOutcome outcome(CloseOperation operation) {
        return operation.snapshot(this, exactGenerationCapability);
    }

    private void claimFinalization(
            CloseOutcome evidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason,
            boolean requireClosedSuccess) {
        CloseOutcome checked = Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(expectedReason, "expectedReason");
        if (checked.ownerIdentity != this
                || checked.exactGenerationCapability != exactGenerationCapability) {
            throw new IllegalArgumentException(
                    "Resource outcome does not belong to this exact writer generation");
        }
        synchronized (coordination) {
            if (closeOperation == null || checked.operationIdentity != closeOperation) {
                throw new IllegalArgumentException(
                        "Resource outcome does not belong to the active close operation");
            }
            closeOperation.claimFinalization(
                    checked, expectedReason, requireClosedSuccess);
        }
    }

    private static PaimonWriteCloseModel.CloseState retainedDependencyState(
            CloseOperation operation) {
        // Factory rollback has a distinct terminal state because Service must retain the
        // unpublished generation rather than treating it as an active Context close failure.
        if (operation.state().isTerminal()) {
            return operation.state();
        }
        return operation.initiatingReason()
                        == PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK
                ? PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED
                : PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED;
    }

    private static PaimonWriteCloseModel.CloseState retainedIoState(
            CloseOperation operation) {
        return operation.initiatingReason()
                        == PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK
                ? PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED
                : PaimonWriteCloseModel.CloseState.IO_CLOSE_FAILED_RETAINED;
    }

    private CloseOutcome transitionOnWaiting(
            CloseOperation operation, Throwable factoryFailure) {
        operation.transitionOnWaiting(factoryFailure);
        return outcome(operation);
    }

    private static void transitionOnInterruptedWait(
            CloseOperation operation, InterruptedException interrupted) {
        operation.transitionOnInterruptedWait(interrupted);
    }

    private CloseOutcome stopBeforeNewPhaseIfDeadlineExpired(
            CloseOperation operation, long absoluteDeadlineNanos) {
        // Once an external phase starts it is never interrupted by this deadline. The check is
        // deliberately placed only between phases, matching the cumulative admission contract.
        // Long.MAX_VALUE is the completion-driven path used by the STOP daemon.
        if (remainingNanos(absoluteDeadlineNanos, System.nanoTime()) != 0L) {
            return null;
        }
        return transitionOnWaiting(
                operation,
                new IllegalStateException(
                        "Paimon close deadline elapsed before a new cleanup phase"));
    }

    private static Throwable append(Throwable first, Throwable additional) {
        if (additional == null) {
            return first;
        }
        if (first == null) {
            return additional;
        }
        if (first != additional) {
            first.addSuppressed(additional);
        }
        return first;
    }

    private static InterruptedException interruption(Throwable failure) {
        return failure instanceof InterruptedException
                ? (InterruptedException) failure
                : null;
    }

    private static InterruptedException preferFirstInterruption(
            InterruptedException first, InterruptedException additional) {
        if (first == null) {
            return additional;
        }
        if (additional != null && additional != first) {
            first.addSuppressed(additional);
        }
        return first;
    }

    private static void rethrowInterruption(InterruptedException interrupted)
            throws InterruptedException {
        if (interrupted != null) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    private static long remainingNanos(long deadline, long now) {
        if (deadline <= now) {
            return 0L;
        }
        long remaining = deadline - now;
        return remaining > 0L ? remaining : Long.MAX_VALUE;
    }

    private static final class MaintenanceAttempt {
        private final PaimonWriteCloseModel.MaintenanceOutcome outcome;
        private final Throwable invocationFailure;
        private final InterruptedException interruption;

        private MaintenanceAttempt(
                PaimonWriteCloseModel.MaintenanceOutcome outcome,
                Throwable invocationFailure,
                InterruptedException interruption) {
            this.outcome = outcome;
            this.invocationFailure = invocationFailure;
            this.interruption = interruption;
        }

        private static MaintenanceAttempt outcome(
                PaimonWriteCloseModel.MaintenanceOutcome outcome,
                InterruptedException interruption) {
            return new MaintenanceAttempt(
                    Objects.requireNonNull(outcome, "outcome"), null, interruption);
        }

        private static MaintenanceAttempt invocationFailure(Throwable failure) {
            return new MaintenanceAttempt(
                    null, Objects.requireNonNull(failure, "failure"), null);
        }

        PaimonWriteCloseModel.MaintenanceOutcome outcome() {
            return outcome;
        }

        Throwable invocationFailure() {
            return invocationFailure;
        }

        InterruptedException interruption() {
            return interruption;
        }
    }

    private static final class MaintenanceProgress {
        private StepState state = StepState.NOT_STARTED;
        private PaimonWriteCloseModel.MaintenanceOutcome outcome;
        private Throwable invocationFailure;
        private InterruptedException interruption;

        MaintenanceAttempt shutdownAndAwait(
                PaimonMaintenanceAdapter maintenance, long timeoutNanos) {
            synchronized (this) {
                if (state == StepState.SUCCEEDED || state == StepState.FAILED) {
                    return invocationFailure == null
                            ? MaintenanceAttempt.outcome(outcome, interruption)
                            : MaintenanceAttempt.invocationFailure(invocationFailure);
                }
                if (state == StepState.IN_PROGRESS) {
                    return MaintenanceAttempt.outcome(
                            PaimonWriteCloseModel.MaintenanceOutcome.waiting(), null);
                }
                state = StepState.IN_PROGRESS;
            }

            PaimonWriteCloseModel.MaintenanceOutcome completed = null;
            InterruptedException completedInterruption = null;
            Throwable failedInvocation = null;
            try {
                PaimonMaintenanceAdapter.Outcome adapterOutcome =
                        maintenance.shutdownAndAwait(timeoutNanos, TimeUnit.NANOSECONDS);
                completed = map(adapterOutcome);
                completedInterruption = adapterOutcome.interruption();
            } catch (Throwable failure) {
                // An adapter/protocol exception is not a structured-drain proof. Keep it distinct
                // from a normally returned FAILED_DRAINED outcome so committer close is forbidden.
                failedInvocation = failure;
            }

            synchronized (this) {
                invocationFailure = failedInvocation;
                outcome = completed;
                interruption = completedInterruption;
                if (failedInvocation != null) {
                    state = StepState.FAILED;
                    return MaintenanceAttempt.invocationFailure(failedInvocation);
                }
                switch (completed.status()) {
                    case SUCCESS:
                        state = StepState.SUCCEEDED;
                        break;
                    case FAILED_DRAINED:
                        state = StepState.FAILED;
                        break;
                    case WAITING:
                        state = StepState.WAITING;
                        break;
                    default:
                        throw new IllegalStateException(
                                "Unknown maintenance state " + completed.status());
                }
                return MaintenanceAttempt.outcome(completed, completedInterruption);
            }
        }

        synchronized StepState state() {
            return state;
        }

        private static PaimonWriteCloseModel.MaintenanceOutcome map(
                PaimonMaintenanceAdapter.Outcome outcome) {
            Objects.requireNonNull(outcome, "maintenance outcome");
            switch (outcome.status()) {
                case SUCCESS:
                    return PaimonWriteCloseModel.MaintenanceOutcome.success();
                case FAILED_DRAINED:
                    return PaimonWriteCloseModel.MaintenanceOutcome.failedDrained(
                            outcome.failure());
                case WAITING:
                    return PaimonWriteCloseModel.MaintenanceOutcome.waiting();
                default:
                    throw new IllegalStateException(
                            "Unknown maintenance outcome " + outcome.status());
            }
        }
    }

    private static final class OnceCloseStep {
        private static final class Attempt {
            private final boolean completed;
            private final Throwable failure;

            private Attempt(boolean completed, Throwable failure) {
                this.completed = completed;
                this.failure = failure;
            }

            private boolean completed() {
                return completed;
            }

            private Throwable failure() {
                return failure;
            }
        }

        private final PaimonWriteCloseModel.CloseAction action;
        private StepState state = StepState.NOT_STARTED;
        private Throwable failure;

        private OnceCloseStep(PaimonWriteCloseModel.CloseAction action) {
            this.action = action;
        }

        void start() {
            synchronized (this) {
                if (state != StepState.NOT_STARTED) {
                    return;
                }
                state = StepState.IN_PROGRESS;
            }
            Throwable completedFailure = null;
            try {
                action.close();
            } catch (Throwable closeFailure) {
                completedFailure = closeFailure;
                if (closeFailure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
            synchronized (this) {
                failure = completedFailure;
                state = completedFailure == null ? StepState.SUCCEEDED : StepState.FAILED;
                notifyAll();
            }
        }

        Attempt awaitCompletion(long absoluteDeadlineNanos) throws InterruptedException {
            synchronized (this) {
                while (state == StepState.IN_PROGRESS) {
                    long remaining = remainingNanos(absoluteDeadlineNanos, System.nanoTime());
                    if (remaining == 0L) {
                        return new Attempt(false, null);
                    }
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                }
                return new Attempt(state != StepState.NOT_STARTED, failure);
            }
        }

        Throwable run() {
            start();
            boolean interrupted = false;
            synchronized (this) {
                while (state == StepState.IN_PROGRESS) {
                    try {
                        wait();
                    } catch (InterruptedException waitInterrupted) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                return failure;
            }
        }

        synchronized StepState state() {
            return state;
        }

    }

}
