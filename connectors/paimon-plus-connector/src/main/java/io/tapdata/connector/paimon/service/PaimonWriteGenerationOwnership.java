package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.write.PaimonWriteResourceLifecycle;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unforgeable ownership carrier for one physical-table writer generation.
 *
 * <p>Only the service coordinator constructs and advances ownership phases. Cross-package
 * transfers use one-shot tokens: the carrier never exposes its physical lease directly and never
 * accepts an arbitrary lifecycle object. Phase mutations are guarded by the coordinator admission
 * lock; the lifecycle reference uses CAS because Factory binding happens outside that lock.
 */
public final class PaimonWriteGenerationOwnership {

    public enum Phase {
        ACQUISITION_RESERVED,
        LEASE_ACTIVE,
        CONTEXT_ACTIVE,
        FINALIZATION_CLAIMING,
        STOP_FINALIZING,
        RELEASE_IN_PROGRESS,
        DDL_TRANSFER_PREPARED,
        DDL_DETACH_AUTHORIZED,
        TRANSFER_IN_PROGRESS,
        DDL_ACTION_ACTIVE,
        DDL_RETAIN_IN_PROGRESS,
        RETAINED,
        REGISTRY_TRANSITION_FAILED_RETAINED,
        RELEASED,
        ACQUIRE_FAILED
    }

    private final PhysicalTableWriterLease physicalLease;
    private final AtomicBoolean lifecycleBindingIssued = new AtomicBoolean();
    private final AtomicReference<PaimonWriteResourceLifecycle> resourceLifecycle =
            new AtomicReference<>();
    private volatile Phase phase = Phase.ACQUISITION_RESERVED;
    private Object contextOwner;
    private ContextPublication contextPublication;
    private FinalizationClaim finalizationClaim;
    private DdlDetachPermit ddlDetachPermit;
    private ReleaseOperation releaseOperation;
    private volatile Throwable retentionFailure;
    private volatile RetainedDdlActionLease retainedDdlActionLease;

    PaimonWriteGenerationOwnership(PhysicalTableWriterLease physicalLease) {
        this.physicalLease = Objects.requireNonNull(physicalLease, "physicalLease");
    }

    public String generationId() {
        return physicalLease.generationId();
    }

    public Phase phase() {
        return phase;
    }

    public Throwable retentionFailure() {
        return retentionFailure;
    }

    public Object retainedDdlActionLease() {
        return retainedDdlActionLease;
    }

    LifecycleBinding newLifecycleBinding() {
        requirePhase(Phase.LEASE_ACTIVE);
        if (!lifecycleBindingIssued.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "Lifecycle binding was already issued for this writer generation");
        }
        return new LifecycleBinding(this);
    }

    ContextPublication newContextPublication() {
        requirePhase(Phase.LEASE_ACTIVE);
        if (resourceLifecycle.get() == null) {
            throw new IllegalStateException(
                    "Context cannot be published before the exact resource lifecycle is bound");
        }
        if (contextPublication != null) {
            throw new IllegalStateException(
                    "Context publication was already issued for this writer generation");
        }
        contextPublication = new ContextPublication(this);
        return contextPublication;
    }

    void markLeaseActive() {
        requirePhase(Phase.ACQUISITION_RESERVED);
        phase = Phase.LEASE_ACTIVE;
    }

    void markAcquireFailed() {
        requirePhase(Phase.ACQUISITION_RESERVED);
        phase = Phase.ACQUIRE_FAILED;
    }

    void prepareDdlTransfer(Object expectedContext) {
        requireExactContext(expectedContext);
        phase = Phase.DDL_TRANSFER_PREPARED;
    }

    FinalizationClaim prepareClosedSuccessClaim(
            Phase expectedPhase,
            Object expectedContext,
            PaimonWriteResourceLifecycle.CloseOutcome evidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason) {
        requirePhase(expectedPhase);
        if (expectedContext != null && contextOwner != expectedContext) {
            throw new IllegalStateException(
                    "Context identity does not match the finalization claim");
        }
        validateClosedSuccessEvidence(evidence, expectedReason);
        return reserveFinalizationClaim(
                expectedPhase, expectedContext, evidence, expectedReason, true);
    }

    FinalizationClaim prepareRetainedClaim(
            Phase expectedPhase,
            Object expectedContext,
            PaimonWriteResourceLifecycle.CloseOutcome evidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason) {
        requirePhase(expectedPhase);
        if (expectedContext != null && contextOwner != expectedContext) {
            throw new IllegalStateException(
                    "Context identity does not match the retained finalization claim");
        }
        validateRetainedEvidence(evidence, expectedReason);
        return reserveFinalizationClaim(
                expectedPhase, expectedContext, evidence, expectedReason, false);
    }

    DdlDetachPermit authorizeDdlDetach(
            Object expectedContext, FinalizationClaim claim) {
        consumeFinalizationClaim(
                claim,
                Phase.DDL_TRANSFER_PREPARED,
                expectedContext,
                PaimonWriteCloseModel.InitiatingReason.DDL,
                true);
        ddlDetachPermit = new DdlDetachPermit(this, expectedContext);
        phase = Phase.DDL_DETACH_AUTHORIZED;
        return ddlDetachPermit;
    }

    void confirmContextDetached(
            Object expectedContext,
            DdlDetachPermit permit,
            PaimonServiceResourceCoordinator.ContextDetachReceipt receipt) {
        requireAuthorizedDdlDetach(expectedContext, permit);
        Objects.requireNonNull(receipt, "receipt").consumeFor(this, expectedContext);
        permit.consume();
        contextOwner = null;
        phase = Phase.TRANSFER_IN_PROGRESS;
    }

    void validateDdlDetachPermit(Object expectedContext, DdlDetachPermit permit) {
        requireAuthorizedDdlDetach(expectedContext, permit);
    }

    void completeDdlTransfer() {
        requirePhase(Phase.TRANSFER_IN_PROGRESS);
        phase = Phase.DDL_ACTION_ACTIVE;
    }

    void beginStopFinalization(Object expectedContext, FinalizationClaim claim) {
        requireStopClaimOrigin(claim);
        consumeFinalizationClaim(
                claim,
                claim.originalPhase,
                expectedContext,
                PaimonWriteCloseModel.InitiatingReason.STOP,
                true);
        contextOwner = null;
        phase = Phase.STOP_FINALIZING;
    }

    ReleaseOperation beginRelease(FinalizationClaim claim) {
        Phase expectedPhase = claim.originalPhase;
        PaimonWriteCloseModel.InitiatingReason expectedReason = claim.expectedReason;
        consumeFinalizationClaim(
                claim, expectedPhase, claim.expectedContext, expectedReason, true);
        phase = Phase.RELEASE_IN_PROGRESS;
        releaseOperation = new ReleaseOperation(expectedReason);
        return releaseOperation;
    }

    ReleaseOperation beginReleaseAfterValidatedClose(Phase expectedPhase) {
        if (expectedPhase != Phase.STOP_FINALIZING) {
            throw new IllegalArgumentException(
                    "Only STOP may reuse evidence consumed while detaching its Context");
        }
        requirePhase(expectedPhase);
        phase = Phase.RELEASE_IN_PROGRESS;
        releaseOperation = new ReleaseOperation(PaimonWriteCloseModel.InitiatingReason.STOP);
        return releaseOperation;
    }

    void beginDdlRetention() {
        requirePhase(Phase.DDL_ACTION_ACTIVE);
        phase = Phase.DDL_RETAIN_IN_PROGRESS;
    }

    void retainDdlResourceFailure(FinalizationClaim claim) {
        consumeFinalizationClaim(
                claim,
                Phase.DDL_TRANSFER_PREPARED,
                claim.expectedContext,
                PaimonWriteCloseModel.InitiatingReason.DDL,
                false);
        retainWithContext(claim.evidence.failure());
    }

    void retainDdlDetachFailure(
            Object expectedContext, DdlDetachPermit permit, Throwable failure) {
        requireAuthorizedDdlDetach(expectedContext, permit);
        permit.consume();
        retainWithContext(failure);
    }

    void retainClaimedContextFailure(
            Object expectedContext, FinalizationClaim claim, Throwable failure) {
        requireStopClaimOrigin(claim);
        consumeFinalizationClaim(
                claim,
                claim.originalPhase,
                expectedContext,
                PaimonWriteCloseModel.InitiatingReason.STOP,
                true);
        retainWithContext(failure);
    }

    private static void requireStopClaimOrigin(FinalizationClaim claim) {
        Objects.requireNonNull(claim, "claim");
        if (claim.originalPhase != Phase.CONTEXT_ACTIVE
                && claim.originalPhase != Phase.DDL_TRANSFER_PREPARED) {
            throw new IllegalStateException(
                    "STOP cannot finalize ownership claimed from " + claim.originalPhase);
        }
    }

    void release() {
        requirePhase(Phase.RELEASE_IN_PROGRESS);
        phase = Phase.RELEASED;
    }

    void retain(Throwable failure, RetainedDdlActionLease retainedLease) {
        Objects.requireNonNull(failure, "failure");
        if (phase != Phase.RELEASE_IN_PROGRESS
                && phase != Phase.DDL_RETAIN_IN_PROGRESS
                && phase != Phase.LEASE_ACTIVE
                && phase != Phase.STOP_FINALIZING
                && phase != Phase.DDL_TRANSFER_PREPARED
                && phase != Phase.TRANSFER_IN_PROGRESS
                && phase != Phase.DDL_ACTION_ACTIVE) {
            throw new IllegalStateException(
                    "Writer generation cannot be retained from phase " + phase);
        }
        retentionFailure = failure;
        retainedDdlActionLease = retainedLease;
        contextOwner = null;
        phase = Phase.RETAINED;
    }

    void retainRegistryTransitionFailure(
            Throwable failure, RetainedDdlActionLease retainedLease) {
        Objects.requireNonNull(failure, "failure");
        if (phase != Phase.RELEASE_IN_PROGRESS
                && phase != Phase.DDL_RETAIN_IN_PROGRESS) {
            throw new IllegalStateException(
                    "Registry transition failure cannot be retained from phase " + phase);
        }
        retentionFailure = failure;
        retainedDdlActionLease = retainedLease;
        contextOwner = null;
        phase = Phase.REGISTRY_TRANSITION_FAILED_RETAINED;
    }

    private void retainWithContext(Throwable failure) {
        retentionFailure = Objects.requireNonNull(failure, "failure");
        // The Service expected-remove did not succeed. Keep the exact Context strongly reachable;
        // its failed/closed lifecycle and this carrier jointly fence generation reuse.
        phase = Phase.RETAINED;
    }

    boolean ownsContext(Object expectedContext) {
        return contextOwner == expectedContext;
    }

    PaimonWriteResourceLifecycle resourceLifecycleHandle() {
        return resourceLifecycle.get();
    }

    ReleaseOperation releaseOperation() {
        return releaseOperation;
    }

    boolean ownedBy(String expectedServiceOwnerId) {
        return physicalLease.serviceOwnerId().equals(expectedServiceOwnerId);
    }

    PhysicalTableWriterLease physicalLease() {
        return physicalLease;
    }

    private void completeContextPublication(ContextPublication publication, Object context) {
        requirePhase(Phase.LEASE_ACTIVE);
        if (contextPublication != publication) {
            throw new IllegalStateException(
                    "Context publication token does not belong to this writer generation");
        }
        if (contextOwner != null) {
            throw new IllegalStateException("Writer generation already owns a Context");
        }
        contextOwner = Objects.requireNonNull(context, "context");
        phase = Phase.CONTEXT_ACTIVE;
    }

    private FinalizationClaim reserveFinalizationClaim(
            Phase originalPhase,
            Object expectedContext,
            PaimonWriteResourceLifecycle.CloseOutcome evidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason,
            boolean closedSuccess) {
        if (finalizationClaim != null) {
            throw new IllegalStateException("A finalization claim is already reserved");
        }
        FinalizationClaim claim =
                new FinalizationClaim(
                        this,
                        resourceLifecycle.get(),
                        originalPhase,
                        expectedContext,
                        evidence,
                        expectedReason,
                        closedSuccess);
        finalizationClaim = claim;
        phase = Phase.FINALIZATION_CLAIMING;
        return claim;
    }

    void rollbackFinalizationClaim(FinalizationClaim claim) {
        requirePhase(Phase.FINALIZATION_CLAIMING);
        if (finalizationClaim != claim || !claim.belongsTo(this) || claim.claimed()) {
            throw new IllegalStateException(
                    "Finalization claim rollback does not match an unclaimed reservation");
        }
        phase = claim.originalPhase;
        finalizationClaim = null;
    }

    private void consumeFinalizationClaim(
            FinalizationClaim claim,
            Phase expectedOriginalPhase,
            Object expectedContext,
            PaimonWriteCloseModel.InitiatingReason expectedReason,
            boolean closedSuccess) {
        Objects.requireNonNull(claim, "claim");
        requirePhase(Phase.FINALIZATION_CLAIMING);
        if (finalizationClaim != claim
                || !claim.belongsTo(this)
                || claim.originalPhase != expectedOriginalPhase
                || claim.expectedContext != expectedContext
                || claim.expectedReason != expectedReason
                || claim.closedSuccess != closedSuccess) {
            throw new IllegalStateException(
                    "Finalization claim does not match the exact ownership transition");
        }
        claim.consume();
        finalizationClaim = null;
    }

    private void validateClosedSuccessEvidence(
            PaimonWriteResourceLifecycle.CloseOutcome evidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason) {
        validateResourceEvidence(evidence, expectedReason);
        if (evidence.state() != PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS
                || evidence.failure() != null) {
            throw new IllegalStateException(
                    "Physical ownership release requires resource CLOSED_SUCCESS evidence");
        }
    }

    private void validateRetainedEvidence(
            PaimonWriteResourceLifecycle.CloseOutcome evidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason) {
        validateResourceEvidence(evidence, expectedReason);
        if (!evidence.state().isRetained() || evidence.failure() == null) {
            throw new IllegalStateException(
                    "Retained ownership requires a terminal retained resource outcome");
        }
    }

    private void validateResourceEvidence(
            PaimonWriteResourceLifecycle.CloseOutcome evidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason) {
        Objects.requireNonNull(evidence, "evidence");
        PaimonWriteResourceLifecycle lifecycle = resourceLifecycle.get();
        if (lifecycle == null
                || !evidence.belongsTo(lifecycle)
                || !evidence.matchesGenerationCapability(physicalLease)
                || evidence.effectiveReason() != expectedReason) {
            throw new IllegalArgumentException(
                    "Resource outcome does not belong to the exact writer generation and reason");
        }
    }

    private void requireExactContext(Object expectedContext) {
        Objects.requireNonNull(expectedContext, "expectedContext");
        if (phase != Phase.CONTEXT_ACTIVE && phase != Phase.DDL_TRANSFER_PREPARED) {
            throw new IllegalStateException(
                    "Expected an attached Context phase but was " + phase);
        }
        if (contextOwner != expectedContext) {
            throw new IllegalStateException(
                    "Context identity does not match the active writer generation");
        }
    }

    private void requireAuthorizedDdlDetach(
            Object expectedContext, DdlDetachPermit permit) {
        Objects.requireNonNull(expectedContext, "expectedContext");
        Objects.requireNonNull(permit, "permit");
        requirePhase(Phase.DDL_DETACH_AUTHORIZED);
        if (contextOwner != expectedContext
                || ddlDetachPermit != permit
                || !permit.belongsTo(this, expectedContext)) {
            throw new IllegalStateException(
                    "DDL detach permit does not match the exact Context and generation");
        }
    }

    private void requirePhase(Phase expected) {
        if (phase != expected) {
            throw new IllegalStateException(
                    "Expected writer ownership phase " + expected + " but was " + phase);
        }
    }

    /** One-shot handoff from Service acquisition to the write-package lifecycle constructor. */
    public static final class LifecycleBinding {
        private final PaimonWriteGenerationOwnership ownership;
        private final AtomicBoolean consumed = new AtomicBoolean();

        private LifecycleBinding(PaimonWriteGenerationOwnership ownership) {
            this.ownership = ownership;
        }

        public Object generationCapability() {
            return ownership.physicalLease;
        }

        public void bind(PaimonWriteResourceLifecycle lifecycle) {
            Objects.requireNonNull(lifecycle, "lifecycle");
            if (!lifecycle.matchesGenerationCapability(ownership.physicalLease)) {
                throw new IllegalArgumentException(
                        "Resource lifecycle does not match the writer generation");
            }
            if (!consumed.compareAndSet(false, true)) {
                throw new IllegalStateException("Lifecycle binding token was already consumed");
            }
            if (!ownership.resourceLifecycle.compareAndSet(null, lifecycle)) {
                throw new IllegalStateException(
                        "Resource lifecycle is already bound to the writer generation");
            }
        }
    }

    /** One-shot Context publication capability; constructed only by the service coordinator. */
    public static final class ContextPublication {
        private final PaimonWriteGenerationOwnership ownership;
        private final AtomicBoolean consumed = new AtomicBoolean();

        private ContextPublication(PaimonWriteGenerationOwnership ownership) {
            this.ownership = ownership;
        }

        public PaimonWriteGenerationOwnership ownership() {
            return ownership;
        }

        public void claim(Object context) {
            Objects.requireNonNull(context, "context");
            if (!consumed.compareAndSet(false, true)) {
                throw new IllegalStateException("Context publication token was already consumed");
            }
            ownership.completeContextPublication(this, context);
        }
    }

    /**
     * Barrier-visible reservation whose lifecycle claim is executed without the coordinator lock.
     * The coordinator alone may commit or roll back this exact token.
     */
    static final class FinalizationClaim {
        private final PaimonWriteGenerationOwnership ownership;
        private final PaimonWriteResourceLifecycle lifecycle;
        private final Phase originalPhase;
        private final Object expectedContext;
        private final PaimonWriteResourceLifecycle.CloseOutcome evidence;
        private final PaimonWriteCloseModel.InitiatingReason expectedReason;
        private final boolean closedSuccess;
        private final AtomicBoolean claimAttempted = new AtomicBoolean();
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final AtomicBoolean consumed = new AtomicBoolean();

        private FinalizationClaim(
                PaimonWriteGenerationOwnership ownership,
                PaimonWriteResourceLifecycle lifecycle,
                Phase originalPhase,
                Object expectedContext,
                PaimonWriteResourceLifecycle.CloseOutcome evidence,
                PaimonWriteCloseModel.InitiatingReason expectedReason,
                boolean closedSuccess) {
            this.ownership = Objects.requireNonNull(ownership, "ownership");
            this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
            this.originalPhase = Objects.requireNonNull(originalPhase, "originalPhase");
            this.expectedContext = expectedContext;
            this.evidence = Objects.requireNonNull(evidence, "evidence");
            this.expectedReason = Objects.requireNonNull(expectedReason, "expectedReason");
            this.closedSuccess = closedSuccess;
        }

        void claimOutsideCoordinatorLock() {
            if (!claimAttempted.compareAndSet(false, true)) {
                throw new IllegalStateException("Finalization claim was already attempted");
            }
            if (closedSuccess) {
                lifecycle.claimClosedSuccess(evidence, expectedReason);
            } else {
                lifecycle.claimRetained(evidence, expectedReason);
            }
            claimed.set(true);
        }

        private boolean belongsTo(PaimonWriteGenerationOwnership expectedOwnership) {
            return ownership == expectedOwnership;
        }

        private boolean claimed() {
            return claimed.get();
        }

        private void consume() {
            if (!claimed.get()) {
                throw new IllegalStateException("Finalization authority was not claimed");
            }
            if (!consumed.compareAndSet(false, true)) {
                throw new IllegalStateException("Finalization claim was already consumed");
            }
        }
    }

    /** One-shot authorization produced before Service attempts expected-remove of the Context. */
    static final class DdlDetachPermit {
        private final PaimonWriteGenerationOwnership ownership;
        private final Object expectedContext;
        private final AtomicBoolean consumed = new AtomicBoolean();

        private DdlDetachPermit(
                PaimonWriteGenerationOwnership ownership, Object expectedContext) {
            this.ownership = ownership;
            this.expectedContext = expectedContext;
        }

        private boolean belongsTo(
                PaimonWriteGenerationOwnership expectedOwnership, Object context) {
            return ownership == expectedOwnership && expectedContext == context;
        }

        private void consume() {
            if (!consumed.compareAndSet(false, true)) {
                throw new IllegalStateException("DDL detach permit was already consumed");
            }
        }
    }

    /** One exact registry release; concurrent finalizers join this result outside coordinator locks. */
    static final class ReleaseOperation {
        private final CountDownLatch completion = new CountDownLatch(1);
        private final PaimonWriteCloseModel.InitiatingReason reason;
        private volatile boolean released;
        private volatile Throwable transitionFailure;

        private ReleaseOperation(PaimonWriteCloseModel.InitiatingReason reason) {
            this.reason = Objects.requireNonNull(reason, "reason");
        }

        void requireJoinReason(PaimonWriteCloseModel.InitiatingReason expectedReason) {
            if (reason != expectedReason) {
                throw new IllegalStateException(
                        "Physical release join is forbidden for "
                                + reason
                                + " -> "
                                + expectedReason);
            }
        }

        void complete(boolean released, Throwable transitionFailure) {
            this.released = released;
            this.transitionFailure = transitionFailure;
            completion.countDown();
        }

        boolean awaitResult() throws InterruptedException {
            completion.await();
            if (transitionFailure instanceof RuntimeException) {
                throw (RuntimeException) transitionFailure;
            }
            if (transitionFailure instanceof Error) {
                throw (Error) transitionFailure;
            }
            return released;
        }
    }
}
