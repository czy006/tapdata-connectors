package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.PaimonWriteResourceLifecycle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Linearizes service read admission, per-table DDL read fences, and operation joins.
 *
 * <p>The admission lock protects only coordinator state. It is never held while waiting, closing a
 * resource, invoking Paimon, or calling user code. A caller publishes a service/table fence and
 * obtains an immutable operation snapshot under the short critical section, then requests stop and
 * joins that snapshot after the lock has been released.
 *
 * <p>Canonical lock order is: service ingress, per-table commit lock, and then a short coordinator
 * state check. A caller must release every per-table lock before joining an operation. This class
 * never acquires a per-table, operation, physical-lease, Catalog, or FileIO lock.
 */
final class PaimonServiceResourceCoordinator {

    enum Completion {
        CLOSED_SUCCESS,
        RETAINED
    }

    private final Object admissionLock = new Object();
    private final Map<ReadIdentity, ReadAdmission> activeReads = new LinkedHashMap<>();
    private final Map<ReadIdentity, ReadAdmission> retainedReads = new LinkedHashMap<>();
    private final Map<String, LinkedHashMap<Long, ReadBorrow>> activeBorrows =
            new LinkedHashMap<>();
    private final Map<Long, ReadBorrow> retainedBorrows = new LinkedHashMap<>();
    private final Map<String, TableReadFence> tableReadFences = new LinkedHashMap<>();
    private final Map<String, PaimonWriteGenerationOwnership> activeWriteGenerations =
            new LinkedHashMap<>();
    private final Map<String, PaimonWriteGenerationOwnership> retainedWriteGenerations =
            new LinkedHashMap<>();
    private final Map<String, PaimonWriteGenerationOwnership> ownershipTransfers =
            new LinkedHashMap<>();
    private final Map<String, PaimonWriteGenerationOwnership> activeDdlActions =
            new LinkedHashMap<>();
    private final Map<String, PaimonTableWriteContext> writeContexts;
    private final String serviceOwnerId;
    private final PhysicalTableWriterLease.Registry physicalLeaseRegistry;
    private final Runnable acquisitionReservedObserver;
    private long nextRegistrationId;
    private long nextBorrowId;
    private long nextFenceId;
    private ServiceReadFence serviceReadFence;

    PaimonServiceResourceCoordinator() {
        this(
                UUID.randomUUID().toString(),
                PhysicalTableWriterLease.jvmRegistry(),
                new LinkedHashMap<>(),
                () -> {});
    }

    PaimonServiceResourceCoordinator(
            String serviceOwnerId, PhysicalTableWriterLease.Registry physicalLeaseRegistry) {
        this(serviceOwnerId, physicalLeaseRegistry, new LinkedHashMap<>(), () -> {});
    }

    PaimonServiceResourceCoordinator(
            String serviceOwnerId,
            PhysicalTableWriterLease.Registry physicalLeaseRegistry,
            Runnable acquisitionReservedObserver) {
        this(
                serviceOwnerId,
                physicalLeaseRegistry,
                new LinkedHashMap<>(),
                acquisitionReservedObserver);
    }

    PaimonServiceResourceCoordinator(
            String serviceOwnerId,
            PhysicalTableWriterLease.Registry physicalLeaseRegistry,
            Map<String, PaimonTableWriteContext> writeContexts,
            Runnable acquisitionReservedObserver) {
        requireNonBlank(serviceOwnerId, "Service owner id");
        this.serviceOwnerId = serviceOwnerId;
        this.physicalLeaseRegistry =
                Objects.requireNonNull(physicalLeaseRegistry, "physicalLeaseRegistry");
        this.writeContexts = Objects.requireNonNull(writeContexts, "writeContexts");
        this.acquisitionReservedObserver =
                Objects.requireNonNull(acquisitionReservedObserver, "acquisitionReservedObserver");
    }

    /**
     * Reserves coordinator ownership before entering the JVM registry, then acquires the exact
     * physical slot without nesting the two locks. The reservation itself blocks the global-close
     * barrier while the registry operation is in flight.
     */
    PaimonWriteGenerationOwnership acquireWriterGeneration(
            String physicalTableHash, String logicalTableKey) {
        requireNonBlank(physicalTableHash, "Physical table hash");
        requireNonBlank(logicalTableKey, "Logical table key");
        PhysicalTableWriterLease lease =
                PhysicalTableWriterLease.newGeneration(
                        physicalTableHash,
                        logicalTableKey,
                        serviceOwnerId,
                        PhysicalTableWriterLease.Purpose.WRITER);
        PaimonWriteGenerationOwnership ownership =
                new PaimonWriteGenerationOwnership(lease);
        synchronized (admissionLock) {
            if (serviceReadFence != null) {
                throw new AdmissionRejectedException(
                        "Paimon service write admission is fenced");
            }
            if (activeWriteGenerations.containsKey(logicalTableKey)
                    || hasRetainedWriteGenerationLocked(logicalTableKey)) {
                throw new AdmissionRejectedException(
                        "Paimon writer generation already exists for " + logicalTableKey);
            }
            activeWriteGenerations.put(logicalTableKey, ownership);
        }

        try {
            acquisitionReservedObserver.run();
        } catch (RuntimeException | Error observerFailure) {
            synchronized (admissionLock) {
                removeActiveExpectedLocked(ownership);
                ownership.markAcquireFailed();
            }
            throw observerFailure;
        }
        boolean acquired = physicalLeaseRegistry.tryAcquire(lease);
        synchronized (admissionLock) {
            if (!acquired) {
                removeActiveExpectedLocked(ownership);
                ownership.markAcquireFailed();
                throw new AdmissionRejectedException(
                        "Physical Paimon table already has an active writer");
            }
            ownership.markLeaseActive();
            return ownership;
        }
    }

    PaimonWriteGenerationOwnership.LifecycleBinding prepareLifecycleBinding(
            PaimonWriteGenerationOwnership expectedOwnership) {
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            return expectedOwnership.newLifecycleBinding();
        }
    }

    void publishContext(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonTableWriteContext context) {
        Objects.requireNonNull(context, "context");
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            if (expectedOwnership.phase()
                    != PaimonWriteGenerationOwnership.Phase.LEASE_ACTIVE) {
                throw new IllegalStateException(
                        "Writer generation is not awaiting Context publication");
            }
            if (!expectedOwnership.physicalLease().logicalTableKey().equals(context.tableKey())) {
                throw new IllegalArgumentException(
                        "Context table key does not match the writer generation");
            }
            if (writeContexts.containsKey(context.tableKey())) {
                throw new IllegalStateException(
                        "A Context is already published for " + context.tableKey());
            }
            context.attachGenerationOwnership(expectedOwnership.newContextPublication());
            writeContexts.put(context.tableKey(), context);
        }
    }

    void beginStopFinalization(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonTableWriteContext expectedContext,
            PaimonWriteResourceLifecycle.CloseOutcome resourceClosedEvidence) {
        PaimonWriteGenerationOwnership.FinalizationClaim claim;
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            requireExactContextLocked(expectedOwnership, expectedContext);
            PaimonWriteGenerationOwnership.Phase claimOrigin =
                    expectedOwnership.phase();
            if (claimOrigin != PaimonWriteGenerationOwnership.Phase.CONTEXT_ACTIVE
                    && claimOrigin
                            != PaimonWriteGenerationOwnership.Phase.DDL_TRANSFER_PREPARED) {
                throw new IllegalStateException(
                        "STOP cannot claim writer generation from " + claimOrigin);
            }
            claim =
                    expectedOwnership.prepareClosedSuccessClaim(
                            claimOrigin,
                            expectedContext,
                            resourceClosedEvidence,
                            PaimonWriteCloseModel.InitiatingReason.STOP);
        }
        claimFinalizationOutsideAdmissionLock(expectedOwnership, claim);
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            if (!removeExactContextLocked(expectedOwnership, expectedContext)) {
                IllegalStateException failure =
                        new IllegalStateException(
                                "STOP exact Context compare-remove was rejected");
                expectedOwnership.retainClaimedContextFailure(
                        expectedContext, claim, failure);
                retainClaimedContextFailureLocked(expectedOwnership);
                throw failure;
            }
            expectedOwnership.beginStopFinalization(expectedContext, claim);
            ownershipTransfers.remove(ownershipKey(expectedOwnership));
        }
    }

    boolean completeStopFinalization(PaimonWriteGenerationOwnership expectedOwnership)
            throws InterruptedException {
        return completeExactReleaseAfterValidatedClose(
                expectedOwnership,
                PaimonWriteGenerationOwnership.Phase.STOP_FINALIZING,
                "STOP exact physical writer lease release was rejected");
    }

    boolean releaseUnpublishedGeneration(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonWriteResourceLifecycle.CloseOutcome safeRollbackEvidence)
            throws InterruptedException {
        return completeExactRelease(
                expectedOwnership,
                PaimonWriteGenerationOwnership.Phase.LEASE_ACTIVE,
                safeRollbackEvidence,
                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                "Factory exact physical writer lease release was rejected");
    }

    void prepareDdlTransfer(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonTableWriteContext expectedContext) {
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            expectedOwnership.prepareDdlTransfer(expectedContext);
        }
    }

    /** Claims effective DDL finalizer authority before exact Context detach. */
    PaimonWriteGenerationOwnership.DdlDetachPermit authorizeDdlDetach(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonTableWriteContext expectedContext,
            PaimonWriteResourceLifecycle.CloseOutcome resourceClosedEvidence) {
        PaimonWriteGenerationOwnership.FinalizationClaim claim;
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            requireExactContextLocked(expectedOwnership, expectedContext);
            claim =
                    expectedOwnership.prepareClosedSuccessClaim(
                            PaimonWriteGenerationOwnership.Phase.DDL_TRANSFER_PREPARED,
                            expectedContext,
                            resourceClosedEvidence,
                            PaimonWriteCloseModel.InitiatingReason.DDL);
        }
        claimFinalizationOutsideAdmissionLock(expectedOwnership, claim);
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            return expectedOwnership.authorizeDdlDetach(expectedContext, claim);
        }
    }

    /** Atomically expected-removes the coordinator-owned Context and confirms the same lease transfer. */
    boolean detachExactContextForDdl(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonTableWriteContext expectedContext,
            PaimonWriteGenerationOwnership.DdlDetachPermit detachPermit) {
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            expectedOwnership.validateDdlDetachPermit(expectedContext, detachPermit);
            if (!removeExactContextLocked(expectedOwnership, expectedContext)) {
                IllegalStateException failure =
                        new IllegalStateException(
                                "DDL exact Context compare-remove was rejected");
                expectedOwnership.retainDdlDetachFailure(
                        expectedContext, detachPermit, failure);
                retainClaimedContextFailureLocked(expectedOwnership);
                return false;
            }
            ContextDetachReceipt receipt =
                    new ContextDetachReceipt(expectedOwnership, expectedContext);
            expectedOwnership.confirmContextDetached(
                    expectedContext, detachPermit, receipt);
            String key = ownershipKey(expectedOwnership);
            if (ownershipTransfers.put(key, expectedOwnership) != null) {
                throw new IllegalStateException("DDL ownership transfer is already registered");
            }
            return true;
        }
    }

    void completeDdlTransfer(PaimonWriteGenerationOwnership expectedOwnership) {
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            String key = ownershipKey(expectedOwnership);
            if (ownershipTransfers.get(key) != expectedOwnership) {
                throw new IllegalStateException(
                        "DDL transfer identity does not match the active writer generation");
            }
            expectedOwnership.completeDdlTransfer();
            activeDdlActions.put(key, expectedOwnership);
            ownershipTransfers.remove(key);
        }
    }

    void retainDdlResourceFailure(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonWriteResourceLifecycle.CloseOutcome retainedEvidence) {
        PaimonWriteGenerationOwnership.FinalizationClaim claim;
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            PaimonTableWriteContext expectedContext = exactContextLocked(expectedOwnership);
            claim =
                    expectedOwnership.prepareRetainedClaim(
                            PaimonWriteGenerationOwnership.Phase.DDL_TRANSFER_PREPARED,
                            expectedContext,
                            retainedEvidence,
                            PaimonWriteCloseModel.InitiatingReason.DDL);
        }
        claimFinalizationOutsideAdmissionLock(expectedOwnership, claim);
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            String key = ownershipKey(expectedOwnership);
            expectedOwnership.retainDdlResourceFailure(claim);
            retainedWriteGenerations.put(key, expectedOwnership);
            ownershipTransfers.remove(key);
            removeActiveExpectedLocked(expectedOwnership);
        }
    }

    /**
     * Retains a completed DDL action scope. Task 31 owns the success path and will expose release
     * only from the action executor after both action and unconditional cleanup have succeeded.
     */
    void retainDdlActionFailure(
            PaimonWriteGenerationOwnership expectedOwnership,
            Throwable actionOrCleanupFailure) {
        Objects.requireNonNull(actionOrCleanupFailure, "actionOrCleanupFailure");
        String key = ownershipKey(expectedOwnership);
        synchronized (admissionLock) {
            requireActiveDdlOwnedLocked(expectedOwnership, key);
            expectedOwnership.beginDdlRetention();
        }

        RetainedDdlActionLease retainedLease;
        try {
            retainedLease =
                    physicalLeaseRegistry.retainAfterDdlFailure(
                            expectedOwnership.physicalLease(), actionOrCleanupFailure);
        } catch (Throwable registryFailure) {
            Throwable transitionFailure = transitionFailure(registryFailure);
            RetainedDdlActionLease transitionedRetainedLease =
                    retainedLease(registryFailure);
            synchronized (admissionLock) {
                requireActiveOwnedLocked(expectedOwnership);
                retainRegistryTransitionFailureLocked(
                        expectedOwnership,
                        actionOrCleanupFailure,
                        transitionedRetainedLease);
            }
            // Publish the conservative retained state before enriching diagnostics. Even an
            // unexpected failure from Throwable.addSuppressed must not leave the carrier in the
            // DDL_RETAIN_IN_PROGRESS intermediate phase.
            if (transitionFailure != actionOrCleanupFailure) {
                actionOrCleanupFailure.addSuppressed(transitionFailure);
            }
            rethrowUnchecked(transitionFailure);
            throw new IllegalStateException(
                    "Physical lease registry transition failed", transitionFailure);
        }
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            retainLocked(expectedOwnership, actionOrCleanupFailure, retainedLease);
        }
    }

    ReadAdmission admitRead(
            String readOperationId, String serviceOwnerId, Collection<String> requestedTableKeys) {
        requireNonBlank(readOperationId, "Read operation id");
        requireNonBlank(serviceOwnerId, "Service owner id");
        List<String> tableKeys = normalizeTableKeys(requestedTableKeys);
        ReadIdentity identity = new ReadIdentity(readOperationId, serviceOwnerId);

        synchronized (admissionLock) {
            if (serviceReadFence != null) {
                throw new AdmissionRejectedException(
                        "Paimon service read admission is fenced for " + identity);
            }
            if (activeReads.containsKey(identity) || retainedReads.containsKey(identity)) {
                throw new AdmissionRejectedException(
                        "Paimon read operation identity is already registered: " + identity);
            }
            for (String tableKey : tableKeys) {
                if (tableReadFences.containsKey(tableKey)) {
                    throw new AdmissionRejectedException(
                            "Paimon table read admission is fenced for " + tableKey);
                }
            }

            long registrationId = ++nextRegistrationId;
            ReadAdmission admission =
                    new ReadAdmission(this, registrationId, identity, tableKeys);
            activeReads.put(identity, admission);
            for (ReadBorrow borrow : admission.borrows.values()) {
                activeBorrows
                        .computeIfAbsent(borrow.tableKey, ignored -> new LinkedHashMap<>())
                        .put(registrationId, borrow);
            }
            return admission;
        }
    }

    ServiceReadFence beginServiceReadFence() {
        synchronized (admissionLock) {
            if (serviceReadFence == null) {
                serviceReadFence =
                        new ServiceReadFence(
                                this,
                                ++nextFenceId,
                                new ArrayList<>(activeReads.values()));
            }
            return serviceReadFence;
        }
    }

    TableReadFence beginTableDdlFence(String tableKey) {
        requireNonBlank(tableKey, "Table key");
        synchronized (admissionLock) {
            TableReadFence existing = tableReadFences.get(tableKey);
            if (existing != null) {
                return existing;
            }
            if (serviceReadFence != null) {
                throw new AdmissionRejectedException(
                        "Paimon service read admission is already fenced");
            }
            Map<Long, ReadBorrow> borrowers = activeBorrows.get(tableKey);
            List<ReadBorrow> snapshot =
                    borrowers == null
                            ? Collections.emptyList()
                            : new ArrayList<>(borrowers.values());
            TableReadFence fence =
                    new TableReadFence(this, ++nextFenceId, tableKey, snapshot);
            tableReadFences.put(tableKey, fence);
            return fence;
        }
    }

    void releaseTableDdlFence(TableReadFence expectedFence) {
        if (expectedFence == null) {
            throw new IllegalArgumentException("Expected table read fence must not be null");
        }
        if (expectedFence.coordinator != this) {
            throw new IllegalArgumentException("Table read fence belongs to another coordinator");
        }
        synchronized (admissionLock) {
            TableReadFence actual = tableReadFences.get(expectedFence.tableKey);
            if (actual != expectedFence) {
                throw new IllegalStateException(
                        "Table read fence identity no longer matches " + expectedFence.tableKey);
            }
            if (activeBorrowCountLocked(expectedFence.tableKey) != 0) {
                throw new IllegalStateException(
                        "Active read borrowers still exist for " + expectedFence.tableKey);
            }
            if (hasRetainedBorrowLocked(expectedFence.tableKey)
                    || !allClosedSuccessfully(expectedFence.operations())) {
                throw new IllegalStateException(
                        "Retained read borrowers prevent releasing the fence for "
                                + expectedFence.tableKey);
            }
            tableReadFences.remove(expectedFence.tableKey);
        }
    }

    int activeReadCount() {
        synchronized (admissionLock) {
            return activeReads.size();
        }
    }

    int activeBorrowCount(String tableKey) {
        requireNonBlank(tableKey, "Table key");
        synchronized (admissionLock) {
            return activeBorrowCountLocked(tableKey);
        }
    }

    int retainedReadCount() {
        synchronized (admissionLock) {
            return retainedReads.size();
        }
    }

    int retainedBorrowCount() {
        synchronized (admissionLock) {
            return retainedBorrows.size();
        }
    }

    int activeWriteGenerationCount() {
        synchronized (admissionLock) {
            return activeWriteGenerations.size();
        }
    }

    int retainedWriteGenerationCount() {
        synchronized (admissionLock) {
            return retainedWriteGenerations.size();
        }
    }

    int ownershipTransferCount() {
        synchronized (admissionLock) {
            return ownershipTransfers.size();
        }
    }

    int activeDdlActionCount() {
        synchronized (admissionLock) {
            return activeDdlActions.size();
        }
    }

    boolean canCloseGlobalResources() {
        boolean coordinatorClear;
        synchronized (admissionLock) {
            coordinatorClear =
                    serviceReadFence != null
                            && activeReads.isEmpty()
                            && activeBorrows.isEmpty()
                            && retainedReads.isEmpty()
                            && retainedBorrows.isEmpty()
                            && tableReadFences.isEmpty()
                            && activeWriteGenerations.isEmpty()
                            && retainedWriteGenerations.isEmpty()
                            && ownershipTransfers.isEmpty()
                            && activeDdlActions.isEmpty()
                            && writeContexts.isEmpty();
        }
        return coordinatorClear
                && physicalLeaseRegistry.snapshotOwnedBy(serviceOwnerId).totalCount() == 0;
    }

    private boolean completeExactRelease(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonWriteGenerationOwnership.Phase expectedPhase,
            PaimonWriteResourceLifecycle.CloseOutcome resourceClosedEvidence,
            PaimonWriteCloseModel.InitiatingReason expectedReason,
            String rejectedMessage)
            throws InterruptedException {
        Objects.requireNonNull(expectedOwnership, "expectedOwnership");
        PaimonWriteGenerationOwnership.FinalizationClaim claim;
        synchronized (admissionLock) {
            requireOwnedByThisService(expectedOwnership);
            if (expectedOwnership.releaseOperation() != null) {
                // FACTORY_ROLLBACK is deliberately single-owner and never joinable. Future callers
                // added to this generic path must define an explicit scenario join matrix first.
                throw new IllegalStateException(
                        "Physical release is already owned by another finalizer");
            }
            requireActiveOwnedLocked(expectedOwnership);
            claim =
                    expectedOwnership.prepareClosedSuccessClaim(
                            expectedPhase,
                            null,
                            resourceClosedEvidence,
                            expectedReason);
        }
        claimFinalizationOutsideAdmissionLock(expectedOwnership, claim);
        PaimonWriteGenerationOwnership.ReleaseOperation operation;
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            operation = expectedOwnership.beginRelease(claim);
        }
        return finishExactRelease(expectedOwnership, operation, rejectedMessage);
    }

    private boolean completeExactReleaseAfterValidatedClose(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonWriteGenerationOwnership.Phase expectedPhase,
            String rejectedMessage)
            throws InterruptedException {
        Objects.requireNonNull(expectedOwnership, "expectedOwnership");
        PaimonWriteGenerationOwnership.ReleaseOperation operation;
        boolean owner;
        synchronized (admissionLock) {
            requireOwnedByThisService(expectedOwnership);
            operation = expectedOwnership.releaseOperation();
            owner = operation == null;
            if (owner) {
                requireActiveOwnedLocked(expectedOwnership);
                operation = expectedOwnership.beginReleaseAfterValidatedClose(expectedPhase);
            } else {
                operation.requireJoinReason(PaimonWriteCloseModel.InitiatingReason.STOP);
            }
        }
        return owner
                ? finishExactRelease(expectedOwnership, operation, rejectedMessage)
                : operation.awaitResult();
    }

    private boolean finishExactRelease(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonWriteGenerationOwnership.ReleaseOperation operation,
            String rejectedMessage) {
        final boolean released;
        try {
            released =
                    physicalLeaseRegistry.releaseActive(expectedOwnership.physicalLease());
        } catch (Throwable registryFailure) {
            Throwable transitionFailure = transitionFailure(registryFailure);
            synchronized (admissionLock) {
                requireActiveOwnedLocked(expectedOwnership);
                retainRegistryTransitionFailureLocked(
                        expectedOwnership,
                        transitionFailure,
                        retainedLease(registryFailure));
            }
            operation.complete(false, transitionFailure);
            rethrowUnchecked(transitionFailure);
            throw new IllegalStateException(
                    "Physical lease registry transition failed", transitionFailure);
        }
        synchronized (admissionLock) {
            requireActiveOwnedLocked(expectedOwnership);
            if (released) {
                removeActiveExpectedLocked(expectedOwnership);
                expectedOwnership.release();
                operation.complete(true, null);
                return true;
            }
            IllegalStateException rejection = new IllegalStateException(rejectedMessage);
            retainLocked(
                    expectedOwnership,
                    rejection,
                    null);
            // A false expected-value transition is a normal compare rejection, not a registry
            // execution failure. The sticky cause lives on the retained carrier; every STOP
            // joiner observes the same boolean false contract as the release owner.
            operation.complete(false, null);
            return false;
        }
    }

    private void retainLocked(
            PaimonWriteGenerationOwnership expectedOwnership,
            Throwable failure,
            RetainedDdlActionLease retainedLease) {
        String key = ownershipKey(expectedOwnership);
        expectedOwnership.retain(failure, retainedLease);
        retainedWriteGenerations.put(key, expectedOwnership);
        ownershipTransfers.remove(key);
        activeDdlActions.remove(key);
        removeActiveExpectedLocked(expectedOwnership);
    }

    private void retainRegistryTransitionFailureLocked(
            PaimonWriteGenerationOwnership expectedOwnership,
            Throwable failure,
            RetainedDdlActionLease retainedLease) {
        String key = ownershipKey(expectedOwnership);
        expectedOwnership.retainRegistryTransitionFailure(failure, retainedLease);
        retainedWriteGenerations.put(key, expectedOwnership);
        ownershipTransfers.remove(key);
        activeDdlActions.remove(key);
        removeActiveExpectedLocked(expectedOwnership);
    }

    private void claimFinalizationOutsideAdmissionLock(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonWriteGenerationOwnership.FinalizationClaim claim) {
        try {
            claim.claimOutsideCoordinatorLock();
        } catch (RuntimeException | Error claimFailure) {
            synchronized (admissionLock) {
                requireActiveOwnedLocked(expectedOwnership);
                expectedOwnership.rollbackFinalizationClaim(claim);
            }
            throw claimFailure;
        }
    }

    private PaimonTableWriteContext exactContextLocked(
            PaimonWriteGenerationOwnership expectedOwnership) {
        String tableKey = expectedOwnership.physicalLease().logicalTableKey();
        PaimonTableWriteContext context = writeContexts.get(tableKey);
        if (context == null || !expectedOwnership.ownsContext(context)) {
            throw new IllegalStateException(
                    "Coordinator Context does not match the exact active writer generation");
        }
        return context;
    }

    private void requireExactContextLocked(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonTableWriteContext expectedContext) {
        if (exactContextLocked(expectedOwnership) != expectedContext) {
            throw new IllegalStateException(
                    "Context identity does not match the coordinator-owned writer generation");
        }
    }

    private boolean removeExactContextLocked(
            PaimonWriteGenerationOwnership expectedOwnership,
            PaimonTableWriteContext expectedContext) {
        String tableKey = expectedOwnership.physicalLease().logicalTableKey();
        return expectedOwnership.ownsContext(expectedContext)
                && writeContexts.remove(tableKey, expectedContext);
    }

    private void retainClaimedContextFailureLocked(
            PaimonWriteGenerationOwnership expectedOwnership) {
        String key = ownershipKey(expectedOwnership);
        retainedWriteGenerations.put(key, expectedOwnership);
        ownershipTransfers.remove(key);
        activeDdlActions.remove(key);
        removeActiveExpectedLocked(expectedOwnership);
    }

    private void requireActiveOwnedLocked(
            PaimonWriteGenerationOwnership expectedOwnership) {
        requireOwnedByThisService(expectedOwnership);
        if (activeWriteGenerations.get(
                        expectedOwnership.physicalLease().logicalTableKey())
                != expectedOwnership) {
            throw new IllegalStateException(
                    "Writer generation is not the exact active ownership carrier");
        }
    }

    private void requireOwnedByThisService(
            PaimonWriteGenerationOwnership expectedOwnership) {
        Objects.requireNonNull(expectedOwnership, "expectedOwnership");
        if (!expectedOwnership.ownedBy(serviceOwnerId)) {
            throw new IllegalArgumentException(
                    "Writer generation belongs to another Paimon service");
        }
    }

    private void requireActiveDdlOwnedLocked(
            PaimonWriteGenerationOwnership expectedOwnership, String key) {
        requireActiveOwnedLocked(expectedOwnership);
        if (activeDdlActions.get(key) != expectedOwnership) {
            throw new IllegalStateException(
                    "DDL action scope does not match the writer generation");
        }
    }

    private void removeActiveExpectedLocked(
            PaimonWriteGenerationOwnership expectedOwnership) {
        String logicalTableKey = expectedOwnership.physicalLease().logicalTableKey();
        if (activeWriteGenerations.get(logicalTableKey) != expectedOwnership) {
            throw new IllegalStateException(
                    "Cannot remove a writer generation that is no longer the exact active carrier");
        }
        activeWriteGenerations.remove(logicalTableKey);
    }

    private boolean hasRetainedWriteGenerationLocked(String logicalTableKey) {
        for (PaimonWriteGenerationOwnership ownership : retainedWriteGenerations.values()) {
            if (ownership.physicalLease().logicalTableKey().equals(logicalTableKey)) {
                return true;
            }
        }
        return false;
    }

    private static String ownershipKey(PaimonWriteGenerationOwnership ownership) {
        return ownership.physicalLease().ownerToken();
    }

    /** Unforgeable proof minted only after this coordinator exact-removes its Context map entry. */
    static final class ContextDetachReceipt {
        private final PaimonWriteGenerationOwnership ownership;
        private final PaimonTableWriteContext context;
        private final AtomicBoolean consumed = new AtomicBoolean();

        private ContextDetachReceipt(
                PaimonWriteGenerationOwnership ownership,
                PaimonTableWriteContext context) {
            this.ownership = Objects.requireNonNull(ownership, "ownership");
            this.context = Objects.requireNonNull(context, "context");
        }

        void consumeFor(
                PaimonWriteGenerationOwnership expectedOwnership,
                Object expectedContext) {
            if (ownership != expectedOwnership || context != expectedContext) {
                throw new IllegalStateException(
                        "Context detach receipt does not match the exact generation");
            }
            if (!consumed.compareAndSet(false, true)) {
                throw new IllegalStateException("Context detach receipt was already consumed");
            }
        }
    }

    private static void rethrowIfFatal(Throwable failure) {
        if (failure instanceof Error) {
            throw (Error) failure;
        }
    }

    private static void rethrowUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        rethrowIfFatal(failure);
    }

    private static Throwable transitionFailure(Throwable registryFailure) {
        if (registryFailure
                instanceof PhysicalTableWriterLease.Registry.RegistryTransitionException) {
            return ((PhysicalTableWriterLease.Registry.RegistryTransitionException) registryFailure)
                    .transitionFailure();
        }
        return registryFailure;
    }

    private static RetainedDdlActionLease retainedLease(Throwable registryFailure) {
        if (registryFailure
                instanceof PhysicalTableWriterLease.Registry.RegistryTransitionException) {
            return ((PhysicalTableWriterLease.Registry.RegistryTransitionException) registryFailure)
                    .retainedLease();
        }
        return null;
    }

    private boolean completeBorrow(ReadBorrow expectedBorrow, Completion completion) {
        requireCompletion(completion);
        synchronized (admissionLock) {
            if (expectedBorrow.coordinator != this) {
                throw new IllegalArgumentException("Read borrower belongs to another coordinator");
            }
            if (expectedBorrow.completion() != null) {
                if (expectedBorrow.completion() != completion) {
                    throw new IllegalStateException(
                            "Read borrower already completed as " + expectedBorrow.completion());
                }
                return false;
            }
            ReadAdmission parent = activeReads.get(expectedBorrow.parent.identity);
            if (parent != expectedBorrow.parent) {
                throw new IllegalStateException("Read borrower parent is no longer active");
            }
            Map<Long, ReadBorrow> tableBorrowers = activeBorrows.get(expectedBorrow.tableKey);
            if (tableBorrowers == null
                    || tableBorrowers.get(expectedBorrow.parent.registrationId)
                            != expectedBorrow) {
                throw new IllegalStateException(
                        "Read borrower identity no longer matches " + expectedBorrow.tableKey);
            }

            tableBorrowers.remove(expectedBorrow.parent.registrationId);
            if (tableBorrowers.isEmpty()) {
                activeBorrows.remove(expectedBorrow.tableKey);
            }
            expectedBorrow.publishCompletion(completion);
            if (completion == Completion.RETAINED) {
                retainedBorrows.put(expectedBorrow.borrowId, expectedBorrow);
            }
        }
        expectedBorrow.signalCompletion();
        return true;
    }

    private boolean completeRead(ReadAdmission expectedAdmission, Completion completion) {
        requireCompletion(completion);
        synchronized (admissionLock) {
            if (expectedAdmission.coordinator != this) {
                throw new IllegalArgumentException("Read admission belongs to another coordinator");
            }
            if (expectedAdmission.completion() != null) {
                if (expectedAdmission.completion() != completion) {
                    throw new IllegalStateException(
                            "Read admission already completed as "
                                    + expectedAdmission.completion());
                }
                return false;
            }
            if (activeReads.get(expectedAdmission.identity) != expectedAdmission) {
                throw new IllegalStateException("Read admission identity is no longer active");
            }
            boolean hasRetainedChild = false;
            for (ReadBorrow borrow : expectedAdmission.borrows.values()) {
                if (borrow.completion() == null) {
                    throw new IllegalStateException(
                            "Read parent cannot complete before child " + borrow.tableKey);
                }
                hasRetainedChild |= borrow.completion() == Completion.RETAINED;
            }
            if (completion == Completion.CLOSED_SUCCESS && hasRetainedChild) {
                throw new IllegalStateException(
                        "Read parent cannot publish success with a retained child");
            }

            activeReads.remove(expectedAdmission.identity);
            expectedAdmission.publishCompletion(completion);
            if (completion == Completion.RETAINED) {
                retainedReads.put(expectedAdmission.identity, expectedAdmission);
            }
        }
        expectedAdmission.signalCompletion();
        return true;
    }

    private void requireOutsideAdmissionLock(String operation) {
        if (Thread.holdsLock(admissionLock)) {
            throw new IllegalStateException(
                    operation + " must execute after releasing the coordinator admission lock");
        }
    }

    private int activeBorrowCountLocked(String tableKey) {
        Map<Long, ReadBorrow> borrowers = activeBorrows.get(tableKey);
        return borrowers == null ? 0 : borrowers.size();
    }

    private boolean hasRetainedBorrowLocked(String tableKey) {
        for (ReadBorrow borrow : retainedBorrows.values()) {
            if (borrow.tableKey.equals(tableKey)) {
                return true;
            }
        }
        return false;
    }

    private static boolean allClosedSuccessfully(List<? extends JoinableOperation> operations) {
        for (JoinableOperation operation : operations) {
            if (operation.completion() != Completion.CLOSED_SUCCESS) {
                return false;
            }
        }
        return true;
    }

    private static List<String> normalizeTableKeys(Collection<String> requestedTableKeys) {
        if (requestedTableKeys == null || requestedTableKeys.isEmpty()) {
            throw new IllegalArgumentException("Requested table keys must not be empty");
        }
        Set<String> normalized = new TreeSet<>();
        for (String tableKey : requestedTableKeys) {
            requireNonBlank(tableKey, "Table key");
            normalized.add(tableKey);
        }
        return Collections.unmodifiableList(new ArrayList<>(normalized));
    }

    private static void requireCompletion(Completion completion) {
        if (completion == null) {
            throw new IllegalArgumentException("Read completion must not be null");
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    static final class ReadAdmission extends JoinableOperation {
        private final PaimonServiceResourceCoordinator coordinator;
        private final long registrationId;
        private final ReadIdentity identity;
        private final List<String> tableKeys;
        private final Map<String, ReadBorrow> borrows;

        private ReadAdmission(
                PaimonServiceResourceCoordinator coordinator,
                long registrationId,
                ReadIdentity identity,
                List<String> tableKeys) {
            super(coordinator);
            this.coordinator = coordinator;
            this.registrationId = registrationId;
            this.identity = identity;
            this.tableKeys = tableKeys;
            Map<String, ReadBorrow> mutableBorrows = new LinkedHashMap<>();
            for (String tableKey : tableKeys) {
                mutableBorrows.put(
                        tableKey,
                        new ReadBorrow(
                                coordinator, this, ++coordinator.nextBorrowId, tableKey));
            }
            this.borrows = Collections.unmodifiableMap(mutableBorrows);
        }

        List<String> tableKeys() {
            return tableKeys;
        }

        ReadBorrow borrow(String tableKey) {
            requireNonBlank(tableKey, "Table key");
            ReadBorrow borrow = borrows.get(tableKey);
            if (borrow == null) {
                throw new IllegalArgumentException(
                        "Read admission does not borrow table " + tableKey);
            }
            return borrow;
        }

        @Override
        void requestStop() {
            coordinator.requireOutsideAdmissionLock("Read stop request");
            super.requestStop();
            for (ReadBorrow borrow : borrows.values()) {
                borrow.requestStop();
            }
        }

        boolean complete(Completion completion) {
            return coordinator.completeRead(this, completion);
        }
    }

    static final class ReadBorrow extends JoinableOperation {
        private final PaimonServiceResourceCoordinator coordinator;
        private final ReadAdmission parent;
        private final long borrowId;
        private final String tableKey;

        private ReadBorrow(
                PaimonServiceResourceCoordinator coordinator,
                ReadAdmission parent,
                long borrowId,
                String tableKey) {
            super(coordinator);
            this.coordinator = coordinator;
            this.parent = parent;
            this.borrowId = borrowId;
            this.tableKey = tableKey;
        }

        boolean complete(Completion completion) {
            return coordinator.completeBorrow(this, completion);
        }
    }

    static final class ServiceReadFence extends OperationSnapshot<ReadAdmission> {
        private ServiceReadFence(
                PaimonServiceResourceCoordinator coordinator,
                long fenceId,
                List<ReadAdmission> operations) {
            super(coordinator, fenceId, operations);
        }
    }

    static final class TableReadFence extends OperationSnapshot<ReadBorrow> {
        private final PaimonServiceResourceCoordinator coordinator;
        private final String tableKey;

        private TableReadFence(
                PaimonServiceResourceCoordinator coordinator,
                long fenceId,
                String tableKey,
                List<ReadBorrow> operations) {
            super(coordinator, fenceId, operations);
            this.coordinator = coordinator;
            this.tableKey = tableKey;
        }
    }

    abstract static class OperationSnapshot<T extends JoinableOperation> {
        private final PaimonServiceResourceCoordinator coordinator;
        private final long fenceId;
        private final List<T> operations;

        private OperationSnapshot(
                PaimonServiceResourceCoordinator coordinator,
                long fenceId,
                List<T> operations) {
            this.coordinator = coordinator;
            this.fenceId = fenceId;
            this.operations =
                    Collections.unmodifiableList(new ArrayList<>(operations));
        }

        void requestStop() {
            coordinator.requireOutsideAdmissionLock("Read stop request");
            for (T operation : operations) {
                operation.requestStop();
            }
        }

        void awaitCompletion() throws InterruptedException {
            coordinator.requireOutsideAdmissionLock("Read operation join");
            for (T operation : operations) {
                operation.awaitCompletion();
            }
        }

        int operationCount() {
            return operations.size();
        }

        boolean allClosedSuccessfully() {
            return PaimonServiceResourceCoordinator.allClosedSuccessfully(operations);
        }

        final List<T> operations() {
            return operations;
        }

        long fenceId() {
            return fenceId;
        }
    }

    abstract static class JoinableOperation {
        private final PaimonServiceResourceCoordinator coordinator;
        private final AtomicBoolean stopRequested = new AtomicBoolean();
        private final CountDownLatch completionLatch = new CountDownLatch(1);
        private volatile Completion completion;

        private JoinableOperation(PaimonServiceResourceCoordinator coordinator) {
            this.coordinator = coordinator;
        }

        void requestStop() {
            coordinator.requireOutsideAdmissionLock("Read stop request");
            stopRequested.set(true);
        }

        boolean stopRequested() {
            return stopRequested.get();
        }

        void awaitCompletion() throws InterruptedException {
            coordinator.requireOutsideAdmissionLock("Read operation join");
            completionLatch.await();
        }

        Completion completion() {
            return completion;
        }

        final void publishCompletion(Completion completion) {
            this.completion = completion;
        }

        final void signalCompletion() {
            completionLatch.countDown();
        }
    }

    static final class AdmissionRejectedException extends IllegalStateException {
        private AdmissionRejectedException(String message) {
            super(message);
        }
    }

    private static final class ReadIdentity {
        private final String readOperationId;
        private final String serviceOwnerId;

        private ReadIdentity(String readOperationId, String serviceOwnerId) {
            this.readOperationId = readOperationId;
            this.serviceOwnerId = serviceOwnerId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ReadIdentity)) {
                return false;
            }
            ReadIdentity that = (ReadIdentity) other;
            return readOperationId.equals(that.readOperationId)
                    && serviceOwnerId.equals(that.serviceOwnerId);
        }

        @Override
        public int hashCode() {
            int result = readOperationId.hashCode();
            return 31 * result + serviceOwnerId.hashCode();
        }

        @Override
        public String toString() {
            return serviceOwnerId + "/" + readOperationId;
        }
    }
}
