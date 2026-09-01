package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.write.PaimonTableCommitter;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.PaimonWriteResourceLifecycle;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.connector.paimon.service.PaimonServiceResourceCoordinator.Completion.CLOSED_SUCCESS;
import static io.tapdata.connector.paimon.service.PaimonServiceResourceCoordinator.Completion.RETAINED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonServiceResourceCoordinatorTest {

    @Test
    void acquisitionReservationMustBlockGlobalCloseBeforePhysicalSlotExists() throws Exception {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        CountDownLatch reservationPublished = new CountDownLatch(1);
        CountDownLatch continueAcquire = new CountDownLatch(1);
        AtomicReference<PaimonWriteGenerationOwnership> acquired = new AtomicReference<>();

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            PaimonServiceResourceCoordinator coordinator =
                    new PaimonServiceResourceCoordinator(
                            "service",
                            leases,
                            () -> {
                                reservationPublished.countDown();
                                try {
                                    fixture.await(continueAcquire);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(
                                            "Acquisition observer interrupted", interrupted);
                                }
                            });
            Future<?> acquisition =
                    fixture.submit(
                            () ->
                                    acquired.set(
                                            coordinator.acquireWriterGeneration(
                                                    "physical-hash", "db.table")));

            fixture.await(reservationPublished);
            assertEquals(1, coordinator.activeWriteGenerationCount());
            assertEquals(0, leases.snapshotOwnedBy("service").totalCount());
            coordinator.beginServiceReadFence();
            assertFalse(coordinator.canCloseGlobalResources());

            continueAcquire.countDown();
            fixture.await(acquisition);
            assertEquals(
                    PaimonWriteGenerationOwnership.Phase.LEASE_ACTIVE,
                    acquired.get().phase());
            BoundResource bound =
                    bindResource(
                            coordinator,
                            acquired.get(),
                            PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                            PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                            null);
            assertTrue(
                    coordinator.releaseUnpublishedGeneration(
                            acquired.get(), bound.outcome));
            assertTrue(coordinator.canCloseGlobalResources());
        }
    }

    @Test
    void writerGenerationReservationMustBeServiceScopedAndBlockOnlyItsOwner() throws Exception {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator serviceA =
                new PaimonServiceResourceCoordinator("service-a", leases);
        PaimonServiceResourceCoordinator serviceB =
                new PaimonServiceResourceCoordinator("service-b", leases);

        PaimonWriteGenerationOwnership generation =
                serviceB.acquireWriterGeneration("physical-hash", "db.table");
        serviceA.beginServiceReadFence();
        serviceB.beginServiceReadFence();

        assertTrue(serviceA.canCloseGlobalResources());
        assertFalse(serviceB.canCloseGlobalResources());
        assertEquals(0, leases.snapshotOwnedBy("service-a").totalCount());
        assertEquals(1, leases.snapshotOwnedBy("service-b").activeCount());
        assertThrows(
                PaimonServiceResourceCoordinator.AdmissionRejectedException.class,
                () -> serviceA.acquireWriterGeneration("other-hash", "db.other"));

        BoundResource bound =
                bindResource(
                        serviceB,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        assertTrue(serviceB.releaseUnpublishedGeneration(generation, bound.outcome));
        assertTrue(serviceB.canCloseGlobalResources());
    }

    @Test
    void physicalReleaseMustRejectWrongReasonAndNonSuccessResourceEvidence()
            throws Exception {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonWriteResourceLifecycle.CloseOutcome wrongReason =
                outcome(
                        bound.lifecycle,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        RuntimeException failure = new RuntimeException("close failed");
        PaimonWriteResourceLifecycle.CloseOutcome retained =
                outcome(
                        bound.lifecycle,
                        PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                        PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                        failure);

        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.releaseUnpublishedGeneration(generation, wrongReason));
        assertThrows(
                IllegalStateException.class,
                () -> coordinator.releaseUnpublishedGeneration(generation, retained));
        assertSame(generation.physicalLease(), leases.currentLease("physical-hash"));
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.LEASE_ACTIVE,
                generation.phase());
        assertTrue(coordinator.releaseUnpublishedGeneration(generation, bound.outcome));
    }

    @Test
    void contextPublicationMustRequireLifecycleExactTableAndOneShotToken() throws Exception {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        PaimonTableWriteContext first = context("db.table");
        PaimonTableWriteContext second = context("db.table");

        assertThrows(
                IllegalStateException.class,
                () -> coordinator.publishContext(generation, first));
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        assertSame(bound.lifecycle, generation.resourceLifecycleHandle());
        assertThrows(
                IllegalStateException.class,
                () ->
                        coordinator.prepareLifecycleBinding(generation));
        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.publishContext(generation, context("db.other")));

        coordinator.publishContext(generation, first);

        assertSame(generation, first.generationOwnership());
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.CONTEXT_ACTIVE,
                generation.phase());
        assertThrows(
                IllegalStateException.class,
                () -> coordinator.publishContext(generation, second));
        assertThrows(
                NoSuchMethodException.class,
                () ->
                        PaimonTableWriteContext.class.getMethod(
                                "attachGenerationOwnership",
                        PaimonWriteGenerationOwnership.class));
    }

    @Test
    void closedSuccessClaimMustNotHoldAdmissionLockAndReservationMustStayVisible()
            throws Exception {
        CountDownLatch claimEntered = new CountDownLatch(1);
        CountDownLatch allowClaim = new CountDownLatch(1);
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        doAnswer(
                        ignored -> {
                            claimEntered.countDown();
                            assertTrue(allowClaim.await(5L, TimeUnit.SECONDS));
                            return null;
                        })
                .when(bound.lifecycle)
                .claimClosedSuccess(
                        same(bound.outcome),
                        eq(PaimonWriteCloseModel.InitiatingReason.STOP));

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> finalizer =
                    fixture.submit(
                            () ->
                                    coordinator.beginStopFinalization(
                                            generation, context, bound.outcome));
            fixture.await(claimEntered);

            AtomicInteger observedActive = new AtomicInteger();
            Future<?> admissionRead =
                    fixture.submit(
                            () -> observedActive.set(coordinator.activeWriteGenerationCount()));
            admissionRead.get(1L, TimeUnit.SECONDS);
            assertEquals(1, observedActive.get());
            assertEquals(
                    PaimonWriteGenerationOwnership.Phase.FINALIZATION_CLAIMING,
                    generation.phase());
            coordinator.beginServiceReadFence();
            assertFalse(coordinator.canCloseGlobalResources());

            allowClaim.countDown();
            fixture.await(finalizer);
        }

        assertEquals(
                PaimonWriteGenerationOwnership.Phase.STOP_FINALIZING,
                generation.phase());
    }

    @Test
    void retainedClaimMustNotHoldAdmissionLock() throws Exception {
        CountDownLatch claimEntered = new CountDownLatch(1);
        CountDownLatch allowClaim = new CountDownLatch(1);
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        RuntimeException closeFailure = new RuntimeException("close failed");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.DDL,
                        PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED,
                        closeFailure);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.prepareDdlTransfer(generation, context);
        doAnswer(
                        ignored -> {
                            claimEntered.countDown();
                            assertTrue(allowClaim.await(5L, TimeUnit.SECONDS));
                            return null;
                        })
                .when(bound.lifecycle)
                .claimRetained(
                        same(bound.outcome),
                        eq(PaimonWriteCloseModel.InitiatingReason.DDL));

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> retention =
                    fixture.submit(
                            () -> coordinator.retainDdlResourceFailure(generation, bound.outcome));
            fixture.await(claimEntered);
            AtomicInteger observedActive = new AtomicInteger();
            Future<?> admissionRead =
                    fixture.submit(
                            () -> observedActive.set(coordinator.activeWriteGenerationCount()));
            admissionRead.get(1L, TimeUnit.SECONDS);
            assertEquals(1, observedActive.get());
            assertEquals(
                    PaimonWriteGenerationOwnership.Phase.FINALIZATION_CLAIMING,
                    generation.phase());

            allowClaim.countDown();
            fixture.await(retention);
        }

        assertEquals(PaimonWriteGenerationOwnership.Phase.RETAINED, generation.phase());
        assertTrue(generation.ownsContext(context));
    }

    @Test
    void failedLifecycleClaimMustRollbackReservationWithoutDroppingOwnership() {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        RuntimeException claimFailure = new RuntimeException("claim failed");
        doThrow(claimFailure)
                .when(bound.lifecycle)
                .claimClosedSuccess(
                        same(bound.outcome),
                        eq(PaimonWriteCloseModel.InitiatingReason.STOP));

        RuntimeException propagated =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                coordinator.beginStopFinalization(
                                        generation, context, bound.outcome));

        assertSame(claimFailure, propagated);
        assertEquals(PaimonWriteGenerationOwnership.Phase.CONTEXT_ACTIVE, generation.phase());
        assertTrue(generation.ownsContext(context));
        assertEquals(1, coordinator.activeWriteGenerationCount());
        assertSame(generation.physicalLease(), leases.currentLease("physical-hash"));
    }

    @Test
    void stopReleaseCompareFailureMustRetainCarrierWithoutAnEmptyOwnershipWindow()
            throws Exception {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service-a", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.beginStopFinalization(generation, context, bound.outcome);

        PhysicalTableWriterLease exactLease = generation.physicalLease();
        assertTrue(leases.releaseActive(exactLease));
        PhysicalTableWriterLease replacement =
                PhysicalTableWriterLease.of(
                        "physical-hash",
                        "db.replacement",
                        "service-b",
                        "replacement-generation",
                        PhysicalTableWriterLease.Purpose.WRITER);
        assertTrue(leases.tryAcquire(replacement));

        assertFalse(coordinator.completeStopFinalization(generation));
        assertEquals(0, coordinator.activeWriteGenerationCount());
        assertEquals(1, coordinator.retainedWriteGenerationCount());
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.RETAINED,
                generation.phase());
        assertSame(bound.lifecycle, generation.resourceLifecycleHandle());
        coordinator.beginServiceReadFence();
        assertFalse(coordinator.canCloseGlobalResources());
        assertSame(replacement, leases.currentLease("physical-hash"));
    }

    @Test
    void concurrentStopFinalizersMustBothObserveFalseOnCompareRejection()
            throws Exception {
        CountDownLatch rejectedTransitionReached = new CountDownLatch(1);
        CountDownLatch allowRejectedTransitionReturn = new CountDownLatch(1);
        AtomicInteger rejectedReleaseAttempts = new AtomicInteger();
        PhysicalTableWriterLease.Registry leases =
                new PhysicalTableWriterLease.Registry(
                        new PhysicalTableWriterLease.Registry.TransitionObserver() {
                            @Override
                            public void afterReleaseActive(
                                    PhysicalTableWriterLease expected, boolean released) {
                                if (released) {
                                    return;
                                }
                                rejectedReleaseAttempts.incrementAndGet();
                                rejectedTransitionReached.countDown();
                                try {
                                    assertTrue(
                                            allowRejectedTransitionReturn.await(
                                                    5L, TimeUnit.SECONDS));
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(
                                            "Registry observer interrupted", interrupted);
                                }
                            }
                        });
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service-a", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.beginStopFinalization(generation, context, bound.outcome);

        assertTrue(leases.releaseActive(generation.physicalLease()));
        PhysicalTableWriterLease replacement =
                PhysicalTableWriterLease.of(
                        "physical-hash",
                        "db.replacement",
                        "service-b",
                        "replacement-generation",
                        PhysicalTableWriterLease.Purpose.WRITER);
        assertTrue(leases.tryAcquire(replacement));
        AtomicReference<Boolean> ownerResult = new AtomicReference<>();
        AtomicReference<Boolean> joinerResult = new AtomicReference<>();

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> owner =
                    fixture.submit(
                            () ->
                                    ownerResult.set(
                                            coordinator.completeStopFinalization(generation)));
            fixture.await(rejectedTransitionReached);
            Future<?> joiner =
                    fixture.submit(
                            () ->
                                    joinerResult.set(
                                            coordinator.completeStopFinalization(generation)));
            assertFalse(joiner.isDone());

            allowRejectedTransitionReturn.countDown();
            fixture.await(owner);
            fixture.await(joiner);
        }

        assertEquals(1, rejectedReleaseAttempts.get());
        assertFalse(ownerResult.get());
        assertFalse(joinerResult.get());
        assertEquals(PaimonWriteGenerationOwnership.Phase.RETAINED, generation.phase());
        assertSame(replacement, leases.currentLease("physical-hash"));
    }

    @Test
    void concurrentStopFinalizersMustJoinOneExactRegistryRelease() throws Exception {
        CountDownLatch registryTransitionReached = new CountDownLatch(1);
        CountDownLatch allowRegistryReturn = new CountDownLatch(1);
        AtomicInteger releaseAttempts = new AtomicInteger();
        PhysicalTableWriterLease.Registry leases =
                new PhysicalTableWriterLease.Registry(
                        new PhysicalTableWriterLease.Registry.TransitionObserver() {
                            @Override
                            public void afterReleaseActive(
                                    PhysicalTableWriterLease expected, boolean released) {
                                releaseAttempts.incrementAndGet();
                                registryTransitionReached.countDown();
                                try {
                                    assertTrue(
                                            allowRegistryReturn.await(
                                                    5L, TimeUnit.SECONDS));
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(
                                            "Registry observer interrupted", interrupted);
                                }
                            }
                        });
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.beginStopFinalization(generation, context, bound.outcome);
        coordinator.beginServiceReadFence();
        AtomicReference<Boolean> ownerResult = new AtomicReference<>();
        AtomicReference<Boolean> joinerResult = new AtomicReference<>();

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> owner =
                    fixture.submit(
                            () ->
                                    ownerResult.set(
                                            coordinator.completeStopFinalization(generation)));
            fixture.await(registryTransitionReached);
            assertEquals(
                    PaimonWriteGenerationOwnership.Phase.RELEASE_IN_PROGRESS,
                    generation.phase());
            assertEquals(1, coordinator.activeWriteGenerationCount());
            assertFalse(coordinator.canCloseGlobalResources());

            Future<?> joiner =
                    fixture.submit(
                            () ->
                                    joinerResult.set(
                                            coordinator.completeStopFinalization(generation)));
            assertFalse(joiner.isDone());
            allowRegistryReturn.countDown();

            fixture.await(owner);
            fixture.await(joiner);
        }

        assertEquals(1, releaseAttempts.get());
        assertTrue(ownerResult.get());
        assertTrue(joinerResult.get());
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.RELEASED,
                generation.phase());
        assertTrue(coordinator.canCloseGlobalResources());
    }

    @Test
    void factoryRollbackMustRejectASecondReleaseOwnerInsteadOfJoining() throws Exception {
        CountDownLatch registryTransitionReached = new CountDownLatch(1);
        CountDownLatch allowRegistryReturn = new CountDownLatch(1);
        PhysicalTableWriterLease.Registry leases =
                new PhysicalTableWriterLease.Registry(
                        new PhysicalTableWriterLease.Registry.TransitionObserver() {
                            @Override
                            public void afterReleaseActive(
                                    PhysicalTableWriterLease expected, boolean released) {
                                registryTransitionReached.countDown();
                                try {
                                    assertTrue(allowRegistryReturn.await(5L, TimeUnit.SECONDS));
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(
                                            "Registry observer interrupted", interrupted);
                                }
                            }
                        });
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        AtomicReference<Boolean> ownerResult = new AtomicReference<>();

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> owner =
                    fixture.submit(
                            () ->
                                    ownerResult.set(
                                            coordinator.releaseUnpublishedGeneration(
                                                    generation, bound.outcome)));
            fixture.await(registryTransitionReached);

            assertThrows(
                    IllegalStateException.class,
                    () ->
                            coordinator.releaseUnpublishedGeneration(
                                    generation, bound.outcome));
            allowRegistryReturn.countDown();
            fixture.await(owner);
            assertTrue(ownerResult.get());
        }
    }

    @Test
    void releaseTransitionFailureMustPublishRetainedAndRemainJoinable() throws Exception {
        RuntimeException transitionFailure = new RuntimeException("registry return failed");
        AtomicInteger releaseAttempts = new AtomicInteger();
        PhysicalTableWriterLease.Registry leases =
                new PhysicalTableWriterLease.Registry(
                        new PhysicalTableWriterLease.Registry.TransitionObserver() {
                            @Override
                            public void afterReleaseActive(
                                    PhysicalTableWriterLease expected, boolean released) {
                                releaseAttempts.incrementAndGet();
                                throw transitionFailure;
                            }
                        });
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.beginStopFinalization(generation, context, bound.outcome);

        RuntimeException ownerFailure =
                assertThrows(
                        RuntimeException.class,
                        () -> coordinator.completeStopFinalization(generation));
        RuntimeException joinFailure =
                assertThrows(
                        RuntimeException.class,
                        () -> coordinator.completeStopFinalization(generation));

        assertSame(transitionFailure, ownerFailure);
        assertSame(transitionFailure, joinFailure);
        assertEquals(1, releaseAttempts.get());
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.REGISTRY_TRANSITION_FAILED_RETAINED,
                generation.phase());
        assertSame(transitionFailure, generation.retentionFailure());
        assertEquals(0, coordinator.activeWriteGenerationCount());
        assertEquals(1, coordinator.retainedWriteGenerationCount());
        coordinator.beginServiceReadFence();
        assertFalse(coordinator.canCloseGlobalResources());
    }

    @Test
    void foreignCoordinatorCannotMutateAnotherServicesCarrier() throws Exception {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator serviceA =
                new PaimonServiceResourceCoordinator("service-a", leases);
        PaimonServiceResourceCoordinator serviceB =
                new PaimonServiceResourceCoordinator("service-b", leases);
        PaimonWriteGenerationOwnership generation =
                serviceA.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        serviceA,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        serviceB.releaseUnpublishedGeneration(
                                generation, bound.outcome));
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.LEASE_ACTIVE,
                generation.phase());
        assertEquals(1, leases.snapshotOwnedBy("service-a").activeCount());
        assertTrue(serviceA.releaseUnpublishedGeneration(generation, bound.outcome));
    }

    @Test
    void ddlTransferMustRequireDetachAndResourceSuccessBeforeActionScope() {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.DDL,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        PhysicalTableWriterLease exactLease = generation.physicalLease();

        coordinator.prepareDdlTransfer(generation, context);
        assertSame(exactLease, leases.currentLease("physical-hash"));
        assertEquals(0, coordinator.ownershipTransferCount());
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.DDL_TRANSFER_PREPARED,
                generation.phase());
        PaimonWriteResourceLifecycle.CloseOutcome wrongReason =
                outcome(
                        bound.lifecycle,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        coordinator.authorizeDdlDetach(
                                generation, context, wrongReason));
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.DDL_TRANSFER_PREPARED,
                generation.phase());
        assertTrue(generation.ownsContext(context));

        PaimonWriteGenerationOwnership.DdlDetachPermit permit =
                coordinator.authorizeDdlDetach(generation, context, bound.outcome);
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.DDL_DETACH_AUTHORIZED,
                generation.phase());
        assertThrows(
                IllegalStateException.class,
                () ->
                        coordinator.detachExactContextForDdl(
                                generation, context("db.table"), permit));
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.DDL_DETACH_AUTHORIZED,
                generation.phase());
        assertTrue(coordinator.detachExactContextForDdl(generation, context, permit));
        assertEquals(1, coordinator.ownershipTransferCount());
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.TRANSFER_IN_PROGRESS,
                generation.phase());
        coordinator.completeDdlTransfer(generation);
        assertSame(exactLease, leases.currentLease("physical-hash"));
        assertEquals(0, coordinator.ownershipTransferCount());
        assertEquals(1, coordinator.activeDdlActionCount());
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.DDL_ACTION_ACTIVE,
                generation.phase());
        assertEquals(1, coordinator.activeWriteGenerationCount());
        assertEquals(1, leases.snapshotOwnedBy("service").activeCount());
    }

    @Test
    void ddlCloseJoinedByStopMustFinalizeAsStopWithoutAdmittingDdlAction()
            throws Exception {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.DDL,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.prepareDdlTransfer(generation, context);
        PaimonWriteResourceLifecycle.CloseOutcome stopJoined =
                outcome(
                        bound.lifecycle,
                        PaimonWriteCloseModel.InitiatingReason.DDL,
                        PaimonWriteCloseModel.InitiatingReason.STOP,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        coordinator.authorizeDdlDetach(
                                generation, context, stopJoined));
        coordinator.beginStopFinalization(generation, context, stopJoined);
        assertEquals(0, coordinator.ownershipTransferCount());
        assertEquals(0, coordinator.activeDdlActionCount());
        assertTrue(coordinator.completeStopFinalization(generation));
        assertEquals(PaimonWriteGenerationOwnership.Phase.RELEASED, generation.phase());
    }

    @Test
    void admittedDdlMustNotBePreemptedAfterExactContextDetach() {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.DDL,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.prepareDdlTransfer(generation, context);
        PaimonWriteGenerationOwnership.DdlDetachPermit permit =
                coordinator.authorizeDdlDetach(generation, context, bound.outcome);
        assertTrue(coordinator.detachExactContextForDdl(generation, context, permit));

        assertThrows(
                IllegalStateException.class,
                () -> coordinator.beginStopFinalization(generation, context, bound.outcome));
        assertEquals(1, coordinator.ownershipTransferCount());

        coordinator.completeDdlTransfer(generation);

        assertEquals(0, coordinator.ownershipTransferCount());
        assertEquals(1, coordinator.activeDdlActionCount());
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.DDL_ACTION_ACTIVE,
                generation.phase());
    }

    @Test
    void absentOrReplacedExpectedContextMustRetainExactContextAndConsumePermit() {
        for (boolean replaceContext : new boolean[] {false, true}) {
            PhysicalTableWriterLease.Registry leases =
                    new PhysicalTableWriterLease.Registry();
            Map<String, PaimonTableWriteContext> contexts = new ConcurrentHashMap<>();
            PaimonServiceResourceCoordinator coordinator =
                    new PaimonServiceResourceCoordinator(
                            "service", leases, contexts, () -> {});
            PaimonWriteGenerationOwnership generation =
                    coordinator.acquireWriterGeneration("physical-hash", "db.table");
            BoundResource bound =
                    bindResource(
                            coordinator,
                            generation,
                            PaimonWriteCloseModel.InitiatingReason.DDL,
                            PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                            null);
            PaimonTableWriteContext context = context("db.table");
            coordinator.publishContext(generation, context);
            coordinator.prepareDdlTransfer(generation, context);
            PaimonWriteGenerationOwnership.DdlDetachPermit permit =
                    coordinator.authorizeDdlDetach(generation, context, bound.outcome);
            contexts.remove("db.table", context);
            if (replaceContext) {
                contexts.put("db.table", context("db.table"));
            }

            assertFalse(coordinator.detachExactContextForDdl(generation, context, permit));

            assertEquals(PaimonWriteGenerationOwnership.Phase.RETAINED, generation.phase());
            assertTrue(generation.ownsContext(context));
            assertNotNull(generation.retentionFailure());
            assertEquals(1, coordinator.retainedWriteGenerationCount());
            assertSame(generation.physicalLease(), leases.currentLease("physical-hash"));
            assertThrows(
                    IllegalStateException.class,
                    () -> coordinator.detachExactContextForDdl(generation, context, permit));
        }
    }

    @Test
    void failedDdlActionMustAtomicallyRetainTheSamePhysicalLease() {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.DDL,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.prepareDdlTransfer(generation, context);
        authorizeAndConfirmDdlDetach(coordinator, generation, context, bound.outcome);
        coordinator.completeDdlTransfer(generation);
        RuntimeException failure = new RuntimeException("ddl failed");

        coordinator.retainDdlActionFailure(generation, failure);
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.RETAINED,
                generation.phase());
        assertSame(failure, generation.retentionFailure());
        assertSame(
                generation.retainedDdlActionLease(),
                leases.currentRetained("physical-hash"));
        assertEquals(1, coordinator.retainedWriteGenerationCount());
        assertEquals(1, leases.snapshotOwnedBy("service").retainedCount());
    }

    @Test
    void ddlRetentionMustRemainBarrierVisibleAcrossRegistryCoordinatorTransition()
            throws Exception {
        CountDownLatch registryRetained = new CountDownLatch(1);
        CountDownLatch allowCoordinatorPublication = new CountDownLatch(1);
        PhysicalTableWriterLease.Registry leases =
                new PhysicalTableWriterLease.Registry(
                        new PhysicalTableWriterLease.Registry.TransitionObserver() {
                            @Override
                            public void afterRetainDdl(
                                    PhysicalTableWriterLease expected,
                                    RetainedDdlActionLease retained) {
                                registryRetained.countDown();
                                try {
                                    assertTrue(
                                            allowCoordinatorPublication.await(
                                                    5L, TimeUnit.SECONDS));
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(
                                            "Registry observer interrupted", interrupted);
                                }
                            }
                        });
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.DDL,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.prepareDdlTransfer(generation, context);
        authorizeAndConfirmDdlDetach(coordinator, generation, context, bound.outcome);
        coordinator.completeDdlTransfer(generation);
        coordinator.beginServiceReadFence();

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> retention =
                    fixture.submit(
                            () ->
                                    coordinator.retainDdlActionFailure(
                                            generation, new RuntimeException("ddl failed")));
            fixture.await(registryRetained);

            assertEquals(
                    PaimonWriteGenerationOwnership.Phase.DDL_RETAIN_IN_PROGRESS,
                    generation.phase());
            assertEquals(1, coordinator.activeWriteGenerationCount());
            assertEquals(1, coordinator.activeDdlActionCount());
            assertEquals(1, leases.snapshotOwnedBy("service").retainedCount());
            assertFalse(coordinator.canCloseGlobalResources());

            allowCoordinatorPublication.countDown();
            fixture.await(retention);
        }

        assertEquals(PaimonWriteGenerationOwnership.Phase.RETAINED, generation.phase());
        assertEquals(0, coordinator.activeWriteGenerationCount());
        assertEquals(0, coordinator.activeDdlActionCount());
        assertEquals(1, coordinator.retainedWriteGenerationCount());
        assertFalse(coordinator.canCloseGlobalResources());
    }

    @Test
    void ddlRegistryTransitionFailureMustPublishRestartRequiredCarrier() {
        RuntimeException registryFailure = new RuntimeException("registry observer failed");
        PhysicalTableWriterLease.Registry leases =
                new PhysicalTableWriterLease.Registry(
                        new PhysicalTableWriterLease.Registry.TransitionObserver() {
                            @Override
                            public void afterRetainDdl(
                                    PhysicalTableWriterLease expected,
                                    RetainedDdlActionLease retained) {
                                throw registryFailure;
                            }
                        });
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.DDL,
                        PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                        null);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.prepareDdlTransfer(generation, context);
        authorizeAndConfirmDdlDetach(coordinator, generation, context, bound.outcome);
        coordinator.completeDdlTransfer(generation);
        RuntimeException actionFailure = new RuntimeException("ddl failed");

        RuntimeException propagated =
                assertThrows(
                        RuntimeException.class,
                        () -> coordinator.retainDdlActionFailure(generation, actionFailure));

        assertSame(registryFailure, propagated);
        assertEquals(
                PaimonWriteGenerationOwnership.Phase.REGISTRY_TRANSITION_FAILED_RETAINED,
                generation.phase());
        assertSame(actionFailure, generation.retentionFailure());
        assertSame(
                leases.currentRetained("physical-hash"),
                generation.retainedDdlActionLease());
        assertEquals(1, actionFailure.getSuppressed().length);
        assertSame(registryFailure, actionFailure.getSuppressed()[0]);
        assertEquals(1, coordinator.retainedWriteGenerationCount());
        assertEquals(1, leases.snapshotOwnedBy("service").retainedCount());
    }

    @Test
    void retainedDdlResourceOutcomeMustBlockActionAndKeepActivePhysicalSlot() {
        PhysicalTableWriterLease.Registry leases = new PhysicalTableWriterLease.Registry();
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator("service", leases);
        PaimonWriteGenerationOwnership generation =
                coordinator.acquireWriterGeneration("physical-hash", "db.table");
        RuntimeException failure = new RuntimeException("resource close failed");
        BoundResource bound =
                bindResource(
                        coordinator,
                        generation,
                        PaimonWriteCloseModel.InitiatingReason.DDL,
                        PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED,
                        failure);
        PaimonTableWriteContext context = context("db.table");
        coordinator.publishContext(generation, context);
        coordinator.prepareDdlTransfer(generation, context);

        coordinator.retainDdlResourceFailure(generation, bound.outcome);

        assertEquals(0, coordinator.activeDdlActionCount());
        assertEquals(0, coordinator.activeWriteGenerationCount());
        assertEquals(1, coordinator.retainedWriteGenerationCount());
        assertSame(failure, generation.retentionFailure());
        assertTrue(generation.ownsContext(context));
        assertSame(generation.physicalLease(), leases.currentLease("physical-hash"));
    }

    @Test
    void readAdmissionWinningServiceFenceMustRemainJoinableUntilOwnerCompletes() throws Exception {
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator();
        CountDownLatch admitted = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        AtomicReference<PaimonServiceResourceCoordinator.ReadAdmission> admission =
                new AtomicReference<>();

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> owner =
                    fixture.submit(
                            () -> {
                                PaimonServiceResourceCoordinator.ReadAdmission admittedRead =
                                        coordinator.admitRead(
                                                "read-1",
                                                "service-1",
                                                Collections.singleton("db.table_a"));
                                admission.set(admittedRead);
                                admitted.countDown();
                                fixture.await(releaseOwner);
                                admittedRead.borrow("db.table_a").complete(CLOSED_SUCCESS);
                                admittedRead.complete(CLOSED_SUCCESS);
                            });

            fixture.await(admitted);
            PaimonServiceResourceCoordinator.ServiceReadFence fence =
                    coordinator.beginServiceReadFence();
            fence.requestStop();
            assertTrue(admission.get().stopRequested());
            assertEquals(1, fence.operationCount());

            Future<?> join = fixture.submit(fence::awaitCompletion);
            assertFalse(join.isDone());
            releaseOwner.countDown();

            fixture.await(join);
            fixture.await(owner);
            assertTrue(fence.allClosedSuccessfully());
            assertTrue(coordinator.canCloseGlobalResources());
        }
    }

    @Test
    void serviceFenceWinningAdmissionMustRejectBeforeAnyDownstreamAccess() throws Exception {
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator();
        CountDownLatch readReady = new CountDownLatch(1);
        CountDownLatch allowReadAttempt = new CountDownLatch(1);
        AtomicInteger downstreamAccess = new AtomicInteger();

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> read =
                    fixture.submit(
                            () -> {
                                readReady.countDown();
                                fixture.await(allowReadAttempt);
                                assertThrows(
                                        PaimonServiceResourceCoordinator.AdmissionRejectedException.class,
                                        () -> {
                                            coordinator.admitRead(
                                                    "read-after-stop",
                                                    "service-1",
                                                    Collections.singleton("db.table_a"));
                                            downstreamAccess.incrementAndGet();
                                        });
                            });

            fixture.await(readReady);
            PaimonServiceResourceCoordinator.ServiceReadFence fence =
                    coordinator.beginServiceReadFence();
            allowReadAttempt.countDown();

            fixture.await(read);
            assertEquals(0, downstreamAccess.get());
            assertEquals(0, fence.operationCount());
            assertTrue(coordinator.canCloseGlobalResources());
        }
    }

    @Test
    void tableFenceMustJoinOnlyTargetBorrowerAndMustNotBlockAnotherTable() throws Exception {
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator();
        PaimonServiceResourceCoordinator.ReadAdmission readAAndB =
                coordinator.admitRead(
                        "read-ab",
                        "service-1",
                        Arrays.asList("db.table_b", "db.table_a", "db.table_a"));

        PaimonServiceResourceCoordinator.TableReadFence tableAFence =
                coordinator.beginTableDdlFence("db.table_a");
        tableAFence.requestStop();

        assertEquals(Arrays.asList("db.table_a", "db.table_b"), readAAndB.tableKeys());
        assertTrue(readAAndB.borrow("db.table_a").stopRequested());
        assertFalse(readAAndB.borrow("db.table_b").stopRequested());
        assertEquals(1, tableAFence.operationCount());
        assertThrows(
                PaimonServiceResourceCoordinator.AdmissionRejectedException.class,
                () ->
                        coordinator.admitRead(
                                "read-a-rejected",
                                "service-1",
                                Collections.singleton("db.table_a")));

        PaimonServiceResourceCoordinator.ReadAdmission readB =
                coordinator.admitRead(
                        "read-b",
                        "service-1",
                        Collections.singleton("db.table_b"));
        assertNotNull(readB.borrow("db.table_b"));

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> join = fixture.submit(tableAFence::awaitCompletion);
            assertFalse(join.isDone());
            readAAndB.borrow("db.table_a").complete(CLOSED_SUCCESS);
            fixture.await(join);
        }

        assertTrue(tableAFence.allClosedSuccessfully());
        coordinator.releaseTableDdlFence(tableAFence);

        readAAndB.borrow("db.table_b").complete(CLOSED_SUCCESS);
        readAAndB.complete(CLOSED_SUCCESS);
        readB.borrow("db.table_b").complete(CLOSED_SUCCESS);
        readB.complete(CLOSED_SUCCESS);
    }

    @Test
    void tableFenceWinningMultiTableAdmissionMustRejectAtomicallyWithoutPartialBorrow() {
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator();
        PaimonServiceResourceCoordinator.TableReadFence fence =
                coordinator.beginTableDdlFence("db.table_a");

        assertThrows(
                PaimonServiceResourceCoordinator.AdmissionRejectedException.class,
                () ->
                        coordinator.admitRead(
                                "read-ab",
                                "service-1",
                                Arrays.asList("db.table_a", "db.table_b")));
        assertEquals(0, coordinator.activeReadCount());
        assertEquals(0, coordinator.activeBorrowCount("db.table_b"));

        coordinator.releaseTableDdlFence(fence);
        PaimonServiceResourceCoordinator.ReadAdmission readB =
                coordinator.admitRead(
                        "read-b",
                        "service-1",
                        Collections.singleton("db.table_b"));
        readB.borrow("db.table_b").complete(CLOSED_SUCCESS);
        readB.complete(CLOSED_SUCCESS);
    }

    @Test
    void fencePublicationMustNotAwaitBorrowerWhileHoldingAdmissionLock() throws Exception {
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator();
        PaimonServiceResourceCoordinator.ReadAdmission read =
                coordinator.admitRead(
                        "read-a",
                        "service-1",
                        Collections.singleton("db.table_a"));

        try (PaimonCoordinatorLockOrderFixture fixture =
                new PaimonCoordinatorLockOrderFixture()) {
            Future<?> publication =
                    fixture.submit(
                            () -> {
                                PaimonServiceResourceCoordinator.TableReadFence fence =
                                        coordinator.beginTableDdlFence("db.table_a");
                                assertEquals(1, fence.operationCount());
                            });
            fixture.await(publication);

            PaimonServiceResourceCoordinator.TableReadFence fence =
                    coordinator.beginTableDdlFence("db.table_a");
            assertThrows(
                    IllegalStateException.class,
                    () -> fixture.whileAdmissionLockHeld(coordinator, fence::awaitCompletion));

            read.borrow("db.table_a").complete(CLOSED_SUCCESS);
            read.complete(CLOSED_SUCCESS);
            fence.awaitCompletion();
            coordinator.releaseTableDdlFence(fence);
        }
    }

    @Test
    void retainedBorrowerMustRemainVisibleAndBlockFenceReleaseAndGlobalClose() throws Exception {
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator();
        PaimonServiceResourceCoordinator.ReadAdmission read =
                coordinator.admitRead(
                        "read-retained",
                        "service-1",
                        Collections.singleton("db.table_a"));
        PaimonServiceResourceCoordinator.TableReadFence tableFence =
                coordinator.beginTableDdlFence("db.table_a");

        read.borrow("db.table_a").complete(RETAINED);
        read.complete(RETAINED);
        tableFence.awaitCompletion();

        assertFalse(tableFence.allClosedSuccessfully());
        assertThrows(
                IllegalStateException.class,
                () -> coordinator.releaseTableDdlFence(tableFence));
        coordinator.beginServiceReadFence();
        assertFalse(coordinator.canCloseGlobalResources());
        assertEquals(1, coordinator.retainedReadCount());
        assertEquals(1, coordinator.retainedBorrowCount());
    }

    @Test
    void completionMustBeChildFirstExactAndMonotonic() {
        PaimonServiceResourceCoordinator coordinator =
                new PaimonServiceResourceCoordinator();
        PaimonServiceResourceCoordinator.ReadAdmission read =
                coordinator.admitRead(
                        "read-exact",
                        "service-1",
                        Collections.singleton("db.table_a"));
        PaimonServiceResourceCoordinator.TableReadFence fence =
                coordinator.beginTableDdlFence("db.table_a");

        assertThrows(IllegalStateException.class, () -> read.complete(CLOSED_SUCCESS));
        assertThrows(
                IllegalStateException.class, () -> coordinator.releaseTableDdlFence(fence));
        assertTrue(read.borrow("db.table_a").complete(CLOSED_SUCCESS));
        assertFalse(read.borrow("db.table_a").complete(CLOSED_SUCCESS));
        assertThrows(
                IllegalStateException.class,
                () -> read.borrow("db.table_a").complete(RETAINED));
        assertTrue(read.complete(CLOSED_SUCCESS));
        assertFalse(read.complete(CLOSED_SUCCESS));
        assertThrows(IllegalStateException.class, () -> read.complete(RETAINED));

        coordinator.releaseTableDdlFence(fence);
    }

    private static BoundResource bindResource(
            PaimonServiceResourceCoordinator coordinator,
            PaimonWriteGenerationOwnership ownership,
            PaimonWriteCloseModel.InitiatingReason reason,
            PaimonWriteCloseModel.CloseState state,
            Throwable failure) {
        PaimonWriteGenerationOwnership.LifecycleBinding binding =
                coordinator.prepareLifecycleBinding(ownership);
        Object exactCapability = binding.generationCapability();
        PaimonWriteResourceLifecycle lifecycle = mock(PaimonWriteResourceLifecycle.class);
        when(lifecycle.matchesGenerationCapability(exactCapability)).thenReturn(true);
        binding.bind(lifecycle);

        PaimonWriteResourceLifecycle.CloseOutcome outcome =
                outcome(lifecycle, reason, state, failure);
        return new BoundResource(lifecycle, outcome);
    }

    private static void authorizeAndConfirmDdlDetach(
            PaimonServiceResourceCoordinator coordinator,
            PaimonWriteGenerationOwnership ownership,
            PaimonTableWriteContext context,
            PaimonWriteResourceLifecycle.CloseOutcome outcome) {
        PaimonWriteGenerationOwnership.DdlDetachPermit permit =
                coordinator.authorizeDdlDetach(ownership, context, outcome);
        assertTrue(coordinator.detachExactContextForDdl(ownership, context, permit));
    }

    private static PaimonWriteResourceLifecycle.CloseOutcome outcome(
            PaimonWriteResourceLifecycle lifecycle,
            PaimonWriteCloseModel.InitiatingReason reason,
            PaimonWriteCloseModel.CloseState state,
            Throwable failure) {
        return outcome(lifecycle, reason, reason, state, failure);
    }

    private static PaimonWriteResourceLifecycle.CloseOutcome outcome(
            PaimonWriteResourceLifecycle lifecycle,
            PaimonWriteCloseModel.InitiatingReason initiatingReason,
            PaimonWriteCloseModel.InitiatingReason effectiveReason,
            PaimonWriteCloseModel.CloseState state,
            Throwable failure) {
        PaimonWriteResourceLifecycle.CloseOutcome outcome =
                mock(PaimonWriteResourceLifecycle.CloseOutcome.class);
        when(outcome.belongsTo(lifecycle)).thenReturn(true);
        when(outcome.matchesGenerationCapability(any())).thenReturn(true);
        when(outcome.initiatingReason()).thenReturn(initiatingReason);
        when(outcome.effectiveReason()).thenReturn(effectiveReason);
        when(outcome.state()).thenReturn(state);
        when(outcome.failure()).thenReturn(failure);
        return outcome;
    }

    private static PaimonTableWriteContext context(String tableKey) {
        return new PaimonTableWriteContext(
                tableKey,
                "table",
                "commit-user",
                mock(PaimonBucketWriterStrategy.class),
                mock(PaimonTableCommitter.class),
                null,
                Collections.emptyList(),
                0L);
    }

    private static final class BoundResource {
        private final PaimonWriteResourceLifecycle lifecycle;
        private final PaimonWriteResourceLifecycle.CloseOutcome outcome;

        private BoundResource(
                PaimonWriteResourceLifecycle lifecycle,
                PaimonWriteResourceLifecycle.CloseOutcome outcome) {
            this.lifecycle = lifecycle;
            this.outcome = outcome;
        }
    }
}
