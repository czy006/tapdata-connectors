package io.tapdata.connector.paimon.service;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.connector.paimon.service.PaimonServiceResourceCoordinator.Completion.CLOSED_SUCCESS;
import static io.tapdata.connector.paimon.service.PaimonServiceResourceCoordinator.Completion.RETAINED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonServiceResourceCoordinatorTest {

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
}
