package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.service.PaimonWriteCloseModel;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonWriteResourceLifecycleTest {

    @Test
    void successMustFollowFixedSafetyOrderAndReleaseExactLeaseLast() throws Exception {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        PaimonCompactionRuntime compaction = new PaimonCompactionRuntime("success");
        compaction.executor().execute(() -> events.add("compaction"));
        PaimonMaintenanceAdapter maintenance = maintenance(events, success());
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        compaction,
                        () -> events.add("writer"),
                        maintenance,
                        () -> events.add("committer"),
                        () -> events.add("io"),
                        () -> events.add("spill"),
                        () -> {
                            events.add("lease");
                            return true;
                        });

        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));
        lifecycle.beginCompactionShutdown(operation);
        PaimonWriteResourceLifecycle.CloseOutcome outcome =
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);

        assertEquals(PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS, outcome.state());
        assertTrue(outcome.terminal());
        assertEquals(
                Arrays.asList(
                        "compaction", "writer", "maintenance", "committer", "io", "spill", "lease"),
                events);
        PaimonWriteResourceLifecycle.CloseSnapshot snapshot = lifecycle.closeSnapshot();
        assertTrue(snapshot.compactionShutdownStarted());
        assertTrue(snapshot.compactionTerminated());
        assertEquals(
                PaimonWriteCloseModel.DelegateCloseStatus.SUCCEEDED, snapshot.writer());
        assertEquals(PaimonWriteResourceLifecycle.StepState.SUCCEEDED, snapshot.maintenance());
        assertEquals(PaimonWriteResourceLifecycle.StepState.SUCCEEDED, snapshot.spill());
        assertEquals(PaimonWriteResourceLifecycle.StepState.SUCCEEDED, snapshot.lease());

        assertSame(operation, lifecycle.beginClose(proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP)));
        assertSame(outcome.failure(), lifecycle.awaitAndFinish(operation, Long.MAX_VALUE).failure());
        verify(maintenance, times(1))
                .shutdownAndAwait(
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void compactionTimeoutMustKeepOperationActiveAndCloseNothing() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger dependencyCloses = new AtomicInteger();
        PaimonCompactionRuntime compaction = new PaimonCompactionRuntime("blocked");
        compaction.executor().execute(
                () -> {
                    started.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
        assertTrue(started.await(10L, TimeUnit.SECONDS));
        PaimonMaintenanceAdapter maintenance = maintenance(new ArrayList<>(), success());
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        compaction,
                        dependencyCloses::incrementAndGet,
                        maintenance,
                        dependencyCloses::incrementAndGet,
                        dependencyCloses::incrementAndGet,
                        dependencyCloses::incrementAndGet,
                        () -> {
                            dependencyCloses.incrementAndGet();
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));

        PaimonWriteResourceLifecycle.CloseOutcome waiting =
                lifecycle.awaitAndFinish(
                        operation, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(20L));

        assertEquals(PaimonWriteCloseModel.CloseState.WAITING_COMPACTION, waiting.state());
        assertFalse(waiting.terminal());
        assertEquals(0, dependencyCloses.get());
        verify(maintenance, times(0))
                .shutdownAndAwait(
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS));

        release.countDown();
        assertEquals(
                PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE).state());
        assertEquals(5, dependencyCloses.get());
    }

    @Test
    void writerFailureMustStillDrainMaintenanceAndCloseIndependentCommitter()
            throws Exception {
        List<String> events = new ArrayList<>();
        IOException writerFailure = new IOException("writer");
        IOException committerFailure = new IOException("committer");
        PaimonMaintenanceAdapter maintenance = maintenance(events, success());
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        new PaimonCompactionRuntime("writer-failure"),
                        () -> {
                            events.add("writer");
                            throw writerFailure;
                        },
                        maintenance,
                        () -> {
                            events.add("committer");
                            throw committerFailure;
                        },
                        () -> events.add("io"),
                        () -> events.add("spill"),
                        () -> {
                            events.add("lease");
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));

        PaimonWriteResourceLifecycle.CloseOutcome outcome =
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);

        assertEquals(
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED,
                outcome.state());
        assertSame(writerFailure, outcome.failure());
        assertEquals(Collections.singletonList(committerFailure), Arrays.asList(writerFailure.getSuppressed()));
        assertEquals(Arrays.asList("writer", "maintenance", "committer"), events);
        assertEquals(PaimonWriteResourceLifecycle.StepState.NOT_STARTED, lifecycle.closeSnapshot().spill());
        assertEquals(PaimonWriteResourceLifecycle.StepState.NOT_STARTED, lifecycle.closeSnapshot().lease());

        lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);
        assertEquals(Arrays.asList("writer", "maintenance", "committer"), events);
    }

    @Test
    void failedDrainedMaintenanceMustRetainOriginalFailureAndForbidIo()
            throws Exception {
        List<String> events = new ArrayList<>();
        IOException maintenanceFailure = new IOException("maintenance");
        PaimonMaintenanceAdapter maintenance =
                maintenance(events, failedDrained(maintenanceFailure));
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        new PaimonCompactionRuntime("maintenance-failure"),
                        () -> events.add("writer"),
                        maintenance,
                        () -> events.add("committer"),
                        () -> events.add("io"),
                        () -> events.add("spill"),
                        () -> {
                            events.add("lease");
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));

        PaimonWriteResourceLifecycle.CloseOutcome outcome =
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);

        assertEquals(
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED,
                outcome.state());
        assertSame(maintenanceFailure, outcome.failure());
        assertEquals(Arrays.asList("writer", "maintenance", "committer"), events);
        assertEquals(
                PaimonWriteCloseModel.DelegateCloseStatus.NOT_ATTEMPTED,
                lifecycle.closeSnapshot().io());
    }

    @Test
    void waitingMaintenanceMustRemainNonTerminalAndRejoinSameOperation()
            throws Exception {
        List<String> events = new ArrayList<>();
        PaimonMaintenanceAdapter maintenance = mock(PaimonMaintenanceAdapter.class);
        when(maintenance.shutdownAndAwait(
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS)))
                .thenAnswer(
                        ignored -> {
                            events.add("maintenance-waiting");
                            return waiting();
                        })
                .thenAnswer(
                        ignored -> {
                            events.add("maintenance-success");
                            return success();
                        });
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        new PaimonCompactionRuntime("maintenance-waiting"),
                        () -> events.add("writer"),
                        maintenance,
                        () -> events.add("committer"),
                        () -> events.add("io"),
                        () -> events.add("spill"),
                        () -> {
                            events.add("lease");
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));

        PaimonWriteResourceLifecycle.CloseOutcome first =
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);
        assertEquals(
                PaimonWriteCloseModel.CloseState.WAITING_COMMIT_MAINTENANCE,
                first.state());
        assertFalse(first.terminal());
        assertEquals(Arrays.asList("writer", "maintenance-waiting"), events);

        assertEquals(
                PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE).state());
        assertEquals(
                Arrays.asList(
                        "writer",
                        "maintenance-waiting",
                        "maintenance-success",
                        "committer",
                        "io",
                        "spill",
                        "lease"),
                events);
        verify(maintenance, times(2))
                .shutdownAndAwait(
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void ioFailureMustBeStickyAndForbidSpillAndLeaseRelease() throws Exception {
        List<String> events = new ArrayList<>();
        IOException ioFailure = new IOException("io");
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        new PaimonCompactionRuntime("io-failure"),
                        () -> events.add("writer"),
                        maintenance(events, success()),
                        () -> events.add("committer"),
                        () -> {
                            events.add("io");
                            throw ioFailure;
                        },
                        () -> events.add("spill"),
                        () -> {
                            events.add("lease");
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));

        PaimonWriteResourceLifecycle.CloseOutcome outcome =
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);

        assertEquals(PaimonWriteCloseModel.CloseState.IO_CLOSE_FAILED_RETAINED, outcome.state());
        assertSame(ioFailure, outcome.failure());
        assertEquals(Arrays.asList("writer", "maintenance", "committer", "io"), events);
        assertEquals(PaimonWriteResourceLifecycle.StepState.NOT_STARTED, lifecycle.closeSnapshot().spill());
        assertEquals(PaimonWriteResourceLifecycle.StepState.NOT_STARTED, lifecycle.closeSnapshot().lease());
    }

    @Test
    void maintenanceInvocationFailureMustNotBeForgedIntoDrainedProof() throws Exception {
        List<String> events = new ArrayList<>();
        IllegalStateException protocolFailure = new IllegalStateException("invalid outcome");
        PaimonMaintenanceAdapter maintenance = mock(PaimonMaintenanceAdapter.class);
        when(maintenance.shutdownAndAwait(
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS)))
                .thenThrow(protocolFailure);
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        new PaimonCompactionRuntime("maintenance-protocol"),
                        () -> events.add("writer"),
                        maintenance,
                        () -> events.add("committer"),
                        () -> events.add("io"),
                        () -> events.add("spill"),
                        () -> {
                            events.add("lease");
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(
                        proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));

        PaimonWriteResourceLifecycle.CloseOutcome outcome =
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);

        assertEquals(
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED,
                outcome.state());
        assertSame(protocolFailure, outcome.failure());
        assertEquals(Collections.singletonList("writer"), events);
        assertEquals(
                PaimonWriteCloseModel.DelegateCloseStatus.NOT_ATTEMPTED,
                lifecycle.closeSnapshot().committer());
        assertEquals(
                PaimonWriteCloseModel.DelegateCloseStatus.NOT_ATTEMPTED,
                lifecycle.closeSnapshot().io());
    }

    @Test
    void ddlDeadlineMustDeferAndExactStopJoinMustResumeSamePhase() throws Exception {
        AtomicInteger awaits = new AtomicInteger();
        PaimonCompactionRuntime runtime =
                new PaimonCompactionRuntime(
                        new ScheduledThreadPoolExecutor(1),
                        System::nanoTime,
                        (executor, timeoutNanos) -> awaits.incrementAndGet() > 1);
        PaimonWriteResourceLifecycle lifecycle = emptyLifecycle("generation", runtime);
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(
                        proof("generation", PaimonWriteCloseModel.InitiatingReason.DDL));

        PaimonWriteResourceLifecycle.CloseOutcome deferred =
                lifecycle.awaitAndFinish(
                        operation, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(10L));
        assertEquals(
                PaimonWriteCloseModel.CloseState.CLOSE_DEFERRED_TERMINATION,
                deferred.state());
        assertFalse(deferred.terminal());

        PaimonWriteResourceLifecycle.CloseOperation joined =
                lifecycle.beginClose(
                        proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));
        assertSame(operation, joined);
        assertEquals(
                PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                lifecycle.awaitAndFinish(joined, Long.MAX_VALUE).state());
        assertEquals(2, awaits.get());
    }

    @Test
    void stopJoinBeforeDdlTimeoutTransitionMustKeepOperationActive() throws Exception {
        CountDownLatch awaitEntered = new CountDownLatch(1);
        CountDownLatch releaseTimeout = new CountDownLatch(1);
        AtomicInteger awaits = new AtomicInteger();
        PaimonCompactionRuntime runtime =
                new PaimonCompactionRuntime(
                        new ScheduledThreadPoolExecutor(1),
                        System::nanoTime,
                        (executor, timeoutNanos) -> {
                            if (awaits.incrementAndGet() == 1) {
                                awaitEntered.countDown();
                                releaseTimeout.await();
                                return false;
                            }
                            return true;
                        });
        PaimonWriteResourceLifecycle lifecycle = emptyLifecycle("generation", runtime);
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(
                        proof("generation", PaimonWriteCloseModel.InitiatingReason.DDL));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<PaimonWriteResourceLifecycle.CloseOutcome> ddl =
                    executor.submit(() -> lifecycle.awaitAndFinish(operation, Long.MAX_VALUE));
            assertTrue(awaitEntered.await(10L, TimeUnit.SECONDS));

            assertSame(
                    operation,
                    lifecycle.beginClose(
                            proof(
                                    "generation",
                                    PaimonWriteCloseModel.InitiatingReason.STOP)));
            releaseTimeout.countDown();

            assertEquals(
                    PaimonWriteCloseModel.CloseState.WAITING_COMPACTION,
                    ddl.get(10L, TimeUnit.SECONDS).state());
            assertEquals(
                    PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                    lifecycle.awaitAndFinish(operation, Long.MAX_VALUE).state());
        } finally {
            releaseTimeout.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentJoinMustHonorDeadlineWhileShutdownInitiatorIsBlocked() throws Exception {
        BlockingShutdownExecutor rawExecutor = new BlockingShutdownExecutor();
        PaimonCompactionRuntime runtime =
                new PaimonCompactionRuntime(
                        rawExecutor,
                        System::nanoTime,
                        (executor, timeoutNanos) ->
                                executor.awaitTermination(
                                        timeoutNanos, TimeUnit.NANOSECONDS));
        PaimonWriteResourceLifecycle lifecycle = emptyLifecycle("generation", runtime);
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(
                        proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<PaimonWriteResourceLifecycle.CloseOutcome> initiator =
                    executor.submit(() -> lifecycle.awaitAndFinish(operation, Long.MAX_VALUE));
            assertTrue(rawExecutor.shutdownEntered.await(10L, TimeUnit.SECONDS));

            Future<PaimonWriteResourceLifecycle.CloseOutcome> joining =
                    executor.submit(
                            () ->
                                    lifecycle.awaitAndFinish(
                                            operation,
                                            System.nanoTime()
                                                    + TimeUnit.MILLISECONDS.toNanos(20L)));
            PaimonWriteResourceLifecycle.CloseOutcome timedOut =
                    joining.get(2L, TimeUnit.SECONDS);
            assertEquals(
                    PaimonWriteCloseModel.CloseState.WAITING_COMPACTION,
                    timedOut.state());

            rawExecutor.releaseShutdown.countDown();
            assertEquals(
                    PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                    initiator.get(10L, TimeUnit.SECONDS).state());
        } finally {
            rawExecutor.releaseShutdown.countDown();
            executor.shutdownNow();
            rawExecutor.shutdownNow();
        }
    }

    @Test
    void concurrentJoinMustPropagateExactInterruptWhileShutdownInitiatorIsBlocked()
            throws Exception {
        BlockingShutdownExecutor rawExecutor = new BlockingShutdownExecutor();
        PaimonCompactionRuntime runtime =
                new PaimonCompactionRuntime(
                        rawExecutor,
                        System::nanoTime,
                        (executor, timeoutNanos) ->
                                executor.awaitTermination(
                                        timeoutNanos, TimeUnit.NANOSECONDS));
        PaimonWriteResourceLifecycle lifecycle = emptyLifecycle("generation", runtime);
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(
                        proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<InterruptedException> observed = new AtomicReference<>();
        AtomicReference<Throwable> unexpected = new AtomicReference<>();
        try {
            Future<PaimonWriteResourceLifecycle.CloseOutcome> initiator =
                    executor.submit(() -> lifecycle.awaitAndFinish(operation, Long.MAX_VALUE));
            assertTrue(rawExecutor.shutdownEntered.await(10L, TimeUnit.SECONDS));

            Thread joiner =
                    new Thread(
                            () -> {
                                try {
                                    lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);
                                    unexpected.set(
                                            new AssertionError(
                                                    "interrupted shutdown join unexpectedly completed"));
                                } catch (InterruptedException interrupted) {
                                    observed.set(interrupted);
                                } catch (Throwable failure) {
                                    unexpected.set(failure);
                                }
                            },
                            "paimon-shutdown-joiner");
            joiner.start();
            awaitThreadWaiting(joiner);
            joiner.interrupt();
            joiner.join(TimeUnit.SECONDS.toMillis(10L));

            assertFalse(joiner.isAlive());
            assertEquals(null, unexpected.get());
            assertTrue(observed.get() != null);
            rawExecutor.releaseShutdown.countDown();
            assertEquals(
                    PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                    initiator.get(10L, TimeUnit.SECONDS).state());
        } finally {
            rawExecutor.releaseShutdown.countDown();
            executor.shutdownNow();
            rawExecutor.shutdownNow();
        }
    }

    @Test
    void factoryNonTerminationAndIoFailureMustUseFactoryRetainedState() throws Exception {
        AtomicInteger dependencyCloses = new AtomicInteger();
        PaimonCompactionRuntime nonTerminating =
                new PaimonCompactionRuntime(
                        new ScheduledThreadPoolExecutor(1),
                        System::nanoTime,
                        (executor, timeoutNanos) -> false);
        PaimonWriteResourceLifecycle timeoutLifecycle =
                lifecycle(
                        "timeout",
                        nonTerminating,
                        dependencyCloses::incrementAndGet,
                        maintenance(new ArrayList<>(), success()),
                        dependencyCloses::incrementAndGet,
                        dependencyCloses::incrementAndGet,
                        dependencyCloses::incrementAndGet,
                        () -> {
                            dependencyCloses.incrementAndGet();
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation timeoutOperation =
                timeoutLifecycle.beginClose(
                        proof("timeout", PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK));

        assertEquals(
                PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                timeoutLifecycle
                        .awaitAndFinish(timeoutOperation, System.nanoTime() + 1L)
                        .state());
        assertEquals(0, dependencyCloses.get());

        IOException ioFailure = new IOException("io");
        PaimonWriteResourceLifecycle ioLifecycle =
                lifecycle(
                        "io",
                        new PaimonCompactionRuntime("factory-io"),
                        () -> {},
                        maintenance(new ArrayList<>(), success()),
                        () -> {},
                        () -> {
                            throw ioFailure;
                        },
                        () -> {},
                        () -> true);
        PaimonWriteResourceLifecycle.CloseOperation ioOperation =
                ioLifecycle.beginClose(
                        proof("io", PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK));
        PaimonWriteResourceLifecycle.CloseOutcome ioOutcome =
                ioLifecycle.awaitAndFinish(ioOperation, Long.MAX_VALUE);
        assertEquals(
                PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                ioOutcome.state());
        assertSame(ioFailure, ioOutcome.failure());
    }

    @Test
    void factoryInterruptMustRetainOperationRestoreFlagAndPropagateOriginal()
            throws Exception {
        InterruptedException interruption = new InterruptedException("factory interrupted");
        PaimonCompactionRuntime runtime =
                new PaimonCompactionRuntime(
                        new ScheduledThreadPoolExecutor(1),
                        System::nanoTime,
                        (executor, timeoutNanos) -> {
                            throw interruption;
                        });
        PaimonWriteResourceLifecycle lifecycle = emptyLifecycle("generation", runtime);
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(
                        proof(
                                "generation",
                                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK));
        try {
            InterruptedException thrown =
                    assertThrows(
                            InterruptedException.class,
                            () -> lifecycle.awaitAndFinish(operation, Long.MAX_VALUE));
            assertSame(interruption, thrown);
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(
                    PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                    operation.state());
            assertSame(interruption, operation.terminalFailure());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void maintenanceInterruptMustPropagateExactObjectForDdlAndFactory() throws Exception {
        assertMaintenanceInterruption(
                PaimonWriteCloseModel.InitiatingReason.DDL,
                PaimonWriteCloseModel.CloseState.CLOSE_DEFERRED_TERMINATION);
        assertMaintenanceInterruption(
                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED);
    }

    @Test
    void closeActionInterruptionsMustPublishRetainedThenPropagateExactObject()
            throws Exception {
        assertCloseActionInterruption(
                PaimonWriteCloseModel.InitiatingReason.DDL,
                InterruptStage.WRITER,
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED);
        assertCloseActionInterruption(
                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                InterruptStage.WRITER,
                PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED);
        assertCloseActionInterruption(
                PaimonWriteCloseModel.InitiatingReason.DDL,
                InterruptStage.COMMITTER,
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED);
        assertCloseActionInterruption(
                PaimonWriteCloseModel.InitiatingReason.STOP,
                InterruptStage.IO,
                PaimonWriteCloseModel.CloseState.IO_CLOSE_FAILED_RETAINED);
        assertCloseActionInterruption(
                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                InterruptStage.IO,
                PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED);
        assertCloseActionInterruption(
                PaimonWriteCloseModel.InitiatingReason.DDL,
                InterruptStage.SPILL,
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED);
        assertCloseActionInterruption(
                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK,
                InterruptStage.LEASE,
                PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED);
    }

    @Test
    void deliveredWriterInterruptionMustNotBeReplayedToStopContinuation()
            throws Exception {
        InterruptedException interruption = new InterruptedException("ddl writer");
        AtomicInteger maintenanceAttempts = new AtomicInteger();
        AtomicInteger committerCloses = new AtomicInteger();
        PaimonMaintenanceAdapter maintenance = mock(PaimonMaintenanceAdapter.class);
        when(maintenance.shutdownAndAwait(
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS)))
                .thenAnswer(
                        ignored ->
                                maintenanceAttempts.incrementAndGet() == 1
                                        ? waiting()
                                        : success());
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        new PaimonCompactionRuntime("interrupt-continuation"),
                        () -> {
                            throw interruption;
                        },
                        maintenance,
                        committerCloses::incrementAndGet,
                        () -> {},
                        () -> {},
                        () -> true);
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(
                        proof("generation", PaimonWriteCloseModel.InitiatingReason.DDL));
        try {
            assertSame(
                    interruption,
                    assertThrows(
                            InterruptedException.class,
                            () -> lifecycle.awaitAndFinish(operation, Long.MAX_VALUE)));
            assertEquals(
                    PaimonWriteCloseModel.CloseState.CLOSE_DEFERRED_TERMINATION,
                    operation.state());
        } finally {
            Thread.interrupted();
        }

        assertSame(
                operation,
                lifecycle.beginClose(
                        proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP)));
        PaimonWriteResourceLifecycle.CloseOutcome completed =
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);

        assertEquals(
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED,
                completed.state());
        assertSame(interruption, completed.failure());
        assertFalse(Thread.currentThread().isInterrupted());
        assertEquals(2, maintenanceAttempts.get());
        assertEquals(1, committerCloses.get());
    }

    @Test
    void factoryMaintenanceWaitMustKeepWriterFailurePrimary() throws Exception {
        IOException writerFailure = new IOException("factory writer");
        PaimonWriteResourceLifecycle timeoutLifecycle =
                lifecycle(
                        "timeout",
                        new PaimonCompactionRuntime("factory-writer-timeout"),
                        () -> {
                            throw writerFailure;
                        },
                        maintenance(new ArrayList<>(), waiting()),
                        () -> {},
                        () -> {},
                        () -> {},
                        () -> true);
        PaimonWriteResourceLifecycle.CloseOperation timeoutOperation =
                timeoutLifecycle.beginClose(
                        proof(
                                "timeout",
                                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK));

        PaimonWriteResourceLifecycle.CloseOutcome timeout =
                timeoutLifecycle.awaitAndFinish(timeoutOperation, Long.MAX_VALUE);

        assertEquals(
                PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                timeout.state());
        assertSame(writerFailure, timeout.failure());
        assertEquals(1, writerFailure.getSuppressed().length);

        IOException secondWriterFailure = new IOException("factory writer interrupted wait");
        InterruptedException maintenanceInterruption =
                new InterruptedException("factory maintenance");
        PaimonMaintenanceAdapter interruptedMaintenance = mock(PaimonMaintenanceAdapter.class);
        when(interruptedMaintenance.shutdownAndAwait(
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS)))
                .thenAnswer(
                        ignored -> {
                            Thread.currentThread().interrupt();
                            return maintenanceOutcome(
                                    PaimonMaintenanceAdapter.Status.WAITING,
                                    null,
                                    maintenanceInterruption);
                        });
        PaimonWriteResourceLifecycle interruptedLifecycle =
                lifecycle(
                        "interrupted",
                        new PaimonCompactionRuntime("factory-writer-interrupted"),
                        () -> {
                            throw secondWriterFailure;
                        },
                        interruptedMaintenance,
                        () -> {},
                        () -> {},
                        () -> {},
                        () -> true);
        PaimonWriteResourceLifecycle.CloseOperation interruptedOperation =
                interruptedLifecycle.beginClose(
                        proof(
                                "interrupted",
                                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK));
        try {
            assertSame(
                    maintenanceInterruption,
                    assertThrows(
                            InterruptedException.class,
                            () ->
                                    interruptedLifecycle.awaitAndFinish(
                                            interruptedOperation, Long.MAX_VALUE)));
            assertTrue(Thread.currentThread().isInterrupted());
            assertSame(secondWriterFailure, interruptedOperation.terminalFailure());
            assertEquals(
                    Collections.singletonList(maintenanceInterruption),
                    Arrays.asList(secondWriterFailure.getSuppressed()));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void factoryRollbackMustUseAtomicSubmissionSnapshotAtShutdownFence() throws Exception {
        AtomicInteger dependencyCloses = new AtomicInteger();
        PaimonCompactionRuntime runtime = new PaimonCompactionRuntime("factory-fence");
        runtime.executor().submit(() -> {}).get(10L, TimeUnit.SECONDS);
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        runtime,
                        dependencyCloses::incrementAndGet,
                        maintenance(new ArrayList<>(), success()),
                        dependencyCloses::incrementAndGet,
                        dependencyCloses::incrementAndGet,
                        dependencyCloses::incrementAndGet,
                        () -> {
                            dependencyCloses.incrementAndGet();
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(
                        proof(
                                "generation",
                                PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK));

        PaimonWriteResourceLifecycle.CloseOutcome outcome =
                lifecycle.awaitAndFinish(operation, Long.MAX_VALUE);

        assertEquals(
                PaimonWriteCloseModel.CloseState.FACTORY_ROLLBACK_RETAINED,
                outcome.state());
        assertTrue(outcome.failure().getMessage().contains("pre-fence compaction submission"));
        assertEquals(0, dependencyCloses.get());
    }

    @Test
    void spillAndLeaseFailureMustBeStickyAndNeverRetryUnknownPartialClose()
            throws Exception {
        AtomicInteger spillAttempts = new AtomicInteger();
        AtomicInteger leaseAttempts = new AtomicInteger();
        IOException spillFailure = new IOException("spill");
        PaimonWriteResourceLifecycle spillLifecycle =
                lifecycle(
                        "spill",
                        new PaimonCompactionRuntime("spill-failure"),
                        () -> {},
                        maintenance(new ArrayList<>(), success()),
                        () -> {},
                        () -> {},
                        () -> {
                            spillAttempts.incrementAndGet();
                            throw spillFailure;
                        },
                        () -> {
                            leaseAttempts.incrementAndGet();
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation spillOperation =
                spillLifecycle.beginClose(
                        proof("spill", PaimonWriteCloseModel.InitiatingReason.STOP));
        assertEquals(
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED,
                spillLifecycle.awaitAndFinish(spillOperation, Long.MAX_VALUE).state());
        spillLifecycle.awaitAndFinish(spillOperation, Long.MAX_VALUE);
        assertEquals(1, spillAttempts.get());
        assertEquals(0, leaseAttempts.get());

        PaimonWriteResourceLifecycle leaseLifecycle =
                lifecycle(
                        "lease",
                        new PaimonCompactionRuntime("lease-failure"),
                        () -> {},
                        maintenance(new ArrayList<>(), success()),
                        () -> {},
                        () -> {},
                        () -> {},
                        () -> {
                            leaseAttempts.incrementAndGet();
                            return false;
                        });
        PaimonWriteResourceLifecycle.CloseOperation leaseOperation =
                leaseLifecycle.beginClose(
                        proof("lease", PaimonWriteCloseModel.InitiatingReason.STOP));
        assertEquals(
                PaimonWriteCloseModel.CloseState.DEPENDENCY_CLOSE_FAILED_RETAINED,
                leaseLifecycle.awaitAndFinish(leaseOperation, Long.MAX_VALUE).state());
        leaseLifecycle.awaitAndFinish(leaseOperation, Long.MAX_VALUE);
        assertEquals(1, leaseAttempts.get());
    }

    @Test
    void cumulativeDeadlineMustStopBetweenIoSpillAndLeasePhases() throws Exception {
        CountDownLatch ioEntered = new CountDownLatch(1);
        CountDownLatch releaseIo = new CountDownLatch(1);
        List<String> ioEvents = Collections.synchronizedList(new ArrayList<>());
        PaimonWriteResourceLifecycle ioLifecycle =
                lifecycle(
                        "io-deadline",
                        new PaimonCompactionRuntime("io-deadline"),
                        () -> ioEvents.add("writer"),
                        maintenance(ioEvents, success()),
                        () -> ioEvents.add("committer"),
                        () -> {
                            ioEvents.add("io");
                            ioEntered.countDown();
                            releaseIo.await();
                        },
                        () -> ioEvents.add("spill"),
                        () -> {
                            ioEvents.add("lease");
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation ioOperation =
                ioLifecycle.beginClose(
                        proof("io-deadline", PaimonWriteCloseModel.InitiatingReason.STOP));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            long ioDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200L);
            Future<PaimonWriteResourceLifecycle.CloseOutcome> ioClose =
                    executor.submit(() -> ioLifecycle.awaitAndFinish(ioOperation, ioDeadline));
            assertTrue(ioEntered.await(10L, TimeUnit.SECONDS));
            awaitPast(ioDeadline);
            releaseIo.countDown();
            assertEquals(
                    PaimonWriteCloseModel.CloseState.WAITING_COMMIT_MAINTENANCE,
                    ioClose.get(10L, TimeUnit.SECONDS).state());
            assertEquals(
                    Arrays.asList("writer", "maintenance", "committer", "io"),
                    ioEvents);
            assertEquals(
                    PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                    ioLifecycle.awaitAndFinish(ioOperation, Long.MAX_VALUE).state());
            assertEquals(
                    Arrays.asList(
                            "writer", "maintenance", "committer", "io", "spill", "lease"),
                    ioEvents);
        } finally {
            releaseIo.countDown();
            executor.shutdownNow();
        }

        CountDownLatch spillEntered = new CountDownLatch(1);
        CountDownLatch releaseSpill = new CountDownLatch(1);
        List<String> spillEvents = Collections.synchronizedList(new ArrayList<>());
        PaimonWriteResourceLifecycle spillLifecycle =
                lifecycle(
                        "spill-deadline",
                        new PaimonCompactionRuntime("spill-deadline"),
                        () -> spillEvents.add("writer"),
                        maintenance(spillEvents, success()),
                        () -> spillEvents.add("committer"),
                        () -> spillEvents.add("io"),
                        () -> {
                            spillEvents.add("spill");
                            spillEntered.countDown();
                            releaseSpill.await();
                        },
                        () -> {
                            spillEvents.add("lease");
                            return true;
                        });
        PaimonWriteResourceLifecycle.CloseOperation spillOperation =
                spillLifecycle.beginClose(
                        proof(
                                "spill-deadline",
                                PaimonWriteCloseModel.InitiatingReason.STOP));
        ExecutorService spillExecutor = Executors.newSingleThreadExecutor();
        try {
            long spillDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200L);
            Future<PaimonWriteResourceLifecycle.CloseOutcome> spillClose =
                    spillExecutor.submit(
                            () -> spillLifecycle.awaitAndFinish(spillOperation, spillDeadline));
            assertTrue(spillEntered.await(10L, TimeUnit.SECONDS));
            awaitPast(spillDeadline);
            releaseSpill.countDown();
            assertEquals(
                    PaimonWriteCloseModel.CloseState.WAITING_COMMIT_MAINTENANCE,
                    spillClose.get(10L, TimeUnit.SECONDS).state());
            assertEquals(
                    Arrays.asList("writer", "maintenance", "committer", "io", "spill"),
                    spillEvents);
            assertEquals(
                    PaimonWriteResourceLifecycle.StepState.NOT_STARTED,
                    spillLifecycle.closeSnapshot().lease());
            assertEquals(
                    PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                    spillLifecycle.awaitAndFinish(spillOperation, Long.MAX_VALUE).state());
            assertEquals(
                    Arrays.asList(
                            "writer", "maintenance", "committer", "io", "spill", "lease"),
                    spillEvents);
        } finally {
            releaseSpill.countDown();
            spillExecutor.shutdownNow();
        }
    }

    @Test
    void closeSnapshotMustNotBlockBehindExternalWriterClose() throws Exception {
        CountDownLatch writerEntered = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        AtomicInteger writerCloses = new AtomicInteger();
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation",
                        new PaimonCompactionRuntime("snapshot"),
                        () -> {
                            writerCloses.incrementAndGet();
                            writerEntered.countDown();
                            releaseWriter.await();
                        },
                        maintenance(new ArrayList<>(), success()),
                        () -> {},
                        () -> {},
                        () -> {},
                        () -> true);
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(proof("generation", PaimonWriteCloseModel.InitiatingReason.STOP));
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<PaimonWriteResourceLifecycle.CloseOutcome> first =
                    executor.submit(() -> lifecycle.awaitAndFinish(operation, Long.MAX_VALUE));
            assertTrue(writerEntered.await(10L, TimeUnit.SECONDS));
            Future<PaimonWriteResourceLifecycle.CloseOutcome> joining =
                    executor.submit(() -> lifecycle.awaitAndFinish(operation, Long.MAX_VALUE));

            Future<PaimonWriteResourceLifecycle.CloseSnapshot> snapshot =
                    executor.submit(lifecycle::closeSnapshot);
            assertEquals(
                    PaimonWriteCloseModel.DelegateCloseStatus.IN_PROGRESS,
                    snapshot.get(2L, TimeUnit.SECONDS).writer());

            releaseWriter.countDown();
            assertEquals(
                    PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                    first.get(10L, TimeUnit.SECONDS).state());
            assertEquals(
                    PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                    joining.get(10L, TimeUnit.SECONDS).state());
            assertEquals(1, writerCloses.get());
        } finally {
            releaseWriter.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void wrongGenerationProofAndForeignOperationMustBeRejected() throws Exception {
        PaimonCompactionRuntime firstRuntime = new PaimonCompactionRuntime("first");
        PaimonCompactionRuntime secondRuntime = new PaimonCompactionRuntime("second");
        PaimonWriteResourceLifecycle first = emptyLifecycle("first", firstRuntime);
        PaimonWriteResourceLifecycle second = emptyLifecycle("second", secondRuntime);

        assertThrows(
                IllegalArgumentException.class,
                () -> first.beginClose(proof("wrong", PaimonWriteCloseModel.InitiatingReason.STOP)));
        PaimonWriteResourceLifecycle.CloseOperation foreign =
                second.beginClose(proof("second", PaimonWriteCloseModel.InitiatingReason.STOP));
        assertThrows(
                IllegalArgumentException.class,
                () -> first.awaitAndFinish(foreign, Long.MAX_VALUE));

        PaimonWriteResourceLifecycle.CloseOperation firstOperation =
                first.beginClose(proof("first", PaimonWriteCloseModel.InitiatingReason.STOP));
        assertEquals(
                PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                first.awaitAndFinish(firstOperation, Long.MAX_VALUE).state());
        assertEquals(
                PaimonWriteCloseModel.CloseState.CLOSED_SUCCESS,
                second.awaitAndFinish(foreign, Long.MAX_VALUE).state());
    }

    @Test
    void joinMatrixAndTransitionAuthorityMustBeStructurallyClosed() {
        Object capability = new Object();
        PaimonWriteResourceLifecycle stop =
                emptyLifecycle(capability, new PaimonCompactionRuntime("join-stop"));
        stop.beginClose(proof(capability, PaimonWriteCloseModel.InitiatingReason.STOP));
        assertThrows(
                IllegalStateException.class,
                () ->
                        stop.beginClose(
                                proof(capability, PaimonWriteCloseModel.InitiatingReason.DDL)));

        Object ddlCapability = new Object();
        PaimonWriteResourceLifecycle ddl =
                emptyLifecycle(ddlCapability, new PaimonCompactionRuntime("join-ddl"));
        ddl.beginClose(proof(ddlCapability, PaimonWriteCloseModel.InitiatingReason.DDL));
        assertThrows(
                IllegalStateException.class,
                () ->
                        ddl.beginClose(
                                proof(
                                        ddlCapability,
                                        PaimonWriteCloseModel.InitiatingReason.DDL)));

        Object factoryCapability = new Object();
        PaimonWriteResourceLifecycle factory =
                emptyLifecycle(factoryCapability, new PaimonCompactionRuntime("join-factory"));
        factory.beginClose(
                proof(
                        factoryCapability,
                        PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK));
        assertThrows(
                IllegalStateException.class,
                () ->
                        factory.beginClose(
                                proof(
                                        factoryCapability,
                                        PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK)));

        for (Constructor<?> constructor :
                PaimonWriteCloseModel.QuiescenceProof.class.getDeclaredConstructors()) {
            assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        }
        for (Method method : PaimonWriteResourceLifecycle.CloseOperation.class.getDeclaredMethods()) {
            if (method.getName().startsWith("record")
                    || method.getName().startsWith("retain")
                    || method.getName().startsWith("defer")
                    || method.getName().startsWith("resume")
                    || method.getName().startsWith("joinFor")) {
                assertTrue(Modifier.isPrivate(method.getModifiers()), method::toString);
            }
            if (method.getName().equals("joinForStop")
                    || method.getName().equals("transitionOnWaiting")
                    || method.getName().equals("transitionOnInterruptedWait")) {
                assertTrue(Modifier.isSynchronized(method.getModifiers()), method::toString);
            }
        }
    }

    private static PaimonWriteResourceLifecycle emptyLifecycle(
            Object generation, PaimonCompactionRuntime runtime) {
        return lifecycle(
                generation,
                runtime,
                () -> {},
                maintenance(new ArrayList<>(), success()),
                () -> {},
                () -> {},
                () -> {},
                () -> true);
    }

    private static PaimonWriteResourceLifecycle lifecycle(
            Object generation,
            PaimonCompactionRuntime compaction,
            PaimonWriteCloseModel.CloseAction writer,
            PaimonMaintenanceAdapter maintenance,
            PaimonWriteCloseModel.CloseAction committer,
            PaimonWriteCloseModel.CloseAction io,
            PaimonWriteCloseModel.CloseAction spill,
            PaimonWriteResourceLifecycle.LeaseReleaseAction lease) {
        return new PaimonWriteResourceLifecycle(
                generation, compaction, writer, maintenance, committer, io, spill, lease);
    }

    private static PaimonMaintenanceAdapter maintenance(
            List<String> events, PaimonMaintenanceAdapter.Outcome outcome) {
        PaimonMaintenanceAdapter adapter = mock(PaimonMaintenanceAdapter.class);
        when(adapter.shutdownAndAwait(
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS)))
                .thenAnswer(
                        ignored -> {
                            events.add("maintenance");
                            return outcome;
                        });
        return adapter;
    }

    private static PaimonMaintenanceAdapter.Outcome success() {
        return maintenanceOutcome(PaimonMaintenanceAdapter.Status.SUCCESS, null);
    }

    private static PaimonMaintenanceAdapter.Outcome waiting() {
        return maintenanceOutcome(PaimonMaintenanceAdapter.Status.WAITING, null);
    }

    private static PaimonMaintenanceAdapter.Outcome failedDrained(Throwable failure) {
        return maintenanceOutcome(PaimonMaintenanceAdapter.Status.FAILED_DRAINED, failure);
    }

    private static PaimonMaintenanceAdapter.Outcome maintenanceOutcome(
            PaimonMaintenanceAdapter.Status status, Throwable failure) {
        return maintenanceOutcome(status, failure, null);
    }

    private static PaimonMaintenanceAdapter.Outcome maintenanceOutcome(
            PaimonMaintenanceAdapter.Status status,
            Throwable failure,
            InterruptedException interruption) {
        PaimonMaintenanceAdapter.Outcome outcome = mock(PaimonMaintenanceAdapter.Outcome.class);
        when(outcome.status()).thenReturn(status);
        when(outcome.failure()).thenReturn(failure);
        when(outcome.interruption()).thenReturn(interruption);
        return outcome;
    }

    private static void assertMaintenanceInterruption(
            PaimonWriteCloseModel.InitiatingReason reason,
            PaimonWriteCloseModel.CloseState expectedState)
            throws Exception {
        InterruptedException interruption =
                new InterruptedException("maintenance " + reason);
        PaimonMaintenanceAdapter maintenance = mock(PaimonMaintenanceAdapter.class);
        when(maintenance.shutdownAndAwait(
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS)))
                .thenAnswer(
                        ignored -> {
                            Thread.currentThread().interrupt();
                            return maintenanceOutcome(
                                    PaimonMaintenanceAdapter.Status.WAITING,
                                    null,
                                    interruption);
                        });
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation-" + reason,
                        new PaimonCompactionRuntime("maintenance-" + reason),
                        () -> {},
                        maintenance,
                        () -> {},
                        () -> {},
                        () -> {},
                        () -> true);
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(proof("generation-" + reason, reason));
        try {
            InterruptedException thrown =
                    assertThrows(
                            InterruptedException.class,
                            () -> lifecycle.awaitAndFinish(operation, Long.MAX_VALUE));
            assertSame(interruption, thrown);
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(expectedState, operation.state());
            if (reason == PaimonWriteCloseModel.InitiatingReason.FACTORY_ROLLBACK) {
                assertSame(interruption, operation.terminalFailure());
            }
        } finally {
            Thread.interrupted();
        }
    }

    private static void assertCloseActionInterruption(
            PaimonWriteCloseModel.InitiatingReason reason,
            InterruptStage interruptedStage,
            PaimonWriteCloseModel.CloseState expectedState)
            throws Exception {
        InterruptedException interruption =
                new InterruptedException(reason + " " + interruptedStage);
        List<String> events = new ArrayList<>();
        PaimonWriteResourceLifecycle lifecycle =
                lifecycle(
                        "generation-" + reason + '-' + interruptedStage,
                        new PaimonCompactionRuntime(
                                "action-interrupt-" + reason + '-' + interruptedStage),
                        closeAction(events, "writer", InterruptStage.WRITER, interruptedStage, interruption),
                        maintenance(events, success()),
                        closeAction(
                                events,
                                "committer",
                                InterruptStage.COMMITTER,
                                interruptedStage,
                                interruption),
                        closeAction(events, "io", InterruptStage.IO, interruptedStage, interruption),
                        closeAction(
                                events,
                                "spill",
                                InterruptStage.SPILL,
                                interruptedStage,
                                interruption),
                        () -> {
                            events.add("lease");
                            if (interruptedStage == InterruptStage.LEASE) {
                                throw interruption;
                            }
                            return true;
                        });
        Object capability = "generation-" + reason + '-' + interruptedStage;
        PaimonWriteResourceLifecycle.CloseOperation operation =
                lifecycle.beginClose(proof(capability, reason));
        try {
            InterruptedException thrown =
                    assertThrows(
                            InterruptedException.class,
                            () -> lifecycle.awaitAndFinish(operation, Long.MAX_VALUE));
            assertSame(interruption, thrown);
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(expectedState, operation.state());
            assertSame(interruption, operation.terminalFailure());
            if (interruptedStage == InterruptStage.WRITER) {
                assertEquals(
                        Arrays.asList("writer", "maintenance", "committer"),
                        events,
                        "writer interruption must still execute the mandatory safety tail");
            }
        } finally {
            Thread.interrupted();
        }
    }

    private static PaimonWriteCloseModel.CloseAction closeAction(
            List<String> events,
            String event,
            InterruptStage actionStage,
            InterruptStage interruptedStage,
            InterruptedException interruption) {
        return () -> {
            events.add(event);
            if (actionStage == interruptedStage) {
                throw interruption;
            }
        };
    }

    private static void awaitThreadWaiting(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING) {
            if (!thread.isAlive() || System.nanoTime() >= deadline) {
                throw new AssertionError("thread did not enter a shutdown join wait");
            }
            Thread.yield();
        }
    }

    private static void awaitPast(long absoluteDeadlineNanos) {
        while (System.nanoTime() <= absoluteDeadlineNanos) {
            Thread.yield();
        }
    }

    private static final class BlockingShutdownExecutor extends ScheduledThreadPoolExecutor {
        private final CountDownLatch shutdownEntered = new CountDownLatch(1);
        private final CountDownLatch releaseShutdown = new CountDownLatch(1);

        private BlockingShutdownExecutor() {
            super(1);
        }

        @Override
        public void shutdown() {
            shutdownEntered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    releaseShutdown.await();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            super.shutdown();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private enum InterruptStage {
        WRITER,
        COMMITTER,
        IO,
        SPILL,
        LEASE
    }

    private static PaimonWriteCloseModel.QuiescenceProof proof(
            Object generation, PaimonWriteCloseModel.InitiatingReason reason) {
        PaimonWriteCloseModel.QuiescenceProof proof =
                mock(PaimonWriteCloseModel.QuiescenceProof.class);
        when(proof.matchesGenerationCapability(generation)).thenReturn(true);
        when(proof.initiatingReason()).thenReturn(reason);
        return proof;
    }
}
