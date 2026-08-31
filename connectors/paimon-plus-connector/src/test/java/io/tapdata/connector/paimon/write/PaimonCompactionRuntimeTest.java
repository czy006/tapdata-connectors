package io.tapdata.connector.paimon.write;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonCompactionRuntimeTest {

    @Test
    void blockedWorkerMustRetainDependenciesUntilActualExecutorTermination() throws Exception {
        try (PaimonCompactionExecutorFixture fixture =
                new PaimonCompactionExecutorFixture()) {
            Future<?> worker = fixture.startInterruptIgnoringWorker();
            fixture.awaitWorkerEntered();

            fixture.runtime.beginShutdown();
            assertFalse(
                    fixture.closeDependenciesAfterTerminationProof(
                            PaimonCompactionExecutorFixture.expiredDeadline()));

            assertEquals(0, fixture.workerInterrupts.get());
            assertFalse(worker.isDone());
            assertFalse(fixture.runtime.terminated());
            assertEquals(0, fixture.writerCloseCount.get());
            assertEquals(0, fixture.committerCloseCount.get());
            assertEquals(0, fixture.ioCloseCount.get());
            assertThrows(
                    RejectedExecutionException.class,
                    () -> fixture.runtime.executor().submit(() -> {}));
            assertThrows(
                    RejectedExecutionException.class,
                    () -> fixture.runtime.executor().invokeAll(Collections.emptyList()));
            assertThrows(
                    RejectedExecutionException.class,
                    () ->
                            fixture.runtime
                                    .executor()
                                    .schedule(() -> {}, 0L, TimeUnit.NANOSECONDS));

            fixture.releaseAndAwaitWorkerReturn();
            worker.get(10L, TimeUnit.SECONDS);

            assertTrue(
                    fixture.closeDependenciesAfterTerminationProof(
                            PaimonCompactionExecutorFixture.deadlineAfterSeconds(10L)));
            assertTrue(fixture.runtime.terminated());
            assertEquals(1, fixture.writerCloseCount.get());
            assertEquals(1, fixture.committerCloseCount.get());
            assertEquals(1, fixture.ioCloseCount.get());
            assertTrue(
                    fixture.workerReturnOrder.get() < fixture.terminationPublicationOrder.get(),
                    "TERMINATED publication must follow the worker's actual return");
        }
    }

    @Test
    void shutdownMustBeIdempotentGracefulAndExposeNoHandlelessForcePath() throws Exception {
        try (PaimonCompactionExecutorFixture fixture =
                new PaimonCompactionExecutorFixture()) {
            Future<?> worker = fixture.startInterruptIgnoringWorker();
            fixture.awaitWorkerEntered();

            fixture.runtime.beginShutdown();
            fixture.runtime.beginShutdown();

            assertEquals(1, fixture.rawExecutor.shutdownCalls());
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> fixture.runtime.executor().shutdownNow());
            assertEquals(0, fixture.rawExecutor.shutdownNowCalls());
            assertEquals(0, fixture.workerInterrupts.get());
            assertFalse(worker.isDone());

            fixture.releaseAndAwaitWorkerReturn();
            assertTrue(
                    fixture.runtime.awaitTermination(
                            PaimonCompactionExecutorFixture.deadlineAfterSeconds(10L)));
            assertFalse(worker.isCancelled());
            assertNull(worker.get(10L, TimeUnit.SECONDS));
        }
    }

    @Test
    void shutdownFenceMustAtomicallyDistinguishPreFenceAndRejectedPostFenceSubmission()
            throws Exception {
        PaimonCompactionRuntime clean = new PaimonCompactionRuntime("factory-clean");
        assertTrue(clean.beginShutdown());
        assertThrows(
                RejectedExecutionException.class,
                () -> clean.executor().execute(() -> {}));
        assertTrue(
                clean.beginShutdown(),
                "a rejected post-fence attempt must not rewrite the shutdown snapshot");
        assertTrue(
                clean.awaitTermination(
                        PaimonCompactionExecutorFixture.deadlineAfterSeconds(10L)));

        PaimonCompactionRuntime submitted = new PaimonCompactionRuntime("factory-submitted");
        try {
            submitted.executor().submit(() -> {}).get(10L, TimeUnit.SECONDS);
            assertFalse(
                    submitted.beginShutdown(),
                    "an accepted pre-fence submission must make rollback evidence false");
            assertFalse(submitted.beginShutdown());
            assertTrue(
                    submitted.awaitTermination(
                            PaimonCompactionExecutorFixture.deadlineAfterSeconds(10L)));
        } finally {
            submitted.beginShutdown();
        }
    }

    @Test
    void gracefulShutdownMustPreserveNaturalTaskFailure() throws Exception {
        try (PaimonCompactionExecutorFixture fixture =
                new PaimonCompactionExecutorFixture()) {
            CountDownLatch taskEntered = new CountDownLatch(1);
            CountDownLatch releaseFailure = new CountDownLatch(1);
            IllegalStateException naturalFailure =
                    new IllegalStateException("natural compaction failure");
            Future<?> failedTask =
                    fixture.runtime
                            .executor()
                            .submit(
                                    () -> {
                                        taskEntered.countDown();
                                        try {
                                            releaseFailure.await();
                                        } catch (InterruptedException interrupted) {
                                            throw new AssertionError(
                                                    "graceful shutdown interrupted task",
                                                    interrupted);
                                        }
                                        throw naturalFailure;
                                    });
            assertTrue(taskEntered.await(10L, TimeUnit.SECONDS));

            fixture.runtime.beginShutdown();
            releaseFailure.countDown();

            ExecutionException propagated =
                    assertThrows(
                            ExecutionException.class,
                            () -> failedTask.get(10L, TimeUnit.SECONDS));
            assertEquals(naturalFailure, propagated.getCause());
            assertTrue(
                    fixture.runtime.awaitTermination(
                            PaimonCompactionExecutorFixture.deadlineAfterSeconds(10L)));
            assertFalse(failedTask.isCancelled());
        }
    }

    @Test
    void cancelledFutureMustNotBeMistakenForWorkerTermination() throws Exception {
        try (PaimonCompactionExecutorFixture fixture =
                new PaimonCompactionExecutorFixture()) {
            Future<?> worker = fixture.startInterruptIgnoringWorker();
            fixture.awaitWorkerEntered();

            assertTrue(worker.cancel(true));
            assertTrue(worker.isDone());
            assertTrue(worker.isCancelled());
            fixture.runtime.beginShutdown();

            assertFalse(
                    fixture.closeDependenciesAfterTerminationProof(
                            PaimonCompactionExecutorFixture.expiredDeadline()));
            assertFalse(fixture.runtime.terminated());
            assertEquals(0, fixture.writerCloseCount.get());
            assertEquals(0, fixture.ioCloseCount.get());

            fixture.releaseAndAwaitWorkerReturn();
            assertTrue(
                    fixture.runtime.awaitTermination(
                            PaimonCompactionExecutorFixture.deadlineAfterSeconds(10L)));
        }
    }

    @Test
    void interruptionMustNotPublishTerminationAndMustRestoreInterruptFlag() throws Exception {
        CountDownLatch awaitEntered = new CountDownLatch(1);
        try (PaimonCompactionExecutorFixture fixture =
                new PaimonCompactionExecutorFixture(awaitEntered)) {
            fixture.startInterruptIgnoringWorker();
            fixture.awaitWorkerEntered();
            fixture.runtime.beginShutdown();
            AtomicBoolean interruptedFlagObserved = new AtomicBoolean();
            AtomicReference<Throwable> unexpected = new AtomicReference<>();

            Thread waiter =
                    new Thread(
                            () -> {
                                try {
                                    fixture.runtime.awaitTermination(
                                            PaimonCompactionExecutorFixture.deadlineAfterSeconds(
                                                    10L));
                                    unexpected.set(
                                            new AssertionError(
                                                    "interrupted await unexpectedly completed"));
                                } catch (InterruptedException expected) {
                                    interruptedFlagObserved.set(Thread.currentThread().isInterrupted());
                                } catch (Throwable failure) {
                                    unexpected.set(failure);
                                }
                            },
                            "compaction-termination-waiter");
            waiter.start();
            assertTrue(awaitEntered.await(10L, TimeUnit.SECONDS));
            waiter.interrupt();
            waiter.join(TimeUnit.SECONDS.toMillis(10L));

            assertFalse(waiter.isAlive());
            assertNull(unexpected.get());
            assertTrue(interruptedFlagObserved.get());
            assertFalse(fixture.runtime.terminated());
            assertEquals(0, fixture.writerCloseCount.get());
            assertEquals(0, fixture.ioCloseCount.get());
        }
    }

    @Test
    void everyAdmissionVariantMustSetExactMonotonicSubmissionObservation() throws Exception {
        List<Admission> admissions =
                Arrays.asList(
                        executor -> executor.execute(() -> {}),
                        executor -> executor.submit(() -> {}),
                        executor -> executor.submit(() -> {}, "result"),
                        executor -> executor.submit(() -> "result"),
                        executor -> executor.invokeAll(Collections.singletonList(() -> "result")),
                        executor ->
                                executor.invokeAll(
                                        Collections.singletonList(() -> "result"),
                                        10L,
                                        TimeUnit.SECONDS),
                        executor ->
                                executor.invokeAny(Collections.singletonList(() -> "result")),
                        executor ->
                                executor.invokeAny(
                                        Collections.singletonList(() -> "result"),
                                        10L,
                                        TimeUnit.SECONDS),
                        executor -> executor.schedule(() -> {}, 0L, TimeUnit.NANOSECONDS),
                        executor -> executor.schedule(() -> "result", 0L, TimeUnit.NANOSECONDS),
                        executor ->
                                executor.scheduleAtFixedRate(
                                                () -> {}, 1L, 1L, TimeUnit.DAYS)
                                        .cancel(false),
                        executor ->
                                executor.scheduleWithFixedDelay(
                                                () -> {}, 1L, 1L, TimeUnit.DAYS)
                                        .cancel(false));

        int index = 0;
        for (Admission admission : admissions) {
            try (PaimonCompactionExecutorFixture fixture =
                    new PaimonCompactionExecutorFixture()) {
                assertFalse(
                        fixture.runtime.submissionObserved(),
                        "fresh runtime must strictly mean no submission");
                admission.admit(fixture.runtime.executor());
                assertTrue(
                        fixture.runtime.submissionObserved(),
                        "admission variant " + index + " was not observed");
            }
            index++;
        }
    }

    @Test
    void defaultRuntimeMustUseLazyDiagnosticDaemonWorker() throws Exception {
        PaimonCompactionRuntime runtime = new PaimonCompactionRuntime("generation-7");
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        try {
            assertFalse(runtime.submissionObserved());
            assertFalse(runtime.workerStartedForDiagnostics());

            Future<?> task = runtime.executor().submit(() -> workerThread.set(Thread.currentThread()));
            task.get(10L, TimeUnit.SECONDS);
            Thread worker = workerThread.get();

            assertNotNull(worker);
            assertTrue(worker.isDaemon());
            assertTrue(worker.getName().startsWith("paimon-compaction-generation-7-thread-"));
            assertTrue(runtime.workerStartedForDiagnostics());
        } finally {
            runtime.beginShutdown();
            assertTrue(
                    runtime.awaitTermination(
                            PaimonCompactionExecutorFixture.deadlineAfterSeconds(10L)));
        }
    }

    @FunctionalInterface
    private interface Admission {
        void admit(ScheduledExecutorService executor) throws Exception;
    }
}
