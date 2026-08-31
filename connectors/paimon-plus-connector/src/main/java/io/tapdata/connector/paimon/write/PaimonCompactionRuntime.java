package io.tapdata.connector.paimon.write;

import org.apache.paimon.utils.ExecutorThreadFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the executor used by one Paimon writer's asynchronous compaction tasks.
 *
 * <p>The exposed executor is deliberately a controlled facade. Task admission is linearized with
 * graceful shutdown, while forceful executor shutdown is not exposed because this runtime does not
 * own Paimon's individual compaction {@link Future} handles. Dependency owners must wait for a
 * positive {@link #awaitTermination(long)} result before closing a writer, committer or IO manager.
 */
final class PaimonCompactionRuntime {

    private static final String THREAD_NAME_PREFIX = "paimon-compaction-";

    private final Object admissionLock = new Object();
    private final ScheduledThreadPoolExecutor ownedExecutor;
    private final ScheduledExecutorService controlledExecutor;
    private final NanoClock clock;
    private final TerminationAwaiter terminationAwaiter;
    private final AtomicBoolean submissionObserved = new AtomicBoolean();

    private volatile Phase phase = Phase.OPEN;
    private boolean submissionFreeAtShutdownFence;

    PaimonCompactionRuntime(String diagnosticGenerationName) {
        this(
                new ScheduledThreadPoolExecutor(
                        1,
                        new ExecutorThreadFactory(
                                THREAD_NAME_PREFIX
                                        + requireDiagnosticName(diagnosticGenerationName))),
                System::nanoTime,
                (executor, timeoutNanos) ->
                        executor.awaitTermination(timeoutNanos, TimeUnit.NANOSECONDS));
    }

    PaimonCompactionRuntime(
            ScheduledThreadPoolExecutor ownedExecutor,
            NanoClock clock,
            TerminationAwaiter terminationAwaiter) {
        this.ownedExecutor = Objects.requireNonNull(ownedExecutor, "ownedExecutor");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.terminationAwaiter =
                Objects.requireNonNull(terminationAwaiter, "terminationAwaiter");
        this.controlledExecutor = new ControlledScheduledExecutorService();
    }

    ScheduledExecutorService executor() {
        return controlledExecutor;
    }

    boolean submissionObserved() {
        return submissionObserved.get();
    }

    /**
     * Closes task admission and requests graceful executor shutdown exactly once.
     *
     * <p>{@link ScheduledThreadPoolExecutor#shutdown()} is non-blocking. It does not interrupt the
     * running compaction task, and queued one-shot tasks remain eligible to run.
     */
    boolean beginShutdown() {
        synchronized (admissionLock) {
            if (phase != Phase.OPEN) {
                return submissionFreeAtShutdownFence;
            }
            // Linearize the factory-rollback precondition with admission closure. Every admission
            // path uses this same lock, so an accepted pre-fence task is observed and a post-fence
            // task is rejected; there is no check-then-shutdown window.
            submissionFreeAtShutdownFence = !submissionObserved.get();
            phase = Phase.SHUTDOWN_REQUESTED;
            ownedExecutor.shutdown();
            return submissionFreeAtShutdownFence;
        }
    }

    /**
     * Waits up to an absolute monotonic-clock deadline for actual executor termination.
     *
     * <p>A timeout or interruption never changes the published termination state. The interrupt
     * flag is restored before the interruption is propagated to the lifecycle caller.
     */
    boolean awaitTermination(long absoluteDeadlineNanos) throws InterruptedException {
        if (phase == Phase.TERMINATED) {
            return true;
        }

        long timeoutNanos = remainingNanos(absoluteDeadlineNanos, clock.nanoTime());
        final boolean actuallyTerminated;
        try {
            actuallyTerminated = terminationAwaiter.await(ownedExecutor, timeoutNanos);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }

        if (!actuallyTerminated || !ownedExecutor.isTerminated()) {
            return false;
        }

        synchronized (admissionLock) {
            // Only this runtime can request shutdown in production. Keeping the phase check makes
            // injected test executors unable to publish a terminal event behind the owner's back.
            if (phase == Phase.SHUTDOWN_REQUESTED || phase == Phase.TERMINATED) {
                phase = Phase.TERMINATED;
                return true;
            }
            return false;
        }
    }

    boolean terminated() {
        return phase == Phase.TERMINATED;
    }

    /** Approximate executor data is diagnostics only and is never used as termination proof. */
    boolean workerStartedForDiagnostics() {
        return ownedExecutor.getPoolSize() > 0;
    }

    private void observeAdmissionMethod() {
        synchronized (admissionLock) {
            submissionObserved.set(true);
            requireOpenForAdmission();
        }
    }

    private void executeAdmission(Runnable command) {
        synchronized (admissionLock) {
            submissionObserved.set(true);
            requireOpenForAdmission();
            ownedExecutor.execute(command);
        }
    }

    private <V> ScheduledFuture<V> scheduleAdmission(
            Callable<V> callable, long delay, TimeUnit unit) {
        synchronized (admissionLock) {
            submissionObserved.set(true);
            requireOpenForAdmission();
            return ownedExecutor.schedule(callable, delay, unit);
        }
    }

    private ScheduledFuture<?> scheduleAdmission(Runnable command, long delay, TimeUnit unit) {
        synchronized (admissionLock) {
            submissionObserved.set(true);
            requireOpenForAdmission();
            return ownedExecutor.schedule(command, delay, unit);
        }
    }

    private void requireOpenForAdmission() {
        if (phase != Phase.OPEN) {
            throw new RejectedExecutionException(
                    "Paimon compaction runtime is not accepting tasks: " + phase);
        }
    }

    private static String requireDiagnosticName(String value) {
        String name = Objects.requireNonNull(value, "diagnosticGenerationName").trim();
        if (name.isEmpty()) {
            throw new IllegalArgumentException("diagnosticGenerationName must not be blank");
        }
        return name;
    }

    private static long remainingNanos(long deadline, long now) {
        if (deadline <= now) {
            return 0L;
        }
        long remaining = deadline - now;
        // A caller may deliberately use Long.MAX_VALUE as an unbounded practical deadline while
        // nanoTime is negative. Saturate the one possible subtraction overflow.
        return remaining > 0L ? remaining : Long.MAX_VALUE;
    }

    private static long relativeDeadline(long now, long timeoutNanos) {
        if (timeoutNanos <= 0L) {
            return now;
        }
        long deadline = now + timeoutNanos;
        return deadline < now ? Long.MAX_VALUE : deadline;
    }

    @FunctionalInterface
    interface NanoClock {
        long nanoTime();
    }

    @FunctionalInterface
    interface TerminationAwaiter {
        boolean await(ScheduledThreadPoolExecutor executor, long timeoutNanos)
                throws InterruptedException;
    }

    private enum Phase {
        OPEN,
        SHUTDOWN_REQUESTED,
        TERMINATED
    }

    /**
     * AbstractExecutorService routes every submit/invoke overload through {@link #execute}; the
     * explicit overrides also cover empty invoke collections, for which no execute call occurs.
     */
    private final class ControlledScheduledExecutorService extends AbstractExecutorService
            implements ScheduledExecutorService {

        @Override
        public void shutdown() {
            beginShutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException(
                    "Paimon compaction force shutdown requires owned Future handles");
        }

        @Override
        public boolean isShutdown() {
            return phase != Phase.OPEN;
        }

        @Override
        public boolean isTerminated() {
            return terminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            Objects.requireNonNull(unit, "unit");
            long now = clock.nanoTime();
            return PaimonCompactionRuntime.this.awaitTermination(
                    relativeDeadline(now, unit.toNanos(timeout)));
        }

        @Override
        public void execute(Runnable command) {
            executeAdmission(command);
        }

        @Override
        public Future<?> submit(Runnable task) {
            observeAdmissionMethod();
            return super.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            observeAdmissionMethod();
            return super.submit(task, result);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            observeAdmissionMethod();
            return super.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                throws InterruptedException {
            observeAdmissionMethod();
            return super.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(
                Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException {
            observeAdmissionMethod();
            return super.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                throws InterruptedException, ExecutionException {
            observeAdmissionMethod();
            return super.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(
                Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            observeAdmissionMethod();
            return super.invokeAny(tasks, timeout, unit);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return scheduleAdmission(command, delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return scheduleAdmission(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            synchronized (admissionLock) {
                submissionObserved.set(true);
                requireOpenForAdmission();
                return ownedExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
            }
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            synchronized (admissionLock) {
                submissionObserved.set(true);
                requireOpenForAdmission();
                return ownedExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
            }
        }
    }
}
