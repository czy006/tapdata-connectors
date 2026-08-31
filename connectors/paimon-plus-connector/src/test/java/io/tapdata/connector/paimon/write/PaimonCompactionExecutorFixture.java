package io.tapdata.connector.paimon.write;

import org.apache.paimon.utils.ExecutorThreadFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Deterministic latch fixture for {@link PaimonCompactionRuntimeTest}. */
final class PaimonCompactionExecutorFixture implements AutoCloseable {

    private static final long TEST_TIMEOUT_SECONDS = 10L;

    final CountingScheduledThreadPoolExecutor rawExecutor;
    final PaimonCompactionRuntime runtime;
    final CountDownLatch workerEntered = new CountDownLatch(1);
    final CountDownLatch releaseWorker = new CountDownLatch(1);
    final CountDownLatch workerReturned = new CountDownLatch(1);
    final AtomicInteger workerInterrupts = new AtomicInteger();
    final AtomicInteger writerCloseCount = new AtomicInteger();
    final AtomicInteger committerCloseCount = new AtomicInteger();
    final AtomicInteger ioCloseCount = new AtomicInteger();
    final AtomicLong workerReturnOrder = new AtomicLong();
    final AtomicLong terminationPublicationOrder = new AtomicLong();

    private final AtomicLong order = new AtomicLong();

    PaimonCompactionExecutorFixture() {
        this(null);
    }

    PaimonCompactionExecutorFixture(CountDownLatch awaitEntered) {
        ThreadFactory threadFactory =
                new ExecutorThreadFactory("paimon-compaction-runtime-fixture");
        this.rawExecutor = new CountingScheduledThreadPoolExecutor(threadFactory);
        this.runtime =
                new PaimonCompactionRuntime(
                        rawExecutor,
                        System::nanoTime,
                        (executor, timeoutNanos) -> {
                            if (awaitEntered != null) {
                                awaitEntered.countDown();
                            }
                            return executor.awaitTermination(timeoutNanos, TimeUnit.NANOSECONDS);
                        });
    }

    Future<?> startInterruptIgnoringWorker() {
        return runtime.executor()
                .submit(
                        () -> {
                            workerEntered.countDown();
                            try {
                                boolean released = false;
                                while (!released) {
                                    try {
                                        releaseWorker.await();
                                        released = true;
                                    } catch (InterruptedException ignored) {
                                        workerInterrupts.incrementAndGet();
                                    }
                                }
                            } finally {
                                workerReturnOrder.set(order.incrementAndGet());
                                workerReturned.countDown();
                            }
                        });
    }

    boolean closeDependenciesAfterTerminationProof(long absoluteDeadlineNanos)
            throws InterruptedException {
        if (!runtime.awaitTermination(absoluteDeadlineNanos)) {
            return false;
        }
        terminationPublicationOrder.compareAndSet(0L, order.incrementAndGet());
        writerCloseCount.incrementAndGet();
        committerCloseCount.incrementAndGet();
        ioCloseCount.incrementAndGet();
        return true;
    }

    void awaitWorkerEntered() throws InterruptedException {
        assertTrue(
                workerEntered.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "compaction worker did not enter");
    }

    void releaseAndAwaitWorkerReturn() throws InterruptedException {
        releaseWorker.countDown();
        assertTrue(
                workerReturned.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "compaction worker did not return");
    }

    static long deadlineAfterSeconds(long seconds) {
        long now = System.nanoTime();
        long delta = TimeUnit.SECONDS.toNanos(seconds);
        long deadline = now + delta;
        return deadline < now ? Long.MAX_VALUE : deadline;
    }

    static long expiredDeadline() {
        return System.nanoTime();
    }

    @Override
    public void close() throws Exception {
        releaseWorker.countDown();
        runtime.beginShutdown();
        assertTrue(
                runtime.awaitTermination(deadlineAfterSeconds(TEST_TIMEOUT_SECONDS)),
                "fixture compaction executor did not terminate");
    }

    static final class CountingScheduledThreadPoolExecutor
            extends ScheduledThreadPoolExecutor {

        private final AtomicInteger shutdownCalls = new AtomicInteger();
        private final AtomicInteger shutdownNowCalls = new AtomicInteger();

        private CountingScheduledThreadPoolExecutor(ThreadFactory threadFactory) {
            super(1, threadFactory);
        }

        @Override
        public void shutdown() {
            shutdownCalls.incrementAndGet();
            super.shutdown();
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
            shutdownNowCalls.incrementAndGet();
            return super.shutdownNow();
        }

        int shutdownCalls() {
            return shutdownCalls.get();
        }

        int shutdownNowCalls() {
            return shutdownNowCalls.get();
        }
    }
}
