package io.tapdata.connector.paimon.service;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test-only fixture for deterministic coordinator linearization and lock-order checks. */
final class PaimonCoordinatorLockOrderFixture implements AutoCloseable {

    private static final long TIMEOUT_SECONDS = 5L;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    Future<?> submit(ThrowingRunnable action) {
        return executor.submit(
                () -> {
                    action.run();
                    return null;
                });
    }

    void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    void await(Future<?> future) throws Exception {
        future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    void whileAdmissionLockHeld(
            PaimonServiceResourceCoordinator coordinator, ThrowingRunnable action)
            throws Exception {
        Field field =
                PaimonServiceResourceCoordinator.class.getDeclaredField("admissionLock");
        field.setAccessible(true);
        Object admissionLock = field.get(coordinator);
        synchronized (admissionLock) {
            action.run();
        }
    }

    @Override
    public void close() throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }
}
