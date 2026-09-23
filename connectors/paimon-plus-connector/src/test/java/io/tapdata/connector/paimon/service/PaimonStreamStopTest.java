package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.read.PaimonReadOffset;
import io.tapdata.connector.paimon.read.PaimonStreamReadService;
import io.tapdata.connector.paimon.read.PaimonStreamReadSession;
import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaimonStreamStopTest {
    static PaimonStreamReadService streamService(PaimonService service) throws Exception {
        java.lang.reflect.Field field = PaimonService.class.getDeclaredField("streamReadService");
        field.setAccessible(true);
        return (PaimonStreamReadService) field.get(service);
    }

    PaimonStreamReadSession session() {
        PaimonReadOffset offset = new PaimonReadOffset(UUID.randomUUID().toString(), Collections.singletonMap("a",
                new PaimonReadOffset.Position(String.join("", Collections.nCopies(64,"a")),1,1)));
        return new PaimonStreamReadSession(offset,1,100,()->true,(e,o)->{});
    }
    void catalog(PaimonService service, Catalog catalog) throws Exception {
        java.lang.reflect.Field field = PaimonService.class.getDeclaredField("catalog"); field.setAccessible(true); field.set(service,catalog);
    }
    @Test void closeCancelsStreamBeforeAwaitingItsIngress() throws Exception {
        PaimonBoundedStopTest.Fixture f = new PaimonBoundedStopTest.Fixture(10,5,2);
        Catalog catalog = mock(Catalog.class); catalog(f.service,catalog);
        PaimonStreamReadSession session = session(); CountDownLatch entered = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> read = executor.submit(() -> { streamService(f.service).executeSession(session, () -> {
                entered.countDown(); while(session.isActive()) Thread.sleep(5);
            }); return null; });
            assertTrue(entered.await(3,TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class,()->streamService(f.service).executeSession(session(),()->{}));
            f.service.close(); read.get(3,TimeUnit.SECONDS);
            assertFalse(session.isActive()); assertTrue(f.resources().isEmpty()); verify(catalog).close();
        } finally { session.cancel(); executor.shutdownNow(); assertTrue(executor.awaitTermination(3,TimeUnit.SECONDS)); }
    }
    @Test void inFlightSourceKeepsCatalogAliveAfterStopBudgetExpires() throws Exception {
        PaimonBoundedStopTest.Fixture f = new PaimonBoundedStopTest.Fixture(1,1,1);
        Catalog catalog = mock(Catalog.class); catalog(f.service,catalog);
        PaimonStreamReadSession session = session(); CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        Thread reader = new Thread(() -> { try { streamService(f.service).executeSession(session, () -> {
            try(PaimonStreamReadSession.Admission ignored = session.admit()) { entered.countDown(); release.await(); }
        }); } catch(Throwable error) { readFailure.set(error); } });
        reader.setDaemon(true); reader.start();
        try {
            assertTrue(entered.await(3,TimeUnit.SECONDS)); f.start();
            long deadline = System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while(!f.control().isStarted() && System.nanoTime()<deadline) Thread.sleep(5);
            assertTrue(f.control().isStarted()); f.advance(2); f.joinFailedRetained();
            assertSame(f.service, PaimonStopResources.retainedRoot(f.control()));
            verify(catalog,never()).close(); assertFalse(session.isActive());
        } finally { release.countDown(); reader.join(3000); if(f.worker()!=null)f.worker().join(3000); }
        assertFalse(reader.isAlive()); verify(catalog,never()).close();
    }
}
