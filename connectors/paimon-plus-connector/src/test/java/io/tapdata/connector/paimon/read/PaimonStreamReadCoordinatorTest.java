package io.tapdata.connector.paimon.read;

import org.apache.paimon.table.source.TableScan;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaimonStreamReadCoordinatorTest {
    PaimonStreamReadCoordinator.Source emptySource(String name, AtomicLong next, AtomicInteger calls, boolean initiallyIdle) throws Exception {
        PaimonStreamReadCoordinator.Source source = mock(PaimonStreamReadCoordinator.Source.class);
        when(source.name()).thenReturn(name); when(source.checkpoint()).thenAnswer(i -> next.get());
        TableScan.Plan plan = mock(TableScan.Plan.class); when(plan.splits()).thenReturn(Collections.emptyList());
        when(source.plan()).thenAnswer(i -> { int call = calls.incrementAndGet(); if (!initiallyIdle || call > 1) { next.incrementAndGet(); } return plan; });
        return source;
    }
    PaimonReadOffset offset(String... names) {
        Map<String,PaimonReadOffset.Position> map = new LinkedHashMap<>();
        for (String name : names) { map.put(name, new PaimonReadOffset.Position(PaimonReadOffsetTest.HASH, 1, 1)); }
        return new PaimonReadOffset(UUID.randomUUID().toString(), map);
    }
    @Test void moreTablesThanWorkersAllGetAPlan() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true); Set<String> observed = new HashSet<>();
        PaimonStreamReadSession session = new PaimonStreamReadSession(offset("a","b","c"), 2, 100,
                running::get, (events, value) -> {
                    PaimonReadOffset.parse(value).tables().forEach((name,pos) -> { if(pos.nextSnapshot>1) observed.add(name); });
                    if (observed.size()==3) running.set(false);
                });
        List<PaimonStreamReadCoordinator.Source> sources = new ArrayList<>();
        for (String name : Arrays.asList("a","b","c")) sources.add(emptySource(name,new AtomicLong(1),new AtomicInteger(),false));
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(10), () -> session.run(sources, 10, 1000, () -> {}, 1));
        assertEquals(new HashSet<>(Arrays.asList("a","b","c")), observed);
    }
    @Test void noProgressIsRescheduledInsteadOfWaitingForMissingMarker() throws Exception {
        AtomicInteger calls = new AtomicInteger(); AtomicBoolean running = new AtomicBoolean(true);
        PaimonStreamReadSession session = new PaimonStreamReadSession(offset("a"), 2, 100,
                running::get, (events, value) -> running.set(false));
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(10), () -> session.run(
                Collections.singletonList(emptySource("a",new AtomicLong(1),calls,true)), 10, 1000, () -> {},1));
        assertTrue(calls.get()>=2);
    }
    @Test void cancellationRejectsOffersEvenIntoAnEmptyQueue() {
        PaimonStreamReadSession session = new PaimonStreamReadSession(offset("a"),1,10,()->true,(e,o)->{});
        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(1);
        assertTrue(session.offer(queue,1)); assertFalse(session.offer(queue,2));
        session.cancel(); queue.clear();
        assertThrows(CancellationException.class,()->session.offer(queue,3)); assertTrue(queue.isEmpty());
    }
    @Test void blockedNativePlanReportsTimeoutRatherThanFakeTermination() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);
        PaimonStreamReadCoordinator.Source source = emptySource("a",new AtomicLong(1),new AtomicInteger(),false);
        when(source.plan()).thenAnswer(i -> {
            entered.countDown();
            while (true) { try { release.await(); break; } catch(InterruptedException ignored) {} }
            return mock(TableScan.Plan.class);
        });
        PaimonStreamReadSession session = new PaimonStreamReadSession(offset("a"),1,10,running::get,(e,o)->{});
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = executor.submit(() -> { session.run(Collections.singletonList(source),10,100,()->{},1); return null; });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); running.set(false);
            ExecutionException error = assertThrows(ExecutionException.class,()->result.get(5,TimeUnit.SECONDS));
            assertTrue(error.getCause() instanceof TimeoutException);
        } finally { release.countDown(); executor.shutdownNow(); assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS)); }
    }
}
