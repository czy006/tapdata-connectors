package io.tapdata.connector.paimon.read;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class PaimonConsumerLeaseTest {
    long deadline() {return System.nanoTime()+TimeUnit.SECONDS.toNanos(3);}
    @Test void renewsIndependentlyAndUsesOperationStartDeadline() throws Exception {
        AtomicLong clock=new AtomicLong();AtomicInteger writes=new AtomicInteger();AtomicInteger failures=new AtomicInteger();
        PaimonConsumerLease.Target target=new PaimonConsumerLease.Target() {
            public long readTtlNanos(){return TimeUnit.MINUTES.toNanos(10);}
            public void writeAndVerify(){writes.incrementAndGet();clock.addAndGet(TimeUnit.SECONDS.toNanos(30));}
        };
        PaimonConsumerLease lease=new PaimonConsumerLease(Collections.singletonList(target),180000,()->true,e->failures.incrementAndGet(),()->{},clock::get);
        try {
            lease.initialize();assertEquals(1,writes.get());clock.set(TimeUnit.SECONDS.toNanos(90));lease.tick();
            long wait=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while(writes.get()<2 && System.nanoTime()<wait)Thread.sleep(5);
            assertEquals(2,writes.get());
            // Renewal began at 90s, so risk deadline is 570s, not completion(120s)+480s.
            clock.set(TimeUnit.SECONDS.toNanos(571));assertThrows(IllegalStateException.class,lease::check);
            assertEquals(1,failures.get());assertNotNull(lease.failure());
        } finally {lease.close(deadline());}
    }
    @Test void watchdogFencesBlockedInitialIoAndLateSuccessCannotRevive() throws Exception {
        AtomicLong clock=new AtomicLong();CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
        AtomicInteger writes=new AtomicInteger();
        PaimonConsumerLease.Target target=new PaimonConsumerLease.Target() {
            public long readTtlNanos(){return TimeUnit.MINUTES.toNanos(10);}
            public void writeAndVerify(){writes.incrementAndGet();entered.countDown();boolean done=false;while(!done){try{release.await();done=true;}catch(InterruptedException ignored){}}}
        };
        PaimonConsumerLease lease=new PaimonConsumerLease(Collections.singletonList(target),1000,()->true,e->{},()->{},clock::get);
        ExecutorService caller=Executors.newSingleThreadExecutor();
        try {
            Future<?> init=caller.submit(()->{lease.initialize();return null;});assertTrue(entered.await(3,TimeUnit.SECONDS));
            clock.set(TimeUnit.SECONDS.toNanos(2));lease.tick();
            assertThrows(ExecutionException.class,()->init.get(3,TimeUnit.SECONDS));assertEquals(1,writes.get());
            assertThrows(TimeoutException.class,()->lease.close(System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(50)));
            release.countDown();lease.close(deadline());assertThrows(IllegalStateException.class,lease::check);assertEquals(1,writes.get());
        } finally {release.countDown();lease.close(deadline());caller.shutdownNow();assertTrue(caller.awaitTermination(3,TimeUnit.SECONDS));}
    }
    @Test void retryExhaustionIsStickyAndSameConsumerHasOneInFlightOperation() throws Exception {
        AtomicInteger writes=new AtomicInteger(),active=new AtomicInteger(),maximum=new AtomicInteger();
        PaimonConsumerLease.Target target=new PaimonConsumerLease.Target() {
            public long readTtlNanos(){return TimeUnit.MINUTES.toNanos(10);}
            public void writeAndVerify(){maximum.accumulateAndGet(active.incrementAndGet(),Math::max);try{writes.incrementAndGet();throw new IllegalStateException("storage failure");}finally{active.decrementAndGet();}}
        };
        PaimonConsumerLease lease=new PaimonConsumerLease(Collections.singletonList(target),5000,()->true,e->{},()->{},System::nanoTime);
        try {assertThrows(IllegalStateException.class,lease::initialize);assertEquals(3,writes.get());assertEquals(1,maximum.get());assertNotNull(lease.failure());}
        finally {lease.close(deadline());}
    }
}
