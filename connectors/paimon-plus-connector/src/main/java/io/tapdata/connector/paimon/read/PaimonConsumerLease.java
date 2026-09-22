package io.tapdata.connector.paimon.read;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

/**
 * 连接器自建的 Consumer 续租与超时门禁，不是 Paimon 提供的原子租约。
 * 原生 ConsumerManager#expire 按文件 mtime 判过期；存储 I/O 可能阻塞，所以 watchdog 独立且不做 I/O。
 * 超时只禁止本 attempt 继续交付，不能证明底层 I/O 已取消；关闭时仍须等待工作线程终止。
 * @see <a href="https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java#L95">Paimon Consumer 过期实现</a>
 */
public final class PaimonConsumerLease {
    interface Target {
        long readTtlNanos() throws Exception;
        void writeAndVerify() throws Exception;
    }
    // 60 秒续租、120 秒余量都是本地策略；与注册阶段要求的至少 10 分钟 TTL 配套使用。
    private static final long HEARTBEAT = TimeUnit.SECONDS.toNanos(60);
    private static final long MARGIN = 2 * HEARTBEAT;
    private final Object gate = new Object();
    private final List<State> states = new ArrayList<>();
    private final ThreadPoolExecutor io;
    private final ScheduledExecutorService watchdog;
    private final LongSupplier clock;
    private final BooleanSupplier active;
    private final Consumer<Throwable> onFailure;
    private final Runnable threadContext;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean cancelled;
    private boolean started;

    private static final class State {
        final Target target;
        final long created;
        long deadline;
        long ready;
        boolean pending;
        boolean initialized;
        State(Target target, long now, long budget) { this.target=target;created=now;deadline=now+budget;ready=now; }
    }
    public static PaimonConsumerLease create(List<PaimonConsumerRegistration> registrations, long timeoutMillis,
            BooleanSupplier active, Consumer<Throwable> onFailure, Runnable threadContext) {
        return new PaimonConsumerLease(registrations, timeoutMillis, active, onFailure, threadContext, System::nanoTime);
    }
    PaimonConsumerLease(List<? extends Target> targets, long timeoutMillis, BooleanSupplier active,
            Consumer<Throwable> onFailure, Runnable threadContext, LongSupplier clock) {
        if (targets.isEmpty() || timeoutMillis <= 0) { throw new IllegalArgumentException("Invalid Consumer lease configuration"); }
        this.clock=clock;this.active=active;this.onFailure=onFailure;this.threadContext=threadContext;
        long now=clock.getAsLong();
        for(Target target:targets)states.add(new State(target,now,TimeUnit.MILLISECONDS.toNanos(timeoutMillis)));
        int workers=Math.min(targets.size(),Math.max(1,Runtime.getRuntime().availableProcessors()));
        io=new ThreadPoolExecutor(workers,workers,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(targets.size()),
                task->thread(task,"paimon-consumer-io"),new ThreadPoolExecutor.AbortPolicy());
        watchdog=Executors.newSingleThreadScheduledExecutor(task->thread(task,"paimon-consumer-watchdog"));
    }
    private static Thread thread(Runnable body,String name) { Thread t=new Thread(body,name);t.setDaemon(true);return t; }

    public void initialize() throws Exception { initialize(() -> true); }
    public void initialize(BooleanSupplier callerActive) throws Exception {
        check();
        synchronized(gate) {
            if(!started) { started=true;watchdog.scheduleWithFixedDelay(this::tick,0,1,TimeUnit.SECONDS); }
        }
        while(true) {
            if(!callerActive.getAsBoolean())throw new CancellationException("Consumer initialization cancelled");
            check();
            synchronized(gate) {
                if(states.stream().allMatch(s->s.initialized))return;
                gate.wait(20);
            }
        }
    }
    /** Invoked by both source operations and the independent watchdog; performs no storage I/O. */
    public void check() {
        Throwable error=failure.get();
        if(error!=null)throw new IllegalStateException("Paimon Consumer protection failed",error);
        if(cancelled || !active.getAsBoolean())throw new CancellationException("Consumer lease stopped");
        long now=clock.getAsLong();
        boolean expired=false;
        synchronized(gate) {
            for(State state:states) { if(now-state.deadline>=0) {expired=true;break;} }
        }
        if(expired) {
            fail(new TimeoutException("Paimon Consumer protection deadline exceeded"));
            throw new IllegalStateException("Paimon Consumer protection deadline exceeded",failure.get());
        }
    }

    void tick() {
        try {
            threadContext.run();check();
            synchronized(gate) {
                if(cancelled)return;
                for(State state:states) {
                    // 每个 Consumer 最多一个续租任务；提交与关闭共用 gate，避免停机后再派发。
                    if(!state.pending && clock.getAsLong()-state.ready>=0) {
                        state.pending=true;
                        io.execute(()->renew(state));
                    }
                }
            }
        } catch(CancellationException stopped) { if(!cancelled && active.getAsBoolean())fail(stopped); }
        catch(Throwable error) { if(!cancelled && active.getAsBoolean())fail(error); }
    }
    private void renew(State state) {
        try {
            threadContext.run();
            for(int attempt=0;attempt<3;attempt++) {
                check();
                long operationStart=clock.getAsLong();
                try {
                    long ttl=state.target.readTtlNanos();
                    if(ttl<TimeUnit.MINUTES.toNanos(10) || ttl>Long.MAX_VALUE/4)throw new IllegalStateException("Consumer TTL is outside safe deadline range");
                    synchronized(gate) {
                        if(!state.initialized) {
                            long risk=state.created+ttl-MARGIN;
                            if(risk-state.deadline<0)state.deadline=risk;
                        }
                    }
                    check();state.target.writeAndVerify();check();
                    synchronized(gate) {
                        // Use operation START, not completion, as the conservative renewal baseline.
                        // Connector policy atop ConsumerManager's overwrite/readback, not an atomic lease:
                        // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java#L66
                        if(cancelled || failure.get()!=null)throw new CancellationException("Consumer attempt fenced");
                        state.deadline=operationStart+ttl-MARGIN;
                        state.ready=clock.getAsLong()+HEARTBEAT;state.initialized=true;gate.notifyAll();
                    }
                    check();return;
                } catch(CancellationException stopped) { throw stopped; }
                catch(Exception error) {
                    if(attempt==2)throw error;
                    long retryAt=clock.getAsLong()+TimeUnit.SECONDS.toNanos(1);
                    while(clock.getAsLong()-retryAt<0) {check();Thread.sleep(20);}
                }
            }
        } catch(CancellationException stopped) { if(!cancelled && active.getAsBoolean())fail(stopped); }
        catch(Throwable error) { if(!cancelled)fail(error); }
        finally { synchronized(gate) {state.pending=false;gate.notifyAll();} }
    }
    private void fail(Throwable cause) {
        if(failure.compareAndSet(null,cause)) {
            try {onFailure.accept(cause);}
            finally {synchronized(gate){gate.notifyAll();}}
        }
    }
    public Throwable failure() { return failure.get(); }
    // shutdownNow 只发中断；只有 awaitTermination 成功后才能释放依赖的 Catalog/FileIO。
    public void close(long deadlineNanos) throws InterruptedException, TimeoutException {
        synchronized(gate) {cancelled=true;gate.notifyAll();watchdog.shutdownNow();io.shutdownNow();}
        long remaining=Math.max(0,deadlineNanos-System.nanoTime());
        boolean watchdogEnded=watchdog.awaitTermination(remaining,TimeUnit.NANOSECONDS);
        remaining=Math.max(0,deadlineNanos-System.nanoTime());
        if(!io.awaitTermination(remaining,TimeUnit.NANOSECONDS) || !watchdogEnded) {
            throw new TimeoutException("Consumer lease I/O termination not proved; retain shared resources");
        }
    }
}
