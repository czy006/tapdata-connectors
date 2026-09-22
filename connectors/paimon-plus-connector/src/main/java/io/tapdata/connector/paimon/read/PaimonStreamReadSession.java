package io.tapdata.connector.paimon.read;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** Per-invocation cancellation gate and single-threaded delivery; no durable acknowledgement. */
public final class PaimonStreamReadSession {
    private final Object gate = new Object();
    private final BooleanSupplier running;
    private final BiConsumer<List<TapEvent>, Object> consumer;
    private final int batchSize;
    private final long flushNanos;
    private final LongSupplier clock;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean cancelled;
    private volatile long cancelledAt;
    private int inFlight;
    private volatile PaimonStreamReadCoordinator coordinator;
    private PaimonReadOffset submitted;
    private List<TapEvent> batch = new ArrayList<>();
    private long firstEventAt;

    public PaimonStreamReadSession(PaimonReadOffset offset, int batchSize, long flushMillis,
            BooleanSupplier running, BiConsumer<List<TapEvent>, Object> consumer) {
        this(offset, batchSize, flushMillis, running, consumer, System::nanoTime);
    }
    PaimonStreamReadSession(PaimonReadOffset offset, int batchSize, long flushMillis,
            BooleanSupplier running, BiConsumer<List<TapEvent>, Object> consumer, LongSupplier clock) {
        if (batchSize <= 0 || flushMillis <= 0) { throw new IllegalArgumentException("Positive batch/flush interval required"); }
        Math.multiplyExact(batchSize, 10);
        this.submitted = offset; this.batchSize = batchSize; this.flushNanos = TimeUnit.MILLISECONDS.toNanos(flushMillis);
        this.running = running; this.consumer = consumer; this.clock = clock;
    }
    public boolean isActive() { return !cancelled && failure.get() == null && running.getAsBoolean(); }
    public void cancel() {
        synchronized (gate) {
            if (!cancelled) { cancelledAt = System.nanoTime(); cancelled = true; gate.notifyAll(); }
        }
    }
    public long cancelledAtNanos() { return cancelledAt; }
    public Throwable failure() { return failure.get(); }
    public void fail(Throwable cause) {
        Throwable first = failure.get();
        if (first == null && failure.compareAndSet(null, cause)) { cancel(); return; }
        first = failure.get();
        if (first != cause) { synchronized (first) { first.addSuppressed(cause); } }
        cancel();
    }
    public void checkActive() { PaimonStreamReadCoordinator.checkActive(this::isActive); }

    /** Short admission critical section; never execute I/O or a PDK callback while holding it. */
    public Admission admit() {
        synchronized (gate) { checkActive(); inFlight++; return new Admission(); }
    }
    public final class Admission implements AutoCloseable {
        private boolean closed;
        private Admission() {}
        @Override public void close() {
            synchronized (gate) { if (!closed) { closed = true; inFlight--; gate.notifyAll(); } }
        }
    }
    public int inFlight() { synchronized (gate) { return inFlight; } }
    public PaimonReadOffset submittedOffset() { return submitted; }

    <T> boolean offer(BlockingQueue<T> queue, T item) {
        synchronized (gate) { checkActive(); return queue.offer(item); }
    }

    public void run(List<PaimonStreamReadCoordinator.Source> sources, long pollMillis,
            long stopTimeoutMillis, Runnable threadContext) throws Exception {
        run(sources, pollMillis, stopTimeoutMillis, threadContext,
                Math.min(sources.size(), Runtime.getRuntime().availableProcessors()));
    }
    void run(List<PaimonStreamReadCoordinator.Source> sources, long pollMillis,
            long stopTimeoutMillis, Runnable threadContext, int workerCount) throws Exception {
        if (!isActive()) { return; }
        if (stopTimeoutMillis <= 0) { throw new IllegalArgumentException("Positive stop timeout required"); }
        coordinator = new PaimonStreamReadCoordinator(sources, this, Math.multiplyExact(batchSize, 10),
                workerCount, pollMillis, threadContext);
        boolean interrupted = false;
        try {
            coordinator.start();
            while (isActive()) {
                PaimonStreamReadCoordinator.Item item = coordinator.poll(20);
                if (item != null && deliver(item)) { coordinator.delivered(item); }
                flushIfDue();
            }
        } catch (CancellationException cancelled) {
            if (isActive()) { fail(cancelled); }
        } catch (InterruptedException interruption) {
            interrupted = true;
            if (isActive()) { fail(interruption); }
        } catch (Throwable error) { fail(error); }
        finally {
            cancel();
            long deadline = cancelledAtNanos() + TimeUnit.MILLISECONDS.toNanos(stopTimeoutMillis);
            try {
                coordinator.close(deadline);
                if (inFlight() != 0) { throw new IllegalStateException("Paimon source has active admissions after worker termination"); }
            } catch (InterruptedException interruption) { interrupted = true; fail(interruption); }
            catch (Throwable closeFailure) { fail(closeFailure); }
            batch = new ArrayList<>();
            if (interrupted) { Thread.currentThread().interrupt(); }
        }
        Throwable error = failure();
        if (error instanceof Exception) { throw (Exception) error; }
        if (error instanceof Error) { throw (Error) error; }
        if (error != null) { throw new IllegalStateException("Paimon source failed", error); }
    }

    /** Called only by the delivery thread. Returns true when a plan boundary was submitted. */
    public boolean deliver(PaimonStreamReadCoordinator.Item item) {
        checkActive();
        PaimonReadOffset.Position previous = submitted.position(item.table);
        if (item.isFinished()) {
            if (item.before != previous.nextSnapshot || item.after <= item.before) {
                throw new IllegalStateException("Out-of-order Paimon plan completion: " + item.table);
            }
            submit(submitted.withNext(item.table, item.after));
            return true;
        }
        if (batch.isEmpty()) { firstEventAt = clock.getAsLong(); }
        batch.add(item.event);
        if (batch.size() >= batchSize) { submit(null); }
        return false;
    }
    public void flushIfDue() {
        if (!batch.isEmpty() && clock.getAsLong() - firstEventAt >= flushNanos) { submit(null); }
    }

    // PDK 2.0.8-SNAPSHOT StreamReadConsumer.accept (74-76) only delegates BiConsumer.
    // Engine develop@ba3533f47fc3c43e9f0dc408f5e871986f1e8626:
    // iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/
    // HazelcastSourcePdkDataNode.java#generateStreamReadConsumer (1169-1214): empty events ignored.
    // HazelcastSourcePdkBaseNode.java#wrapTapdataEvent (1403-1423,1512): last event carries offset;
    // HeartbeatEvent has an existing control-event conversion. This is submission, NOT durable ack.
    // Paimon API reference: https://paimon.apache.org/docs/1.3/program-api/java-api/#stream-read
    private void submit(PaimonReadOffset candidate) {
        if (batch.isEmpty() && candidate == null) { return; }
        List<TapEvent> events = batch;
        if (events.isEmpty()) {
            events = new ArrayList<>();
            events.add(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis()));
        }
        PaimonReadOffset offset = candidate == null ? submitted : candidate;
        try (Admission ignored = admit()) {
            consumer.accept(events, offset.toMap());
            checkActive();
            submitted = offset;
            batch = new ArrayList<>();
        } catch (CancellationException cancelled) { throw cancelled; }
        catch (RuntimeException | Error error) { fail(error); throw error; }
    }
}
