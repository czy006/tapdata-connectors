package io.tapdata.connector.paimon.read;

import io.tapdata.entity.event.TapEvent;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** One native plan per task; native plan() itself has no bounded-duration guarantee. */
public final class PaimonStreamReadCoordinator {
    public interface Source {
        String name();
        Long checkpoint();
        void validatePosition(long next) throws Exception;
        void beforePlan() throws Exception;
        TableScan.Plan plan() throws Exception;
        void beforeRead(TableScan.Plan plan) throws Exception;
        RecordReader<InternalRow> reader(Split split) throws Exception;
        TapEvent convert(InternalRow row) throws Exception;
    }
    public interface Sink { void put(Item item) throws Exception; }

    public static final class Item {
        public final String table;
        public final TapEvent event;
        public final long before;
        public final long after;
        private long taskToken;
        private Item(String table, TapEvent event, long before, long after) {
            this.table = table; this.event = event; this.before = before; this.after = after;
        }
        public static Item event(String table, TapEvent event) { return new Item(table, event, 0, 0); }
        public static Item finished(String table, long before, long after) { return new Item(table, null, before, after); }
        public boolean isFinished() { return event == null; }
    }

    private final PaimonStreamReadSession session;
    private final BlockingQueue<Item> events;
    private final BlockingQueue<Completion> completions;
    private final Map<String, State> states = new LinkedHashMap<>();
    private final ThreadPoolExecutor workers;
    private final Thread scheduler;
    private final Runnable threadContext;
    private final long pollNanos;
    private long nextToken;

    private static final class State {
        final Source source;
        boolean outstanding;
        long token;
        long readyAt;
        State(Source source) { this.source = source; }
    }
    private static final class Completion {
        final State state;
        final long token;
        final boolean idle;
        Completion(State state, long token, boolean idle) { this.state = state; this.token = token; this.idle = idle; }
    }

    PaimonStreamReadCoordinator(List<Source> sources, PaimonStreamReadSession session,
            int capacity, int workerCount, long pollMillis, Runnable threadContext) {
        if (sources.isEmpty() || workerCount <= 0 || pollMillis <= 0) { throw new IllegalArgumentException("Invalid stream scheduling configuration"); }
        for (Source source : sources) {
            if (states.put(source.name(), new State(source)) != null) { throw new IllegalArgumentException("Duplicate stream table"); }
        }
        this.session = session; this.threadContext = threadContext;
        this.pollNanos = TimeUnit.MILLISECONDS.toNanos(pollMillis);
        this.events = new ArrayBlockingQueue<>(capacity);
        this.completions = new ArrayBlockingQueue<>(sources.size());
        workers = new ThreadPoolExecutor(workerCount, workerCount, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(sources.size()), task -> {
                    Thread thread = new Thread(task, "paimon-stream-worker"); thread.setDaemon(true); return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
        scheduler = new Thread(this::schedule, "paimon-stream-scheduler"); scheduler.setDaemon(true);
    }

    void start() { scheduler.start(); }
    Item poll(long millis) throws InterruptedException { return events.poll(millis, TimeUnit.MILLISECONDS); }
    void delivered(Item item) throws InterruptedException {
        post(new Completion(states.get(item.table), item.taskToken, false));
    }
    private void post(Completion completion) throws InterruptedException {
        while (!session.offer(completions, completion)) { Thread.sleep(10); }
    }
    private void enqueue(Item item) throws InterruptedException {
        // Nonblocking offer and cancel share the short session gate. Never block put() on a full queue.
        while (!session.offer(events, item)) { Thread.sleep(10); }
    }
    private void apply(Completion completion) {
        if (completion == null) { return; }
        State state = completion.state;
        if (state != null && state.outstanding && state.token == completion.token) {
            state.outstanding = false;
            state.readyAt = completion.idle ? System.nanoTime() + pollNanos : 0;
        }
    }
    private void schedule() {
        try {
            // PDK API 2.0.8-SNAPSHOT TapConnectorContext.configContext (33-40) is required
            // on new connector threads. The supplied callback runs before connector work.
            threadContext.run();
            while (session.isActive()) {
                Completion completion;
                while ((completion = completions.poll()) != null) { apply(completion); }
                for (State state : states.values()) {
                    if (!state.outstanding && (state.readyAt == 0 || System.nanoTime() - state.readyAt >= 0)) {
                        try (PaimonStreamReadSession.Admission ignored = session.admit()) {
                            state.outstanding = true;
                            long token = ++nextToken;
                            state.token = token;
                            workers.execute(() -> read(state, token));
                        }
                    }
                }
                apply(completions.poll(20, TimeUnit.MILLISECONDS));
            }
        } catch (CancellationException cancelled) {
            if (session.isActive()) { session.fail(cancelled); }
        } catch (InterruptedException interrupted) {
            if (session.isActive()) { session.fail(interrupted); }
            Thread.currentThread().interrupt();
        } catch (Throwable error) { session.fail(error); }
    }
    private void read(State state, long token) {
        try {
            threadContext.run();
            boolean progressed = readPlan(state.source, session::isActive, item -> {
                item.taskToken = token; enqueue(item);
            });
            if (!progressed) { post(new Completion(state, token, true)); }
        } catch (CancellationException cancelled) {
            if (session.isActive()) { session.fail(cancelled); }
        } catch (InterruptedException interrupted) {
            if (session.isActive()) { session.fail(interrupted); }
            Thread.currentThread().interrupt();
        } catch (Throwable error) { session.fail(error); }
    }

    void close(long deadlineNanos) throws InterruptedException, TimeoutException {
        session.cancel();
        scheduler.interrupt();
        workers.shutdownNow();
        long remaining = Math.max(0, deadlineNanos - System.nanoTime());
        if (remaining > 0) { TimeUnit.NANOSECONDS.timedJoin(scheduler, remaining); }
        remaining = Math.max(0, deadlineNanos - System.nanoTime());
        if (!workers.awaitTermination(remaining, TimeUnit.NANOSECONDS) || scheduler.isAlive()) {
            throw new TimeoutException("Paimon source worker/scheduler termination not proved; retain source resources");
        }
        events.clear(); completions.clear();
    }


    // DataTableStreamScan.nextPlan (199-242) increments next BEFORE split reads and can skip
    // multiple empty snapshots. A checkpoint becomes deliverable only after all readers close.
    // Apache Paimon release-1.3 @ 5c59e6cb01ed0b29563371f56e14fcade4597a2e,
    // paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java.
    // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java#L199
    public static boolean readPlan(Source source, BooleanSupplier active, Sink sink) throws Exception {
        checkActive(active);
        Long before = source.checkpoint();
        if (before == null || before <= 0) { throw new IllegalStateException("Stream scan must be explicitly restored"); }
        source.validatePosition(before);
        source.beforePlan();
        checkActive(active);
        TableScan.Plan plan = source.plan();
        Long after = source.checkpoint();
        if (after == null || after < before || (!plan.splits().isEmpty() && after.equals(before))) {
            throw new IllegalStateException("Invalid Paimon checkpoint transition");
        }
        checkActive(active);
        source.beforeRead(plan);
        for (Split split : plan.splits()) {
            checkActive(active);
            try (RecordReader<InternalRow> reader = source.reader(split)) {
                while (true) {
                    checkActive(active);
                    RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
                    if (iterator == null) { break; }
                    // RecordReader.RecordIterator.releaseBatch contract. TWR preserves the first
                    // read/convert error when batch release or reader close also throws.
                    // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-common/src/main/java/org/apache/paimon/reader/RecordReader.java#L59
                    try (Batch batch = new Batch(iterator)) {
                        while (true) {
                            checkActive(active);
                            InternalRow row = batch.iterator.next();
                            if (row == null) { break; }
                            TapEvent event = source.convert(row);
                            checkActive(active);
                            if (event == null) { throw new IllegalStateException("Paimon row conversion returned null"); }
                            sink.put(Item.event(source.name(), event));
                        }
                    }
                }
            }
        }
        checkActive(active);
        // Also detect a gap reached after native plan skipped several empty snapshots.
        source.validatePosition(after);
        checkActive(active);
        if (after > before) { sink.put(Item.finished(source.name(), before, after)); return true; }
        return false; // Scheduler must release this task through PlanNoProgress, not a fake marker.
    }

    static void checkActive(BooleanSupplier active) {
        if (!active.getAsBoolean() || Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Paimon stream cancelled");
        }
    }
    private static final class Batch implements AutoCloseable {
        private final RecordReader.RecordIterator<InternalRow> iterator;
        private Batch(RecordReader.RecordIterator<InternalRow> iterator) { this.iterator = iterator; }
        @Override public void close() { iterator.releaseBatch(); }
    }
}
