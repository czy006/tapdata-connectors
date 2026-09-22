package io.tapdata.connector.paimon.read;

import io.tapdata.entity.event.TapEvent;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import java.util.concurrent.CancellationException;
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
        private Item(String table, TapEvent event, long before, long after) {
            this.table = table; this.event = event; this.before = before; this.after = after;
        }
        public static Item event(String table, TapEvent event) { return new Item(table, event, 0, 0); }
        public static Item finished(String table, long before, long after) { return new Item(table, null, before, after); }
        public boolean isFinished() { return event == null; }
    }

    private PaimonStreamReadCoordinator() {}

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
