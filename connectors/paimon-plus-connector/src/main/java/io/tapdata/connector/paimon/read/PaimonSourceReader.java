package io.tapdata.connector.paimon.read;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import java.io.IOException;

/** Adds source cancellation admission to the Service-owned reader without blocking cleanup. */
public final class PaimonSourceReader implements RecordReader<InternalRow> {
    private final RecordReader<InternalRow> delegate;
    private final PaimonStreamReadSession session;
    private final Runnable health;
    public PaimonSourceReader(RecordReader<InternalRow> delegate, PaimonStreamReadSession session, Runnable health) {
        this.delegate = delegate; this.session = session; this.health = health;
    }
    @Override public RecordIterator<InternalRow> readBatch() throws IOException {
        try (PaimonStreamReadSession.Admission ignored = session.admit()) {
            health.run();
            RecordIterator<InternalRow> batch = delegate.readBatch();
            if (batch == null) { return null; }
            return new RecordIterator<InternalRow>() {
                @Override public InternalRow next() throws IOException {
                    try (PaimonStreamReadSession.Admission ignored = session.admit()) { health.run(); return batch.next(); }
                }
                // Paimon 1.3 RecordReader contract: release every returned batch, including cancellation.
                // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-common/src/main/java/org/apache/paimon/reader/RecordReader.java#L59
                @Override public void releaseBatch() { batch.releaseBatch(); }
            };
        }
    }
    @Override public void close() throws IOException { delegate.close(); }
}
