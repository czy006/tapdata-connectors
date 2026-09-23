package io.tapdata.connector.paimon.read;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import java.io.IOException;

/**
 * Wraps a Service-managed reader with session admission and Consumer health checks before readBatch/next.
 * A null readBatch ends the reader; a null RecordIterator.next ends only the current batch.
 * releaseBatch/close bypass read admission so cancellation cannot prevent batch and file-handle cleanup.
 * @see <a href="https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-common/src/main/java/org/apache/paimon/reader/RecordReader.java#L45">Paimon reader and batch lifecycle contract</a>
 */
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
