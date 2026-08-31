package io.tapdata.connector.paimon.write;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableWriteImpl;

import java.util.Objects;

/**
 * Staged ownership boundary for a concrete Paimon writer whose external resources are injected.
 *
 * <p>The unprepared {@link TableWriteImpl} is confined to the lexical construction scope of
 * {@link #bind}. The holder deliberately exposes no write, restore, or prepare method. The bucket
 * strategy factory may transfer the writer exactly once after both the optional connector-owned
 * IO manager and the connector-owned compaction executor have been bound.
 */
public final class PaimonPreparedWriterRuntime {

    private final String generationIdentity;
    private final FileStoreTable runtimeTable;
    private final TableWriteImpl<?> writer;
    private final IOManager ioManager;
    private final PaimonCompactionRuntime compactionRuntime;
    private boolean writerTransferred;

    private PaimonPreparedWriterRuntime(
            String generationIdentity,
            FileStoreTable runtimeTable,
            TableWriteImpl<?> writer,
            IOManager ioManager,
            PaimonCompactionRuntime compactionRuntime) {
        this.generationIdentity = generationIdentity;
        this.runtimeTable = runtimeTable;
        this.writer = writer;
        this.ioManager = ioManager;
        this.compactionRuntime = compactionRuntime;
    }

    static PaimonPreparedWriterRuntime bind(
            String generationIdentity,
            FileStoreTable runtimeTable,
            TableWriteImpl<?> rawWriter,
            IOManager ioManager,
            PaimonCompactionRuntime compactionRuntime) {
        String checkedIdentity = requireNonBlank(generationIdentity, "generationIdentity");
        FileStoreTable checkedTable = Objects.requireNonNull(runtimeTable, "runtimeTable");
        TableWriteImpl<?> checkedWriter = Objects.requireNonNull(rawWriter, "rawWriter");
        PaimonCompactionRuntime checkedRuntime =
                Objects.requireNonNull(compactionRuntime, "compactionRuntime");
        requireNoConstructionSubmission(checkedRuntime, checkedIdentity, "before binding");

        if (ioManager != null) {
            checkedWriter.withIOManager(ioManager);
        }
        checkedWriter.withCompactExecutor(checkedRuntime.executor());
        requireNoConstructionSubmission(checkedRuntime, checkedIdentity, "after binding");

        return new PaimonPreparedWriterRuntime(
                checkedIdentity, checkedTable, checkedWriter, ioManager, checkedRuntime);
    }

    public FileStoreTable runtimeTable() {
        return runtimeTable;
    }

    public IOManager ioManager() {
        return ioManager;
    }

    public synchronized StreamTableWrite transferWriterToStrategy() {
        if (writerTransferred) {
            throw new IllegalStateException(
                    "Prepared Paimon writer was already transferred for " + generationIdentity);
        }
        requireNoConstructionSubmission(
                compactionRuntime, generationIdentity, "before strategy transfer");
        writerTransferred = true;
        return writer;
    }

    public synchronized boolean writerTransferred() {
        return writerTransferred;
    }

    public boolean compactionSubmissionObserved() {
        return compactionRuntime.submissionObserved();
    }

    String generationIdentity() {
        return generationIdentity;
    }

    PaimonCompactionRuntime compactionRuntime() {
        return compactionRuntime;
    }

    private static void requireNoConstructionSubmission(
            PaimonCompactionRuntime runtime, String generationIdentity, String phase) {
        if (runtime.submissionObserved()) {
            throw new IllegalStateException(
                    "Paimon compaction submission was observed "
                            + phase
                            + " for unpublished generation "
                            + generationIdentity);
        }
    }

    private static String requireNonBlank(String value, String name) {
        String checked = Objects.requireNonNull(value, name).trim();
        if (checked.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return checked;
    }
}
