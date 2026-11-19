package io.tapdata.connector.paimon.write;

import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;
import java.util.Set;

/** The durable, ordered commit messages owned by one physical table generation. */
public interface PaimonRecoveryTable {

    /**
     * Persists the complete result of one native prepare. A successful return is the batch's
     * durability boundary. The native writer may have drained its increments even on failure.
     */
    void stage(long prepareId, List<CommitMessage> messages, boolean initial, Set<String> sourceLanes)
            throws Exception;

    /**
     * Seals and publishes the current prefix, or resumes the already sealed prefix with the same
     * identities and bytes. Return the last locally confirmed commit id (-1 if none).
     * An explicit empty initial completion is idempotent across calls and process restarts.
     */
    long publish(PaimonTableCommitter committer) throws Exception;

    /** Reclaims locally confirmed messages only after their commit identity is visible in task KV. */
    void discardConfirmed() throws Exception;
}
