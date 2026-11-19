package io.tapdata.connector.paimon.recovery;

import io.tapdata.connector.paimon.commit.PaimonCommitStateStore;
import io.tapdata.connector.paimon.service.PaimonStopController;
import io.tapdata.connector.paimon.service.PaimonStopResources;
import io.tapdata.connector.paimon.write.PaimonRecoveryTable;
import io.tapdata.connector.paimon.write.PaimonStreamTableCommitter;
import io.tapdata.connector.paimon.write.PaimonTableCommitter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Connects one table's durable ordered input to its Paimon commit identity. Callers serialize
 * access with the service's table lock. The writer is rebuilt only after recover() completes.
 */
public final class LocalRecoveryTable implements PaimonRecoveryTable {
    @FunctionalInterface
    public interface TableLoader {
        FileStoreTable load() throws Exception;
    }

    private final PreparedMessageStore.Table stored;
    private final PaimonCommitStateStore.Binding binding;
    private final TableLoader loader;
    private final PaimonStopController controller;
    private final FileStoreTable writerTable;

    public LocalRecoveryTable(PreparedMessageStore.Table stored,
                              PaimonCommitStateStore.Binding binding,
                              TableLoader loader, PaimonStopController controller,
                              FileStoreTable writerTable) {
        this.stored = stored;
        this.binding = binding;
        this.loader = loader;
        this.controller = controller;
        this.writerTable = writerTable;
        if (!stored.identity().equals(identity(writerTable)) || !stored.commitUser().equals(binding.commitUser())) {
            throw new IllegalStateException("Paimon table identity or commit user differs from local recovery state");
        }
        if (binding.nextCommitIdentifier() > stored.confirmedNextIdentifier()) {
            throw new IllegalStateException("Task commit identifier is ahead of locally confirmed publication");
        }
    }

    public static String identity(FileStoreTable table) {
        return PaimonCommitStateStore.physicalTableHash(table.location().toUri().toString())
                + "/" + table.snapshotManager().branch() + "/" + table.uuid() + "/" + table.schema().id();
    }

    public FileStoreTable writerTable() {
        return writerTable;
    }

    @Override
    public void stage(long prepareId, List<CommitMessage> messages, boolean initial, Set<String> sourceLanes)
            throws Exception {
        controller.run("register Paimon prepared files",
                () -> stored.append(prepareId, !initial, sourceLanes, messages));
    }

    /** No DML or heartbeat is admitted while this runs, including when the source stays silent. */
    public void recover(PaimonStopResources resources) throws Exception {
        if (stored.prepared() > stored.published()) {
            publishForStartup(resources);
        }
        // The startup committer's maintenance has now terminated. Recheck the catalog generation
        // and final snapshot before reclaiming any local proof or opening the table to writers.
        assertConfirmedHead();
        syncConfirmed();
        discardConfirmed();
    }

    private void publishForStartup(PaimonStopResources resources) throws Exception {
        // Recovery precedes writer creation. Register this temporary committer with the same
        // service resource ledger and stop deadline that guard the normal writer's committer.
        PaimonStopResources.Scope scope = resources.scope("recover " + stored.name(), controller);
        PaimonTableCommitter committer;
        try {
            StreamTableCommit nativeCommit = scope.create("allocate recovery committer",
                    () -> writerTable.newStreamWriteBuilder().withCommitUser(stored.commitUser()).newCommit());
            committer = new PaimonStreamTableCommitter(nativeCommit, scope);
        } catch (Exception | Error allocation) {
            if (scope.hasResources()) {
                scope.retain(allocation);
            } else {
                scope.completed();
            }
            throw allocation;
        }
        // The wrapper waits for TableCommitImpl's maintenance executor before returning. Its
        // native close only calls shutdownNow; it does not wait for the executor to terminate.
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L398-L410
        try (AutoCloseable owner = () -> {
            try {
                committer.close();
                controller.publish("complete recovery committer close", () -> {
                    scope.completed();
                    return null;
                });
            } catch (Exception | Error closeFailure) {
                scope.retain(closeFailure);
                throw closeFailure;
            }
        }) {
            publish(committer);
        }
    }

    private void assertConfirmedHead() throws Exception {
        FileStoreTable current = loader.load();
        if (!stored.identity().equals(identity(current))
                || !stored.anchor().equals(PublicationVerifier.fingerprint(current))) {
            throw new IllegalStateException("Paimon table diverged from the confirmed snapshot: " + stored.name());
        }
    }

    @Override
    public long publish(PaimonTableCommitter committer) throws Exception {
        // A previous publish may have confirmed a group just before its KV synchronization failed.
        syncConfirmed();
        if (stored.prepared() == 0) {
            // Explicit empty-table completion. A successful empty APPEND is recognizable on restart.
            stage(stored.nextIdentifier(), Collections.emptyList(), true, Collections.emptySet());
        }
        controller.run("seal Paimon publication", stored::seal);
        PreparedMessageStore.Group group;
        while ((group = stored.nextGroup()) != null) {
            FileStoreTable current = loader.load();
            if (!stored.identity().equals(identity(current))) {
                throw new IllegalStateException("Paimon table generation changed before publish: " + stored.name());
            }
            PreparedMessageStore.Group publishing = group;
            String head = controller.call("publish Paimon prepared files", () -> PublicationVerifier.publish(
                    current, stored.commitUser(), publishing.identifier(), publishing.messages(), stored.anchor(),
                    committer));
            controller.run("confirm Paimon published prefix", () -> stored.confirm(publishing, head));
            syncConfirmed();
        }
        return stored.confirmedNextIdentifier() - 1;
    }

    private void syncConfirmed() throws Exception {
        controller.run("synchronize Paimon task-state identifier",
                () -> binding.store().save(stored.confirmedNextIdentifier()));
    }

    @Override
    public void discardConfirmed() throws Exception {
        controller.run("clean confirmed Paimon messages", stored::cleanup);
    }
}
