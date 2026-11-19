package io.tapdata.connector.paimon.recovery;

import io.tapdata.connector.paimon.commit.PaimonCommitStateStore;
import io.tapdata.connector.paimon.service.PaimonStopController;
import io.tapdata.connector.paimon.service.PaimonStopResources;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LocalRecoveryCommitterTest {
    final PaimonStopResources resources = new PaimonStopResources();
    final PaimonStopController controller = new PaimonStopController("startup-test", 180, 120, 30);
    final TableCommitImpl nativeCommit = mock(TableCommitImpl.class);
    final ExecutorService maintenance = mock(ExecutorService.class);
    final PreparedMessageStore.Table stored = mock(PreparedMessageStore.Table.class);
    final IOException sealFailure = new IOException("failed to seal startup group");

    private LocalRecoveryTable pendingTable() throws Exception {
        return pendingTable(null);
    }

    private LocalRecoveryTable pendingTable(LocalRecoveryTable.TableLoader loader) throws Exception {
        controller.attachResourceRoot(resources);
        FileStoreTable table = mock(FileStoreTable.class, RETURNS_DEEP_STUBS);
        when(table.location()).thenReturn(new Path("file:/warehouse/orders"));
        String identity = LocalRecoveryTable.identity(table);
        when(stored.identity()).thenReturn(identity);
        when(stored.name()).thenReturn("orders");
        when(stored.commitUser()).thenReturn("recovering-user");
        when(stored.prepared()).thenReturn(1L);
        doThrow(sealFailure).when(stored).seal();
        PaimonCommitStateStore.Binding binding = mock(PaimonCommitStateStore.Binding.class, RETURNS_DEEP_STUBS);
        when(binding.commitUser()).thenReturn("recovering-user");
        StreamWriteBuilder builder = mock(StreamWriteBuilder.class);
        when(table.newStreamWriteBuilder()).thenReturn(builder);
        when(builder.withCommitUser("recovering-user")).thenReturn(builder);
        when(builder.newCommit()).thenReturn(nativeCommit);
        when(nativeCommit.getMaintainExecutor()).thenReturn(maintenance);
        when(maintenance.isTerminated()).thenReturn(true);
        return new LocalRecoveryTable(stored, binding, loader == null ? () -> table : loader, controller, table);
    }

    @Test
    void failedPublicationReleasesStartupScopeOnlyAfterNativeClose() throws Exception {
        LocalRecoveryTable recovery = pendingTable();
        doAnswer(call -> {
            assertEquals(1, resources.size());
            return null;
        }).when(nativeCommit).close();
        assertSame(sealFailure, assertThrows(IOException.class, () -> recovery.recover(resources)));
        org.mockito.InOrder order = inOrder(maintenance, nativeCommit);
        order.verify(maintenance).shutdown();
        order.verify(maintenance).isTerminated();
        order.verify(nativeCommit).close();
        assertTrue(resources.isEmpty());
        assertFalse(controller.isRetained());
    }

    @Test
    void unknownStartupCloseRetainsServiceRootEvenIfPublicationAlreadyFailed() throws Exception {
        LocalRecoveryTable recovery = pendingTable();
        IOException closeFailure = new IOException("cannot confirm native committer shutdown");
        doThrow(closeFailure).when(nativeCommit).close();
        assertSame(sealFailure, assertThrows(IOException.class, () -> recovery.recover(resources)));
        assertArrayEquals(new Throwable[]{closeFailure}, sealFailure.getSuppressed());
        assertTrue(controller.isRetained());
        assertEquals(1, resources.size());
        verify(stored, never()).cleanup();
    }

    @Test
    void pendingStartupRechecksPublishedHeadAfterMaintenanceBeforeReclaimingMessages() throws Exception {
        IOException inaccessible = new IOException("confirmed table no longer accessible");
        LocalRecoveryTable recovery = pendingTable(() -> {
            verify(nativeCommit).close();
            throw inaccessible;
        });
        doNothing().when(stored).seal();
        assertSame(inaccessible, assertThrows(IOException.class, () -> recovery.recover(resources)));
        verify(stored, never()).cleanup();
        assertTrue(resources.isEmpty());
    }
}
