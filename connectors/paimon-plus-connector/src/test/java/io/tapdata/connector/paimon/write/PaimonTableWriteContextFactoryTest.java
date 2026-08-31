package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyFactory;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractResolver;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonTableWriteContextFactoryTest {

    private static final String COMMIT_USER = "factory-test-user";

    @Test
    void successfulContextMustOwnAndCloseWriterBeforeCommitterExactlyOnce() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);

        PaimonTableWriteContext context = fixture.create();
        org.junit.jupiter.api.Assertions.assertEquals(
                BucketMode.HASH_FIXED, context.writeSemanticContract().bucketMode());
        context.close();
        context.close();

        InOrder order = inOrder(fixture.writer, fixture.committer);
        order.verify(fixture.writer).close();
        order.verify(fixture.committer).close();
        verify(fixture.writer).close();
        verify(fixture.committer).close();
        verify(fixture.table).newWrite(COMMIT_USER);
        verify(fixture.table).newCommit(COMMIT_USER);
        verify(fixture.committer).ignoreEmptyCommit(false);
    }

    @Test
    void preResolvedContractMustBeSharedByContextAndStrategy() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        PaimonWriteSemanticContract contract =
                PaimonWriteSemanticContractResolver.resolve("default.t", fixture.table);

        try (PaimonTableWriteContext context =
                PaimonTableWriteContextFactory.create(
                        "default.t",
                        "t",
                        fixture.table,
                        COMMIT_USER,
                        null,
                        0L,
                        PaimonTableWriteContext.CommitStateStore.NOOP,
                        fixture.runtimeFactory,
                        contract)) {
            assertSame(contract, context.writeSemanticContract());
        }
    }

    @Test
    void committerCreationFailureMustCloseAlreadyCreatedWriter() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        RuntimeException failure = new RuntimeException("committer creation failed");
        when(fixture.table.newCommit(COMMIT_USER)).thenThrow(failure);

        RuntimeException thrown = assertThrows(RuntimeException.class, fixture::create);

        assertSame(failure, thrown);
        verify(fixture.writer).close();
    }

    @Test
    void strategyConstructionFailureMustCloseCommitterThenWriter() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_DYNAMIC);
        TableSchema schema =
                TableSchema.create(
                        0L,
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("value", DataTypes.STRING())
                                .primaryKey("id")
                                .option("bucket", "-1")
                                .build());
        when(fixture.table.schema()).thenReturn(schema);
        when(fixture.table.primaryKeys()).thenReturn(Collections.singletonList("id"));
        when(fixture.table.partitionKeys()).thenReturn(Collections.emptyList());
        RuntimeException failure = new RuntimeException("assigner creation failed");
        when(fixture.runtimeFactory.createHashBucketAssigner(fixture.table, COMMIT_USER))
                .thenThrow(failure);

        RuntimeException thrown = assertThrows(RuntimeException.class, fixture::create);

        assertSame(failure, thrown);
        InOrder order = inOrder(fixture.committer, fixture.writer);
        order.verify(fixture.committer).close();
        order.verify(fixture.writer).close();
    }

    @Test
    void invalidIdentifierMustFailBeforeAllocatingPaimonResources() {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        PaimonTableWriteContextFactory.create(
                                "default.t",
                                "t",
                                fixture.table,
                                COMMIT_USER,
                                null,
                                -1L,
                                PaimonTableWriteContext.CommitStateStore.NOOP,
                                fixture.runtimeFactory));

        verify(fixture.table, never()).newWrite(any());
    }

    @Test
    void unsupportedCrossPartitionMergeEngineMustFailBeforeAllocatingPaimonResources() {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        TableSchema schema =
                TableSchema.create(
                        0L,
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("pt", DataTypes.STRING())
                                .partitionKeys("pt")
                                .primaryKey("id")
                                .build());
        when(fixture.table.schema()).thenReturn(schema);
        when(fixture.table.rowType()).thenReturn(schema.logicalRowType());
        when(fixture.table.primaryKeys()).thenReturn(schema.primaryKeys());
        when(fixture.table.partitionKeys()).thenReturn(schema.partitionKeys());
        when(fixture.coreOptions.mergeEngine()).thenReturn(MergeEngine.FIRST_ROW);

        PaimonFatalWriteException thrown =
                assertThrows(PaimonFatalWriteException.class, fixture::create);

        org.junit.jupiter.api.Assertions.assertTrue(
                thrown.getMessage()
                        .contains("PAIMON_UNSUPPORTED_CROSS_PARTITION_MERGE_ENGINE"));
        verify(fixture.table, never()).newWrite(any());
    }

    @Test
    void spillableWriterMustReceiveIoManagerWhenConfiguredTmpDirsAreBlank() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        when(fixture.coreOptions.writeBufferSpillable()).thenReturn(true);

        try (PaimonTableWriteContext ignored = fixture.create()) {
            verify(fixture.writer).withIOManager(any(IOManager.class));
        }
    }

    @Test
    void preparedBoundaryMustBindBeforeTransferAndFirstWriterUse() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        IOManager ioManager = mock(IOManager.class);
        PaimonCompactionRuntime compactionRuntime =
                new PaimonCompactionRuntime("prepared-boundary");
        PaimonWriteSemanticContract contract =
                PaimonWriteSemanticContractResolver.resolve("default.t", fixture.table);
        when(fixture.writer.prepareCommit(false, 0L)).thenReturn(Collections.emptyList());

        PaimonPreparedWriterRuntime prepared =
                PaimonPreparedWriterRuntime.bind(
                        "default.t/1",
                        fixture.table,
                        fixture.writer,
                        ioManager,
                        compactionRuntime);
        assertFalse(prepared.writerTransferred());
        assertFalse(prepared.compactionSubmissionObserved());

        PaimonBucketWriterStrategy strategy =
                PaimonBucketWriterStrategyFactory.create(
                        prepared,
                        "default.t",
                        COMMIT_USER,
                        contract,
                        fixture.runtimeFactory);
        assertTrue(prepared.writerTransferred());
        assertFalse(prepared.compactionSubmissionObserved());
        assertThrows(IllegalStateException.class, prepared::transferWriterToStrategy);
        strategy.prepareCommit(0L);

        InOrder order = inOrder(fixture.writer);
        order.verify(fixture.writer).withIOManager(ioManager);
        order.verify(fixture.writer).withCompactExecutor(any(ExecutorService.class));
        order.verify(fixture.writer).prepareCommit(false, 0L);

        compactionRuntime.beginShutdown();
        assertTrue(compactionRuntime.awaitTermination(Long.MAX_VALUE));
        strategy.close();
    }

    @Test
    void constructionSubmissionMustDrainBeforeWriterClose() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch allowWorkerReturn = new CountDownLatch(1);
        when(fixture.writer.withCompactExecutor(any(ExecutorService.class)))
                .thenAnswer(
                        invocation -> {
                            ExecutorService executor = invocation.getArgument(0);
                            executor.execute(
                                    () -> {
                                        workerStarted.countDown();
                                        try {
                                            allowWorkerReturn.await();
                                        } catch (InterruptedException interrupted) {
                                            Thread.currentThread().interrupt();
                                        }
                                    });
                            return fixture.writer;
                        });
        ExecutorService factoryCaller = Executors.newSingleThreadExecutor();
        try {
            Future<PaimonTableWriteContext> result = factoryCaller.submit(fixture::create);
            assertTrue(workerStarted.await(5L, TimeUnit.SECONDS));
            verify(fixture.writer, never()).close();

            allowWorkerReturn.countDown();
            ExecutionException failure =
                    assertThrows(
                            ExecutionException.class,
                            () -> result.get(5L, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof IllegalStateException);
            assertTrue(failure.getCause().getMessage().contains("after binding"));
            verify(fixture.writer).close();
        } finally {
            allowWorkerReturn.countDown();
            factoryCaller.shutdownNow();
            assertTrue(factoryCaller.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    private static final class Fixture {
        private final FileStoreTable table = mock(FileStoreTable.class);
        private final CoreOptions coreOptions = mock(CoreOptions.class);
        private final TableWriteImpl writer = mock(TableWriteImpl.class);
        private final TableCommitImpl committer = mock(TableCommitImpl.class);
        private final PaimonBucketWriterRuntimeFactory runtimeFactory =
                mock(PaimonBucketWriterRuntimeFactory.class);

        private Fixture(BucketMode mode) {
            TableSchema schema =
                    TableSchema.create(
                            0L,
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .column("value", DataTypes.STRING())
                                    .primaryKey("id")
                                    .build());
            when(table.bucketMode()).thenReturn(mode);
            when(table.coreOptions()).thenReturn(coreOptions);
            when(table.schema()).thenReturn(schema);
            when(table.rowType()).thenReturn(schema.logicalRowType());
            when(table.primaryKeys()).thenReturn(schema.primaryKeys());
            when(table.partitionKeys()).thenReturn(schema.partitionKeys());
            when(coreOptions.mergeEngine()).thenReturn(MergeEngine.DEDUPLICATE);
            when(coreOptions.changelogProducer()).thenReturn(ChangelogProducer.NONE);
            when(coreOptions.rowkindField()).thenReturn(Optional.empty());
            when(table.newWrite(COMMIT_USER)).thenReturn(writer);
            when(writer.withIOManager(any(IOManager.class))).thenReturn(writer);
            when(writer.withCompactExecutor(any(ExecutorService.class))).thenReturn(writer);
            when(table.newCommit(COMMIT_USER)).thenReturn(committer);
            when(committer.ignoreEmptyCommit(false)).thenReturn(committer);
        }

        private PaimonTableWriteContext create() throws Exception {
            return PaimonTableWriteContextFactory.create(
                    "default.t",
                    "t",
                    table,
                    COMMIT_USER,
                    null,
                    0L,
                    PaimonTableWriteContext.CommitStateStore.NOOP,
                    runtimeFactory);
        }
    }
}
