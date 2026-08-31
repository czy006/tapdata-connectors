package io.tapdata.connector.paimon.service;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonDynamicBucketPreflightTest {

    @Test
    void successMustUseRuntimeThenTtlFreeCopyAndCloseInDependencyOrder() throws Exception {
        Fixture fixture = new Fixture();
        InternalRow row = GenericRow.of(1);
        when(fixture.batch.next()).thenReturn(row, null);
        when(fixture.reader.readBatch()).thenReturn(fixture.batch, null);

        fixture.validate();

        verify(fixture.factory).createRuntimeTable(fixture.source);
        verify(fixture.runtimeTable).copy(Collections.singletonMap(
                CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key(), null));
        verify(fixture.factory).createAssigner(fixture.validationTable);
        verify(fixture.factory).createBootstrapReader(fixture.validationTable);
        assertFalse(fixture.runtimeTable.coreOptions().fileReaderAsyncEnabled());
        assertFalse(fixture.validationTable.coreOptions().fileReaderAsyncEnabled());
        assertEquals("7 d", fixture.runtimeOptions.get(
                CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key()));

        InOrder order = inOrder(fixture.checker, fixture.batch, fixture.reader, fixture.ioManager);
        order.verify(fixture.checker).open(eq(0L), eq(fixture.ioManager), eq(1), eq(0), any());
        order.verify(fixture.checker).bootstrapKey(row);
        order.verify(fixture.batch).releaseBatch();
        order.verify(fixture.reader).close();
        order.verify(fixture.checker).endBoostrap(false);
        order.verify(fixture.checker).close();
        order.verify(fixture.ioManager).close();
    }

    @Test
    void openFailureMustRemainPrimaryAndSuppressCheckerCloseFailureWithoutClosingIo()
            throws Exception {
        Fixture fixture = new Fixture();
        IOException openFailure = new IOException("open failed");
        IOException closeFailure = new IOException("checker close failed");
        doThrow(openFailure).when(fixture.checker)
                .open(eq(0L), eq(fixture.ioManager), eq(1), eq(0), any());
        doThrow(closeFailure).when(fixture.checker).close();

        Exception thrown = assertThrows(Exception.class, fixture::validate);

        assertSame(openFailure, thrown);
        assertEquals(Collections.singletonList(closeFailure),
                java.util.Arrays.asList(thrown.getSuppressed()));
        verify(fixture.checker).close();
        verify(fixture.factory, never()).createBootstrapReader(any(FileStoreTable.class));
        verify(fixture.ioManager, never()).close();
    }

    @Test
    void iterationFailureMustRetainCleanupFailuresAndRetainIoOwner()
            throws Exception {
        Fixture fixture = new Fixture();
        IOException readFailure = new IOException("batch iteration failed");
        RuntimeException releaseFailure = new RuntimeException("batch release failed");
        IOException readerCloseFailure = new IOException("reader close failed");
        when(fixture.batch.next()).thenThrow(readFailure);
        doThrow(releaseFailure).when(fixture.batch).releaseBatch();
        doThrow(readerCloseFailure).when(fixture.reader).close();
        when(fixture.reader.readBatch()).thenReturn(fixture.batch);

        Exception thrown = assertThrows(Exception.class, fixture::validate);

        assertSame(readFailure, thrown);
        assertEquals(java.util.Arrays.asList(releaseFailure, readerCloseFailure),
                java.util.Arrays.asList(thrown.getSuppressed()));
        verify(fixture.batch).releaseBatch();
        verify(fixture.checker).close();
        verify(fixture.ioManager, never()).close();
    }

    @Test
    void endBootstrapFailureMustRemainPrimaryAndBlockIoCloseWhenCheckerCloseFails()
            throws Exception {
        Fixture fixture = new Fixture();
        IOException endFailure = new IOException("end failed");
        IOException closeFailure = new IOException("checker close failed");
        doThrow(endFailure).when(fixture.checker).endBoostrap(false);
        doThrow(closeFailure).when(fixture.checker).close();

        Exception thrown = assertThrows(Exception.class, fixture::validate);

        assertSame(endFailure, thrown);
        assertEquals(Collections.singletonList(closeFailure),
                java.util.Arrays.asList(thrown.getSuppressed()));
        verify(fixture.reader).close();
        verify(fixture.checker).close();
        verify(fixture.ioManager, never()).close();
    }

    @Test
    void snapshotFailureAfterStagingMustCloseCheckerBeforeIoWithoutOpeningChecker()
            throws Exception {
        Fixture fixture = new Fixture();
        RuntimeException snapshotFailure = new RuntimeException("snapshot failed");
        when(fixture.snapshotManager.latestSnapshotIdFromFileSystem())
                .thenThrow(snapshotFailure);

        assertSame(snapshotFailure, assertThrows(Exception.class, fixture::validate));

        verify(fixture.checker, never())
                .open(eq(0L), eq(fixture.ioManager), eq(1), eq(0), any());
        InOrder order = inOrder(fixture.checker, fixture.ioManager);
        order.verify(fixture.checker).close();
        order.verify(fixture.ioManager).close();
    }

    @Test
    void ioCloseFailureMustPropagateSynchronouslyAfterSuccessfulCheckerClose()
            throws Exception {
        Fixture fixture = new Fixture();
        RuntimeException closeFailure = new RuntimeException("io close failed");
        doThrow(closeFailure).when(fixture.ioManager).close();

        assertSame(closeFailure, assertThrows(Exception.class, fixture::validate));

        verify(fixture.checker).close();
        verify(fixture.ioManager).close();
    }

    @Test
    void productionPreflightMustNotReferenceParallelIndexBootstrap() throws Exception {
        assertClassOmits(
                PaimonDynamicBucketPreflight.class,
                "org/apache/paimon/crosspartition/IndexBootstrap",
                "org/apache/paimon/io/SplitsParallelReadUtil",
                "org/apache/paimon/utils/ParallelExecution");
    }

    private static void assertClassOmits(Class<?> type, String... forbiddenNames)
            throws IOException {
        String resource = "/" + type.getName().replace('.', '/') + ".class";
        byte[] bytecode;
        try (InputStream input = type.getResourceAsStream(resource);
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            if (input == null) {
                throw new IOException("Missing class resource " + resource);
            }
            byte[] buffer = new byte[4096];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                output.write(buffer, 0, read);
            }
            bytecode = output.toByteArray();
        }
        String constantPool = new String(bytecode, StandardCharsets.ISO_8859_1);
        for (String forbiddenName : forbiddenNames) {
            assertFalse(
                    constantPool.contains(forbiddenName),
                    () -> type.getName() + " references forbidden " + forbiddenName);
        }
    }

    private static final class Fixture {
        private final FileStoreTable source = mock(FileStoreTable.class);
        private final FileStoreTable runtimeTable = mock(FileStoreTable.class);
        private final FileStoreTable validationTable = mock(FileStoreTable.class);
        private final CoreOptions runtimeCoreOptions = mock(CoreOptions.class);
        private final CoreOptions validationCoreOptions = mock(CoreOptions.class);
        private final SnapshotManager snapshotManager = mock(SnapshotManager.class);
        private final IOManager ioManager = mock(IOManager.class);
        private final GlobalIndexAssigner checker = mock(GlobalIndexAssigner.class);
        private final RecordReader<InternalRow> reader = mock(RecordReader.class);
        private final RecordReader.RecordIterator<InternalRow> batch =
                mock(RecordReader.RecordIterator.class);
        private final PaimonDynamicBucketPreflight.PreflightRuntimeFactory factory =
                mock(PaimonDynamicBucketPreflight.PreflightRuntimeFactory.class);
        private final Map<String, String> runtimeOptions = new LinkedHashMap<>();

        private Fixture() throws Exception {
            runtimeOptions.put(CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key(), "7 d");
            runtimeOptions.put(CoreOptions.FILE_READER_ASYNC_ENABLED.key(), "false");
            when(factory.createRuntimeTable(source)).thenReturn(runtimeTable);
            when(runtimeTable.coreOptions()).thenReturn(runtimeCoreOptions);
            when(runtimeTable.options()).thenReturn(runtimeOptions);
            when(runtimeCoreOptions.fileReaderAsyncEnabled()).thenReturn(false);
            when(runtimeTable.copy(Collections.singletonMap(
                    CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key(), null)))
                    .thenReturn(validationTable);
            when(validationTable.coreOptions()).thenReturn(validationCoreOptions);
            when(validationCoreOptions.fileReaderAsyncEnabled()).thenReturn(false);
            when(validationTable.snapshotManager()).thenReturn(snapshotManager);
            when(snapshotManager.latestSnapshotIdFromFileSystem()).thenReturn(11L, 11L);
            when(factory.createIoOwner("tmp")).thenReturn(
                    new PaimonDynamicBucketPreflight.PreflightIoOwner(
                            ioManager, Collections.emptyList()));
            when(factory.createAssigner(validationTable)).thenReturn(checker);
            when(factory.createBootstrapReader(validationTable)).thenReturn(reader);
            when(reader.readBatch()).thenReturn(null);
        }

        private void validate() throws Exception {
            PaimonDynamicBucketPreflight.validateExactPrimaryKeyUniqueness(
                    "default.t", source, "tmp", factory);
        }
    }
}
