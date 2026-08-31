package io.tapdata.connector.paimon.service;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonRuntimeGateTest {

    @Test
    void requiredOptionsAreAppliedBeforeCatalogCreation() {
        Options options = new Options();

        PaimonRuntimeGate.configureRequiredOptions(options);

        assertTrue(options.getBoolean(HadoopFileIO.OWNED_FILE_SYSTEM_ENABLED, false));
        assertFalse(options.get(CatalogOptions.RESOLVING_FILE_IO_ENABLED));
    }

    @Test
    void verifiesTheActualCatalogOwnedHadoopFileSystem() {
        HadoopFileIO fileIO = mock(HadoopFileIO.class);
        FileSystemCatalog catalog = mock(FileSystemCatalog.class);
        Path warehouse = new Path("hdfs://namenode:8020/warehouse");
        when(catalog.fileIO()).thenReturn(fileIO);

        FileIO verified = PaimonRuntimeGate.verify(catalog, warehouse);

        assertSame(fileIO, verified);
        verify(fileIO).requireOwnedFileSystem(warehouse);
    }

    @Test
    void localAndNativeFileIoDoNotPretendToOwnHadoopFileSystems() {
        FileIO fileIO = mock(FileIO.class);
        FileSystemCatalog catalog = mock(FileSystemCatalog.class);
        when(catalog.fileIO()).thenReturn(fileIO);

        assertSame(
                fileIO,
                PaimonRuntimeGate.verify(catalog, new Path("file:///tmp/warehouse")));
    }

    @Test
    void resolvingFileIoIsRejectedBecauseItCanHideTheResolvedOwner() {
        FileSystemCatalog catalog = mock(FileSystemCatalog.class);
        when(catalog.fileIO()).thenReturn(mock(ResolvingFileIO.class));

        IllegalStateException failure =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                PaimonRuntimeGate.verify(
                                        catalog, new Path("s3a://bucket/warehouse")));
        assertTrue(failure.getMessage().contains("ResolvingFileIO"));
    }

    @Test
    void nonFileSystemCatalogIsRejectedInsteadOfSkippingTheOwnershipGate() {
        Catalog catalog = mock(Catalog.class);

        IllegalStateException failure =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                PaimonRuntimeGate.verify(
                                        catalog, new Path("hdfs://namenode:8020/warehouse")));
        assertTrue(failure.getMessage().contains("FileSystemCatalog"));
    }
}
