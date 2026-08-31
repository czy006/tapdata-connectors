package io.tapdata.connector.paimon.service;

import org.apache.paimon.PaimonRuntimeCapabilities;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.util.Objects;

/** Fail-fast compatibility and exact FileIO-ownership gate for the Connector runtime. */
final class PaimonRuntimeGate {

    private PaimonRuntimeGate() {}

    /** Applies ownership options before {@code CatalogContext} or any FileIO probe is created. */
    static void configureRequiredOptions(Options options) {
        Objects.requireNonNull(options, "options");
        options.set(HadoopFileIO.OWNED_FILE_SYSTEM_ENABLED, "true");
        options.set(CatalogOptions.RESOLVING_FILE_IO_ENABLED, false);
    }

    /**
     * Verifies the complete patched runtime and the actual FileIO retained by the root catalog.
     *
     * <p>Local and native object-store FileIO implementations do not own a Hadoop FileSystem and
     * are therefore not applicable to the Hadoop-specific identity proof. A resolved
     * {@link HadoopFileIO}, however, must prove that this exact instance owns the warehouse entry.
     */
    static FileIO verify(Catalog catalog, Path warehouse) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(warehouse, "warehouse");

        PaimonRuntimeCapabilities capabilities = PaimonRuntimeCapabilities.get();
        capabilities.requireComplete();

        FileIO fileIO = catalogFileIO(catalog);
        if (fileIO instanceof ResolvingFileIO) {
            throw new IllegalStateException(
                    "ResolvingFileIO is forbidden because it hides the resolved FileIO owner.");
        }
        if (fileIO instanceof HadoopFileIO) {
            capabilities.requireHadoopOwnedFileSystem(fileIO, warehouse);
        }
        return fileIO;
    }

    /** Returns the exact FileIO owned by the root filesystem catalog, without reflection. */
    static FileIO catalogFileIO(Catalog catalog) {
        Catalog root = DelegateCatalog.rootCatalog(Objects.requireNonNull(catalog, "catalog"));
        if (!(root instanceof FileSystemCatalog)) {
            throw new IllegalStateException(
                    "Paimon Plus requires a FileSystemCatalog for exact FileIO ownership, but found "
                            + root.getClass().getName());
        }
        FileIO fileIO = ((FileSystemCatalog) root).fileIO();
        if (fileIO == null) {
            throw new IllegalStateException("FileSystemCatalog returned a null FileIO.");
        }
        return fileIO;
    }
}
