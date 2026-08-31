package io.tapdata.connector.paimon.service;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/** Creates operation-scoped table copies with Paimon's static async file reader disabled. */
public final class PaimonRuntimeTableFactory {

    private static final Map<String, String> REQUIRED_OPTIONS =
            Collections.singletonMap(CoreOptions.FILE_READER_ASYNC_ENABLED.key(), "false");

    private PaimonRuntimeTableFactory() {}

    /**
     * Returns an in-memory runtime copy. The catalog schema and physical table remain unchanged.
     */
    public static FileStoreTable create(FileStoreTable source) {
        Objects.requireNonNull(source, "source");
        FileStoreTable runtime =
                Objects.requireNonNull(source.copy(REQUIRED_OPTIONS), "runtime table copy");

        if (!Objects.equals(source.location(), runtime.location())) {
            throw new IllegalStateException("Runtime table copy changed the physical table path.");
        }
        if (source.fileIO() != runtime.fileIO()) {
            throw new IllegalStateException("Runtime table copy changed the physical FileIO owner.");
        }
        if (runtime.coreOptions().fileReaderAsyncEnabled()) {
            throw new IllegalStateException("Runtime table did not disable async file readers.");
        }
        return runtime;
    }
}
