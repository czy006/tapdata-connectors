package io.tapdata.connector.paimon.service;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonRuntimeTableFactoryTest {

    @Test
    void createsInMemoryCopyWithoutMutatingCatalogOptionsOrPhysicalIdentity() {
        FileStoreTable source = mock(FileStoreTable.class);
        FileStoreTable runtime = mock(FileStoreTable.class);
        FileIO fileIO = mock(FileIO.class);
        Path location = new Path("file:///tmp/table");
        Map<String, String> sourceOptions = new LinkedHashMap<>();
        sourceOptions.put(CoreOptions.FILE_READER_ASYNC_ENABLED.key(), "true");
        when(source.options()).thenReturn(Collections.unmodifiableMap(sourceOptions));
        when(source.location()).thenReturn(location);
        when(runtime.location()).thenReturn(location);
        when(source.fileIO()).thenReturn(fileIO);
        when(runtime.fileIO()).thenReturn(fileIO);
        when(source.copy(anyMap())).thenReturn(runtime);
        CoreOptions runtimeOptions = mock(CoreOptions.class);
        when(runtime.coreOptions()).thenReturn(runtimeOptions);
        when(runtimeOptions.fileReaderAsyncEnabled()).thenReturn(false);

        FileStoreTable result = PaimonRuntimeTableFactory.create(source);

        assertSame(runtime, result);
        assertEquals("true", sourceOptions.get(CoreOptions.FILE_READER_ASYNC_ENABLED.key()));
        verify(source)
                .copy(
                        Collections.singletonMap(
                                CoreOptions.FILE_READER_ASYNC_ENABLED.key(), "false"));
        assertFalse(result.coreOptions().fileReaderAsyncEnabled());
    }

    @Test
    void rejectsCopyThatChangesThePhysicalFileIoOwner() {
        FileStoreTable source = mock(FileStoreTable.class);
        FileStoreTable runtime = mock(FileStoreTable.class);
        Path location = new Path("file:///tmp/table");
        when(source.copy(anyMap())).thenReturn(runtime);
        when(source.location()).thenReturn(location);
        when(runtime.location()).thenReturn(location);
        when(source.fileIO()).thenReturn(mock(FileIO.class));
        when(runtime.fileIO()).thenReturn(mock(FileIO.class));

        assertThrows(
                IllegalStateException.class, () -> PaimonRuntimeTableFactory.create(source));
    }
}
