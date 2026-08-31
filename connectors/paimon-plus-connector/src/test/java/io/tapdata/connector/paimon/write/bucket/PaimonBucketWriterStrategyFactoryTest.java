package io.tapdata.connector.paimon.write.bucket;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractTestFactory;
import io.tapdata.connector.paimon.write.PaimonPreparedWriterRuntime;

import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonBucketWriterStrategyFactoryTest {

    @Test
    void supportedModesMustExactlyMatchDependencyEnumAndBeImmutable() {
        Set<BucketMode> modes = PaimonBucketWriterStrategyFactory.supportedModes();

        assertEquals(EnumSet.allOf(BucketMode.class), modes);
        assertThrows(UnsupportedOperationException.class, () -> modes.remove(BucketMode.HASH_FIXED));
    }

    @Test
    void factoryBytecodeMustExposeOnlyPreparedWriterCreateEntry() {
        Method[] createMethods =
                Arrays.stream(PaimonBucketWriterStrategyFactory.class.getDeclaredMethods())
                        .filter(method -> method.getName().equals("create"))
                        .toArray(Method[]::new);

        assertEquals(1, createMethods.length);
        assertTrue(Modifier.isPublic(createMethods[0].getModifiers()));
        assertTrue(Modifier.isStatic(createMethods[0].getModifiers()));
        assertEquals(
                Arrays.asList(
                        PaimonPreparedWriterRuntime.class,
                        String.class,
                        String.class,
                        PaimonWriteSemanticContract.class,
                        PaimonBucketWriterRuntimeFactory.class),
                Arrays.asList(createMethods[0].getParameterTypes()));
        assertFalse(
                Arrays.asList(createMethods[0].getParameterTypes())
                        .contains(PaimonBucketWriterStrategyContext.class));
        assertFalse(
                Arrays.asList(createMethods[0].getParameterTypes())
                        .contains(TableWriteImpl.class));
    }

    @Test
    void allModesMustMapToDedicatedConcreteStrategies() throws Exception {
        Fixture fixture = new Fixture();

        fixture.assertStrategy(BucketMode.HASH_FIXED, HashFixedBucketWriterStrategy.class);
        fixture.assertStrategy(BucketMode.HASH_DYNAMIC, HashDynamicBucketWriterStrategy.class);
        fixture.assertStrategy(BucketMode.KEY_DYNAMIC, KeyDynamicBucketWriterStrategy.class);
        fixture.assertStrategy(BucketMode.POSTPONE_MODE, PostponeBucketWriterStrategy.class);
        fixture.assertStrategy(BucketMode.BUCKET_UNAWARE, BucketUnawareWriterStrategy.class);
    }

    @Test
    void onlyKeyDynamicMustRequireIoManager() {
        for (BucketMode mode : BucketMode.values()) {
            assertEquals(
                    mode == BucketMode.KEY_DYNAMIC,
                    PaimonBucketWriterStrategyFactory.requiresIoManager(mode));
        }
    }

    @Test
    void onlyDynamicModesMustRequireOrderedSingleWriterIngress() {
        for (BucketMode mode : BucketMode.values()) {
            boolean expected =
                    mode == BucketMode.HASH_DYNAMIC || mode == BucketMode.KEY_DYNAMIC;
            assertEquals(
                    expected,
                    PaimonBucketWriterStrategyFactory.requiresOrderedSingleWriterIngress(mode));
        }
        assertTrue(
                PaimonBucketWriterStrategyFactory.requiresOrderedSingleWriterIngress(
                        BucketMode.HASH_DYNAMIC));
        assertFalse(
                PaimonBucketWriterStrategyFactory.requiresOrderedSingleWriterIngress(
                        BucketMode.HASH_FIXED));
    }

    private static final class Fixture {
        private final PaimonBucketWriterRuntimeFactory runtime =
                mock(PaimonBucketWriterRuntimeFactory.class);
        private final BucketAssigner hashAssigner = mock(BucketAssigner.class);
        private final GlobalIndexAssigner globalAssigner = mock(GlobalIndexAssigner.class);
        private final RecordReader<InternalRow> reader = mock(RecordReader.class);

        private Fixture() throws Exception {
            when(runtime.createHashBucketAssigner(any(), any())).thenReturn(hashAssigner);
            when(runtime.createGlobalIndexAssigner(any())).thenReturn(globalAssigner);
            when(runtime.createIndexBootstrapReader(any())).thenReturn(reader);
            when(reader.readBatch()).thenReturn(null);
            doNothing().when(globalAssigner).open(any(Long.class), any(), any(Integer.class), any(Integer.class), any());
        }

        private void assertStrategy(
                BucketMode mode, Class<? extends PaimonBucketWriterStrategy> expectedType)
                throws Exception {
            try (PreparedStrategy created = create(mode)) {
                assertInstanceOf(expectedType, created.strategy);
            }
        }

        private PreparedStrategy create(BucketMode mode) throws Exception {
            FileStoreTable table = mock(FileStoreTable.class);
            TableWriteImpl writer = mock(TableWriteImpl.class);
            IOManager ioManager = mock(IOManager.class);
            when(table.bucketMode()).thenReturn(mode);
            if (mode == BucketMode.HASH_DYNAMIC || mode == BucketMode.KEY_DYNAMIC) {
                TableSchema schema = schema();
                when(table.primaryKeys()).thenReturn(Arrays.asList("id", "pt"));
                when(table.partitionKeys()).thenReturn(Collections.singletonList("pt"));
                when(table.schema()).thenReturn(schema);
                when(table.rowType()).thenReturn(schema.logicalRowType());
            } else {
                when(table.primaryKeys()).thenReturn(Collections.emptyList());
                when(table.partitionKeys()).thenReturn(Collections.emptyList());
            }
            SnapshotManager snapshotManager = mock(SnapshotManager.class);
            when(table.snapshotManager()).thenReturn(snapshotManager);
            when(snapshotManager.latestSnapshotIdFromFileSystem()).thenReturn(1L, 1L);
            when(writer.withIOManager(ioManager)).thenReturn(writer);
            when(writer.withCompactExecutor(any(ExecutorService.class))).thenReturn(writer);

            Object compactionRuntime = newCompactionRuntime(mode.name());
            PaimonPreparedWriterRuntime prepared =
                    bindPreparedWriter(table, writer, ioManager, compactionRuntime, mode.name());
            assertFalse(prepared.compactionSubmissionObserved());
            PaimonBucketWriterStrategy strategy =
                    PaimonBucketWriterStrategyFactory.create(
                            prepared,
                            "default.t",
                            "user",
                            PaimonWriteSemanticContractTestFactory.forMode(mode),
                            runtime);
            return new PreparedStrategy(strategy, compactionRuntime);
        }

        private static Object newCompactionRuntime(String generationName) throws Exception {
            // Keep this test in the bucket package so it can assert the dedicated concrete
            // strategy types. Reflection is limited to test fixture construction of the
            // package-confined writer/compaction binding; the production factory remains unable
            // to accept a raw writer or raw strategy context.
            Class<?> runtimeType =
                    Class.forName("io.tapdata.connector.paimon.write.PaimonCompactionRuntime");
            Constructor<?> constructor = runtimeType.getDeclaredConstructor(String.class);
            constructor.setAccessible(true);
            return constructor.newInstance("strategy-factory-" + generationName);
        }

        private static PaimonPreparedWriterRuntime bindPreparedWriter(
                FileStoreTable table,
                TableWriteImpl writer,
                IOManager ioManager,
                Object compactionRuntime,
                String generationName)
                throws Exception {
            Method bind =
                    PaimonPreparedWriterRuntime.class.getDeclaredMethod(
                            "bind",
                            String.class,
                            FileStoreTable.class,
                            TableWriteImpl.class,
                            IOManager.class,
                            compactionRuntime.getClass());
            bind.setAccessible(true);
            return (PaimonPreparedWriterRuntime)
                    bind.invoke(
                            null,
                            "default.t/" + generationName,
                            table,
                            writer,
                            ioManager,
                            compactionRuntime);
        }

        private static TableSchema schema() {
            java.util.List<DataField> fields =
                    Arrays.asList(
                            new DataField(0, "pt", new IntType()),
                            new DataField(1, "id", new IntType()));
            return new TableSchema(
                    0,
                    fields,
                    RowType.currentHighestFieldId(fields),
                    Collections.singletonList("pt"),
                    Arrays.asList("id", "pt"),
                    Collections.emptyMap(),
                    "");
        }
    }

    private static final class PreparedStrategy implements AutoCloseable {
        private final PaimonBucketWriterStrategy strategy;
        private final Object compactionRuntime;

        private PreparedStrategy(
                PaimonBucketWriterStrategy strategy, Object compactionRuntime) {
            this.strategy = strategy;
            this.compactionRuntime = compactionRuntime;
        }

        @Override
        public void close() throws Exception {
            Method beginShutdown = compactionRuntime.getClass().getDeclaredMethod("beginShutdown");
            beginShutdown.setAccessible(true);
            beginShutdown.invoke(compactionRuntime);
            Method awaitTermination =
                    compactionRuntime
                            .getClass()
                            .getDeclaredMethod("awaitTermination", long.class);
            awaitTermination.setAccessible(true);
            assertTrue((Boolean) awaitTermination.invoke(compactionRuntime, Long.MAX_VALUE));
            strategy.close();
        }
    }
}
