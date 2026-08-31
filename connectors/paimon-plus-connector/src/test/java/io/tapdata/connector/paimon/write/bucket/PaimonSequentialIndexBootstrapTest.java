package io.tapdata.connector.paimon.write.bucket;

import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonSequentialIndexBootstrapTest {

    @Test
    void outputMultisetMustMatchPaimon132AcrossLatestTrimmedBucketPartitionAndTtl()
            throws Exception {
        long now = System.currentTimeMillis();
        PaimonSequentialIndexBootstrapFixture fixture =
                new PaimonSequentialIndexBootstrapFixture(Duration.ofDays(1L), now);
        fixture.addSplit("even-a", 10, 0, now + TimeUnit.MINUTES.toMillis(1L), 100, 101);
        fixture.addSplit("odd", 20, 1, now + TimeUnit.MINUTES.toMillis(1L), 200);
        fixture.addSplit("expired", 30, 2, 0L, 300);
        fixture.addSplit(
                "even-b",
                40,
                4,
                new long[] {0L, now + TimeUnit.MINUTES.toMillis(1L)},
                Arrays.asList(
                        Collections.singletonList(GenericRow.of(400)),
                        Collections.singletonList(GenericRow.of(401))));

        List<String> paimon132 =
                PaimonSequentialIndexBootstrapFixture.drain(
                        new IndexBootstrap(fixture.table).bootstrap(2, 0));
        List<String> sequential =
                PaimonSequentialIndexBootstrapFixture.drain(
                        new PaimonSequentialIndexBootstrap(
                                        fixture.table, fixture.currentTimeSupplier())
                                .bootstrap(2, 0));

        assertEquals(multiset(paimon132), multiset(sequential));
        assertEquals(
                multiset(Arrays.asList("100|10|0", "101|10|0", "400|40|4", "401|40|4")),
                multiset(sequential));
        fixture.assertLatestTrimmedProjection();
        assertEquals(1, fixture.currentTimeSamples.get());
    }

    @Test
    void ttlBoundaryMustBeInclusiveAndSampleCurrentTimeExactlyOnce() throws Exception {
        PaimonSequentialIndexBootstrapFixture fixture =
                new PaimonSequentialIndexBootstrapFixture(Duration.ofMillis(100L), 1_000L);
        fixture.addSplit("boundary", 10, 0, 900L, 1);
        fixture.addSplit("expired", 20, 2, 899L, 2);
        fixture.addSplit(
                "any-file-boundary",
                30,
                4,
                new long[] {0L, 900L},
                Collections.singletonList(Collections.singletonList(GenericRow.of(3))));

        List<String> rows =
                PaimonSequentialIndexBootstrapFixture.drain(
                        new PaimonSequentialIndexBootstrap(
                                        fixture.table, fixture.currentTimeSupplier())
                                .bootstrap(2, 0));

        assertEquals(multiset(Arrays.asList("1|10|0", "3|30|4")), multiset(rows));
        assertEquals(1, fixture.currentTimeSamples.get());
    }

    @Test
    void zeroSplitMustReturnEmptyReaderWithoutConstructingSplitReader() throws Exception {
        PaimonSequentialIndexBootstrapFixture fixture =
                new PaimonSequentialIndexBootstrapFixture(null, 0L);

        RecordReader<InternalRow> reader =
                new PaimonSequentialIndexBootstrap(fixture.table, fixture.currentTimeSupplier())
                        .bootstrap(1, 0);

        assertNull(reader.readBatch());
        reader.close();
        reader.close();
        assertFalse(fixture.currentTimeSamples.get() > 0);
        assertTrue(fixture.events.isEmpty());
        fixture.assertLatestTrimmedProjection();
    }

    @Test
    void explicitCloseBeforeFirstReadMustNotConstructAnySplitReader() throws Exception {
        PaimonSequentialIndexBootstrapFixture fixture =
                new PaimonSequentialIndexBootstrapFixture(null, 0L);
        PaimonSequentialIndexBootstrapFixture.ReaderPlan unopened =
                fixture.addSplit("unopened", 10, 0, 0L, 1);
        RecordReader<InternalRow> reader =
                new PaimonSequentialIndexBootstrap(fixture.table, fixture.currentTimeSupplier())
                        .bootstrap(1, 0);

        reader.close();
        reader.close();

        assertEquals(0, unopened.createCount.get());
        assertTrue(fixture.events.isEmpty());
    }

    @Test
    void multiSplitMustReleaseEachBatchAndCloseEachReaderOnCallerThreadInOrder()
            throws Exception {
        PaimonSequentialIndexBootstrapFixture fixture =
                new PaimonSequentialIndexBootstrapFixture(null, 0L);
        PaimonSequentialIndexBootstrapFixture.ReaderPlan first =
                fixture.addSplit(
                        "first",
                        10,
                        0,
                        new long[] {0L},
                        Arrays.asList(
                                Collections.singletonList(GenericRow.of(1)),
                                Collections.singletonList(GenericRow.of(2))));
        PaimonSequentialIndexBootstrapFixture.ReaderPlan second =
                fixture.addSplit("second", 20, 2, 0L, 3);
        long callerThread = Thread.currentThread().getId();

        List<String> rows =
                PaimonSequentialIndexBootstrapFixture.drain(
                        new PaimonSequentialIndexBootstrap(
                                        fixture.table, fixture.currentTimeSupplier())
                                .bootstrap(1, 0));

        assertEquals(multiset(Arrays.asList("1|10|0", "2|10|0", "3|20|2")), multiset(rows));
        assertEquals(2, first.releaseCount.get());
        assertEquals(1, first.closeCount.get());
        assertEquals(1, second.releaseCount.get());
        assertEquals(1, second.closeCount.get());
        fixture.assertAllReaderEventsOn(callerThread);
        assertEquals(
                Arrays.asList(
                        "create:first",
                        "read:first",
                        "release:first:0",
                        "read:first",
                        "release:first:1",
                        "read:first",
                        "close:first",
                        "create:second",
                        "read:second",
                        "release:second:0",
                        "read:second",
                        "close:second"),
                fixture.eventActions());
    }

    @Test
    void readBatchFailureMustReleaseActiveBatchThenCloseExactReaderAndSuppressCloseFailure()
            throws Exception {
        PaimonSequentialIndexBootstrapFixture fixture =
                new PaimonSequentialIndexBootstrapFixture(null, 0L);
        IOException readFailure = new IOException("read failure");
        IOException closeFailure = new IOException("close failure");
        PaimonSequentialIndexBootstrapFixture.ReaderPlan failed =
                fixture.addSplit("failed", 10, 0, 0L, 1)
                        .failReadBatchOnCall(2, readFailure)
                        .failClose(closeFailure);
        fixture.addSplit("must-not-open", 20, 2, 0L, 2);
        RecordReader<InternalRow> reader =
                new PaimonSequentialIndexBootstrap(fixture.table, fixture.currentTimeSupplier())
                        .bootstrap(1, 0);
        RecordReader.RecordIterator<InternalRow> activeBatch = reader.readBatch();
        assertEquals(1, activeBatch.next().getInt(0));

        IOException thrown = assertThrows(IOException.class, reader::readBatch);

        assertSame(readFailure, thrown);
        assertEquals(Collections.singletonList(closeFailure), Arrays.asList(thrown.getSuppressed()));
        assertEquals(1, failed.releaseCount.get());
        assertEquals(1, failed.closeCount.get());
        assertEquals(
                Arrays.asList(
                        "create:failed",
                        "read:failed",
                        "release:failed:0",
                        "read:failed",
                        "close:failed"),
                fixture.eventActions());
        IOException replay = assertThrows(IOException.class, reader::close);
        assertSame(closeFailure, replay.getCause());
    }

    @Test
    void batchReleaseFailureMustBePrimaryAndSuppressReaderCloseFailure() throws Exception {
        PaimonSequentialIndexBootstrapFixture fixture =
                new PaimonSequentialIndexBootstrapFixture(null, 0L);
        IllegalStateException releaseFailure = new IllegalStateException("release failure");
        IOException closeFailure = new IOException("close failure");
        PaimonSequentialIndexBootstrapFixture.ReaderPlan failed =
                fixture.addSplit("failed", 10, 0, 0L, 1)
                        .failRelease(releaseFailure)
                        .failClose(closeFailure);
        RecordReader<InternalRow> reader =
                new PaimonSequentialIndexBootstrap(fixture.table, fixture.currentTimeSupplier())
                        .bootstrap(1, 0);
        RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
        assertEquals(1, batch.next().getInt(0));

        IllegalStateException thrown = assertThrows(IllegalStateException.class, batch::next);

        assertSame(releaseFailure, thrown);
        assertEquals(Collections.singletonList(closeFailure), Arrays.asList(thrown.getSuppressed()));
        assertEquals(1, failed.releaseCount.get());
        assertEquals(1, failed.closeCount.get());
        IOException replay = assertThrows(IOException.class, reader::close);
        assertSame(releaseFailure, replay.getCause());
        assertEquals(1, failed.releaseCount.get());
        assertEquals(1, failed.closeCount.get());
    }

    @Test
    void readerCloseFailureAtSplitEndMustBeStickyAndNeverRetried() throws Exception {
        PaimonSequentialIndexBootstrapFixture fixture =
                new PaimonSequentialIndexBootstrapFixture(null, 0L);
        IOException closeFailure = new IOException("close failure");
        PaimonSequentialIndexBootstrapFixture.ReaderPlan failed =
                fixture.addSplit("failed", 10, 0, 0L, 1).failClose(closeFailure);
        RecordReader<InternalRow> reader =
                new PaimonSequentialIndexBootstrap(fixture.table, fixture.currentTimeSupplier())
                        .bootstrap(1, 0);
        RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
        assertEquals(1, batch.next().getInt(0));
        assertNull(batch.next());
        batch.releaseBatch();

        IOException thrown = assertThrows(IOException.class, reader::readBatch);
        IOException replay = assertThrows(IOException.class, reader::close);

        assertSame(closeFailure, thrown);
        assertSame(closeFailure, replay.getCause());
        assertEquals(1, failed.releaseCount.get());
        assertEquals(1, failed.closeCount.get());
    }

    @Test
    void readerCreationFailureMustReturnSynchronouslyAndNotOpenLaterSplit() throws Exception {
        PaimonSequentialIndexBootstrapFixture fixture =
                new PaimonSequentialIndexBootstrapFixture(null, 0L);
        IOException createFailure = new IOException("create failure");
        PaimonSequentialIndexBootstrapFixture.ReaderPlan failed =
                fixture.addSplit("failed", 10, 0, 0L).failCreate(createFailure);
        PaimonSequentialIndexBootstrapFixture.ReaderPlan later =
                fixture.addSplit("later", 20, 2, 0L, 2);
        RecordReader<InternalRow> reader =
                new PaimonSequentialIndexBootstrap(fixture.table, fixture.currentTimeSupplier())
                        .bootstrap(1, 0);

        IOException thrown = assertThrows(IOException.class, reader::readBatch);

        assertSame(createFailure, thrown);
        assertEquals(1, failed.createCount.get());
        assertEquals(0, later.createCount.get());
        reader.close();
    }

    @Test
    void productionBytecodeMustNotReferencePaimonParallelBootstrapImplementations()
            throws Exception {
        assertClassOmits(
                PaimonSequentialIndexBootstrap.class,
                "org/apache/paimon/crosspartition/IndexBootstrap",
                "org/apache/paimon/io/SplitsParallelReadUtil",
                "org/apache/paimon/utils/ParallelExecution");
        assertClassOmits(
                Class.forName(
                        PaimonSequentialIndexBootstrap.class.getName()
                                + "$SequentialCompositeReader"),
                "org/apache/paimon/crosspartition/IndexBootstrap",
                "org/apache/paimon/io/SplitsParallelReadUtil",
                "org/apache/paimon/utils/ParallelExecution");
    }

    private static List<String> multiset(List<String> values) {
        java.util.ArrayList<String> sorted = new java.util.ArrayList<>(values);
        Collections.sort(sorted);
        return sorted;
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
        String constantPool = new String(bytecode, java.nio.charset.StandardCharsets.ISO_8859_1);
        for (String forbiddenName : forbiddenNames) {
            assertFalse(
                    constantPool.contains(forbiddenName),
                    () -> type.getName() + " references forbidden " + forbiddenName);
        }
    }
}
