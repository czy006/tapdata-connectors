package io.tapdata.connector.paimon.recovery;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.DataFilePathFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PaimonStagingRecoveryIntegrationTest {
    private static final Identifier TABLE = Identifier.create("test", "pk");
    private static final String USER = "recovery-writer";
    private static final int COMMIT = 10;
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void restoresNativeMessagesInAnotherJvmAndVerifiesARepeatedRecovery(boolean writeOnly) throws Exception {
        runProcess("stage", writeOnly, 42);
        runProcess("recover", writeOnly, 0);
        runProcess("retry", writeOnly, 0);
    }

    private void runProcess(String phase, boolean writeOnly, int exitCode) throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Path log = directory.resolve(phase + ".log");
        Process process = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, Probe.class.getName(), directory.toString(), phase, Boolean.toString(writeOnly))
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "timed out in " + phase);
            assertEquals(exitCode, process.exitValue(), () -> readLog(log));
        } finally {
            process.destroyForcibly();
        }
    }

    private static String readLog(Path file) {
        try {
            return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return e.toString();
        }
    }

    /** The first JVM intentionally stops before native close. The parent runs two fresh recoveries. */
    public static final class Probe {
        public static void main(String[] args) throws Exception {
            Path directory = Paths.get(args[0]);
            String phase = args[1];
            boolean writeOnly = Boolean.parseBoolean(args[2]);
            Path checkpoint = directory.resolve("original-messages");
            CommitMessageSerializer serializer = new CommitMessageSerializer();
            try (Catalog catalog = open(directory)) {
                if (phase.equals("stage")) {
                    FileStoreTable table = create(catalog, writeOnly);
                    try (StreamTableWrite writer = writer(table);
                            DataOutputStream out = new DataOutputStream(Files.newOutputStream(checkpoint))) {
                        out.writeUTF(PublicationVerifier.fingerprint(table));
                        out.writeInt(serializer.getVersion());
                        for (int batch = 1; batch <= 3; batch++) {
                            if (batch == 1) {
                                writer.write(GenericRow.of(1, 10));
                                writer.write(GenericRow.of(2, 200));
                            } else if (batch == 2) {
                                writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 20));
                                writer.write(GenericRow.of(3, 300));
                            } else {
                                writer.write(GenericRow.ofKind(RowKind.DELETE, 2, 200));
                                writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 30));
                            }
                            List<CommitMessage> messages = writer.prepareCommit(true, batch);
                            out.writeInt(messages.size());
                            for (CommitMessage message : messages) {
                                byte[] encoded = serializer.serialize(message);
                                out.writeInt(encoded.length);
                                out.write(encoded);
                            }
                        }
                        out.flush();
                        try (FileChannel channel = FileChannel.open(checkpoint, StandardOpenOption.WRITE)) {
                            channel.force(true);
                        }
                        assertNull(table.snapshotManager().latestSnapshotIdFromFileSystem());
                        Runtime.getRuntime().halt(42);
                    }
                } else {
                    FileStoreTable table = (FileStoreTable) catalog.getTable(TABLE);
                    try (DataInputStream in = new DataInputStream(Files.newInputStream(checkpoint))) {
                        String baseline = in.readUTF();
                        int version = in.readInt();
                        List<CommitMessage> messages = new ArrayList<>();
                        for (int i = 0; i < 3; i++) {
                            int count = in.readInt();
                            for (int j = 0; j < count; j++) {
                                byte[] bytes = new byte[in.readInt()];
                                in.readFully(bytes);
                                messages.add(serializer.deserialize(version, bytes));
                            }
                        }
                        String proof = PublicationVerifier.publish(table, USER, COMMIT, messages, baseline);
                        assertEquals(proof, PublicationVerifier.fingerprint(table));
                        assertEquals(writeOnly ? 1L : 2L,
                                table.snapshotManager().latestSnapshotIdFromFileSystem().longValue());
                        assertEquals(expectedRows(1, 30, 3, 300), rows(table));
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void missingOrTruncatedPreparedOutputCannotPublishEvenOnFirstAttempt(boolean remove) throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, true);
            String baseline = PublicationVerifier.fingerprint(table);
            try (StreamTableWrite writer = writer(table)) {
                writer.write(GenericRow.of(1, 10));
                List<CommitMessage> messages = writer.prepareCommit(false, 1);
                CommitMessageImpl msg = (CommitMessageImpl) messages.get(0);
                DataFileMeta file = msg.newFilesIncrement().newFiles().get(0);
                Path path = Paths.get(new DataFilePathFactories(table.store().pathFactory())
                        .get(msg.partition(), msg.bucket()).toPath(file).toUri());
                assertTrue(Files.size(path) > 0);
                if (remove) {
                    Files.delete(path);
                } else {
                    Files.write(path, new byte[0]);
                }
                assertThrows(IOException.class, () -> PublicationVerifier.publish(table, USER, 1, messages, baseline));
                assertNull(table.snapshotManager().latestSnapshotIdFromFileSystem());
                try (StreamTableCommit raw = commit(table)) {
                    if (remove) {
                        raw.commit(1, messages);
                    } else {
                        assertEquals(1, raw.filterAndCommit(Collections.singletonMap(1L, messages)));
                    }
                }
                assertNotNull(table.snapshotManager().latestSnapshotIdFromFileSystem());
                assertThrows(Exception.class, () -> rows(table));
            }
        }
    }

    @Test
    void appendAloneDoesNotConfirmACommitThatAlsoPreparedCompaction() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, false);
            try (StreamTableWrite writer = writer(table); StreamTableCommit raw = commit(table)) {
                writer.write(GenericRow.of(1, 10));
                raw.commit(1, writer.prepareCommit(true, 1));
                String baseline = PublicationVerifier.fingerprint(table);
                writer.write(GenericRow.of(1, 20));
                List<CommitMessage> original = writer.prepareCommit(true, 2);
                assertEquals(1, original.size());
                CommitMessageImpl msg = (CommitMessageImpl) original.get(0);
                assertFalse(msg.compactIncrement().isEmpty());
                CommitMessage appendOnly = new CommitMessageImpl(msg.partition(), msg.bucket(), msg.totalBuckets(),
                        msg.newFilesIncrement(), CompactIncrement.emptyIncrement());
                raw.commit(2, Collections.singletonList(appendOnly));
                assertEquals(0, raw.filterAndCommit(Collections.singletonMap(2L, original)));
                assertThrows(IOException.class, () -> PublicationVerifier.publish(table, USER, 2, original, baseline));
                assertEquals(Snapshot.CommitKind.APPEND, table.snapshotManager().latestSnapshot().commitKind());
            }
        }
    }

    @Test
    void emptyIntervalHasOneIdentifiableSnapshotAcrossRetries() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, false);
            String baseline = PublicationVerifier.fingerprint(table);
            String confirmed = PublicationVerifier.publish(table, USER, COMMIT, Collections.emptyList(), baseline);
            assertEquals(confirmed, PublicationVerifier.publish(table, USER, COMMIT, Collections.emptyList(), baseline));
            Snapshot head = table.snapshotManager().latestSnapshot();
            assertEquals(1, head.id());
            assertEquals(COMMIT, head.commitIdentifier());
            assertEquals(Snapshot.CommitKind.APPEND, head.commitKind());
            assertTrue(rows(table).isEmpty());
        }
    }

    @Test
    void sameUserIdentifierAndKindCannotConfirmAnIncompleteAppend() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, true);
            String baseline = PublicationVerifier.fingerprint(table);
            try (StreamTableWrite writer = writer(table); StreamTableCommit raw = commit(table)) {
                writer.write(GenericRow.of(1, 10));
                List<CommitMessage> first = writer.prepareCommit(false, 1);
                writer.write(GenericRow.of(2, 20));
                List<CommitMessage> sealed = new ArrayList<>(first);
                sealed.addAll(writer.prepareCommit(false, 2));
                raw.commit(COMMIT, first);
                assertEquals(0, raw.filterAndCommit(Collections.singletonMap((long) COMMIT, sealed)));
                assertThrows(IOException.class,
                        () -> PublicationVerifier.publish(table, USER, COMMIT, sealed, baseline));
                assertEquals(expectedRows(1, 10), rows(table));
            }
        }
    }

    @Test
    void missingCompactInputIsDetectedBeforeBusinessAppend() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, false);
            try (StreamTableWrite writer = writer(table); StreamTableCommit raw = commit(table)) {
                writer.write(GenericRow.of(1, 10));
                raw.commit(1, writer.prepareCommit(true, 1));
                String baseline = PublicationVerifier.fingerprint(table);
                writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 20));
                List<CommitMessage> staged = writer.prepareCommit(true, 2);
                CommitMessageImpl message = (CommitMessageImpl) staged.get(0);
                DataFileMeta old = message.compactIncrement().compactBefore().stream()
                        .filter(f -> message.newFilesIncrement().newFiles().stream()
                                .noneMatch(added -> added.fileName().equals(f.fileName())))
                        .findFirst().orElseThrow(AssertionError::new);
                Path path = Paths.get(new DataFilePathFactories(table.store().pathFactory())
                        .get(message.partition(), message.bucket()).toPath(old).toUri());
                Files.delete(path);
                assertThrows(IOException.class, () -> PublicationVerifier.publish(table, USER, 2, staged, baseline));
                assertEquals(baseline, PublicationVerifier.fingerprint(table));
            }
        }
    }

    @Test
    void compactionCanRemoveAnActiveFileFromThePreviousConfirmedInterval() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, false);
            String initial = PublicationVerifier.fingerprint(table);
            try (StreamTableWrite writer = writer(table)) {
                writer.write(GenericRow.of(1, 10));
                String baseline = PublicationVerifier.publish(table, USER, 1, writer.prepareCommit(true, 1), initial);
                writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 20));
                List<CommitMessage> second = writer.prepareCommit(true, 2);
                assertFalse(((CommitMessageImpl) second.get(0)).compactIncrement().isEmpty());
                PublicationVerifier.publish(table, USER, 2, second, baseline);
                assertEquals(expectedRows(1, 20), rows(table));
                assertEquals(3, table.snapshotManager().latestSnapshot().id());
            }
        }
    }

    @Test
    void uncommittedCompactInputCannotBeRemovedFromAnUnrelatedBaseline() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, false);
            String baseline = PublicationVerifier.fingerprint(table);
            try (StreamTableWrite writer = writer(table)) {
                writer.write(GenericRow.of(1, 10));
                writer.prepareCommit(true, 1);
                writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 20));
                List<CommitMessage> suffixOnly = writer.prepareCommit(true, 2);
                assertFalse(((CommitMessageImpl) suffixOnly.get(0)).compactIncrement().isEmpty());
                assertThrows(IOException.class,
                        () -> PublicationVerifier.publish(table, USER, 2, suffixOnly, baseline));
                assertNull(table.snapshotManager().latestSnapshotIdFromFileSystem());
            }
        }
    }

    @Test
    void recoveryFailsClosedWhenItsPredecessorSnapshotHasExpired() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, true);
            try (StreamTableWrite writer = writer(table); StreamTableCommit raw = commit(table)) {
                writer.write(GenericRow.of(1, 10));
                raw.commit(1, writer.prepareCommit(false, 1));
                String baseline = PublicationVerifier.fingerprint(table);
                writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 20));
                List<CommitMessage> staged = writer.prepareCommit(false, 2);
                String confirmed = PublicationVerifier.publish(table, USER, 2, staged, baseline);
                Files.delete(Paths.get(table.snapshotManager().snapshotPath(1).toUri()));
                assertThrows(IOException.class, () -> PublicationVerifier.publish(table, USER, 2, staged, baseline));
                assertEquals(confirmed, PublicationVerifier.fingerprint(table));
            }
        }
    }

    @Test
    void unchangedHeadJsonDoesNotHideMissingSnapshotMetadata() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, true);
            try (StreamTableWrite writer = writer(table); StreamTableCommit raw = commit(table)) {
                writer.write(GenericRow.of(1, 10));
                raw.commit(1, writer.prepareCommit(false, 1));
                String baseline = PublicationVerifier.fingerprint(table);
                Snapshot head = table.snapshotManager().latestSnapshot();
                Files.delete(Paths.get(table.store().pathFactory().toManifestListPath(head.baseManifestList()).toUri()));
                assertEquals(head.id(), table.snapshotManager().latestSnapshotIdFromFileSystem().longValue());
                assertThrows(IOException.class, () -> PublicationVerifier.fingerprint(table));
                assertThrows(IOException.class,
                        () -> PublicationVerifier.publish(table, USER, 2, Collections.emptyList(), baseline));
            }
        }
    }

    @Test
    void foreignTruncateCannotRepublishOldRows() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, true);
            try (StreamTableWrite writer = writer(table); StreamTableCommit raw = commit(table)) {
                writer.write(GenericRow.of(1, 10));
                raw.commit(1, writer.prepareCommit(false, 1));
                String baseline = PublicationVerifier.fingerprint(table);
                writer.write(GenericRow.of(2, 20));
                List<CommitMessage> original = writer.prepareCommit(false, 2);
                try (BatchTableCommit ddl = table.newBatchWriteBuilder().newCommit()) {
                    ddl.truncateTable();
                }
                String truncated = PublicationVerifier.fingerprint(table);
                assertThrows(IOException.class, () -> PublicationVerifier.publish(table, USER, 2, original, baseline));
                assertEquals(truncated, PublicationVerifier.fingerprint(table));
                assertTrue(rows(table).isEmpty());
                raw.filterAndCommit(Collections.singletonMap(2L, original));
                assertEquals(expectedRows(2, 20), rows(table));
            }
        }
    }

    @Test
    void snapshotNumberReusedAfterRollbackHasADifferentFingerprint() throws Exception {
        try (Catalog catalog = open(directory)) {
            FileStoreTable table = create(catalog, true);
            try (StreamTableWrite writer = writer(table); StreamTableCommit raw = commit(table)) {
                writer.write(GenericRow.of(1, 10));
                raw.commit(1, writer.prepareCommit(false, 1));
                writer.write(GenericRow.of(2, 20));
                raw.commit(2, writer.prepareCommit(false, 2));
            }
            String baseline = PublicationVerifier.fingerprint(table);
            table.rollbackTo(1);
            try (StreamTableWrite writer = writer(table); StreamTableCommit raw = commit(table)) {
                writer.write(GenericRow.of(3, 30));
                raw.commit(3, writer.prepareCommit(false, 3));
            }
            assertEquals(2L, table.snapshotManager().latestSnapshotIdFromFileSystem().longValue());
            assertNotEquals(baseline, PublicationVerifier.fingerprint(table));
            assertThrows(IOException.class,
                    () -> PublicationVerifier.publish(table, USER, 4, Collections.emptyList(), baseline));
            assertEquals(expectedRows(1, 10, 3, 30), rows(table));
        }
    }

    private static StreamTableWrite writer(FileStoreTable table) {
        return table.newStreamWriteBuilder().withCommitUser(USER).newWrite();
    }

    private static StreamTableCommit commit(FileStoreTable table) {
        return table.newStreamWriteBuilder().withCommitUser(USER).newCommit();
    }

    private static Catalog open(Path directory) {
        return CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(
                Collections.singletonMap("warehouse", directory.resolve("warehouse").toUri().toString()))));
    }

    private static FileStoreTable create(Catalog catalog, boolean writeOnly) throws Exception {
        catalog.createDatabase("test", false);
        catalog.createTable(TABLE, Schema.newBuilder().column("k", DataTypes.INT()).column("v", DataTypes.INT())
                .primaryKey("k").option("bucket", "1").option("merge-engine", "deduplicate")
                .option("write-only", Boolean.toString(writeOnly)).option("num-sorted-run.compaction-trigger", "2")
                .build(), false);
        return (FileStoreTable) catalog.getTable(TABLE);
    }

    private static Map<Integer, Integer> expectedRows(int... keyValues) {
        Map<Integer, Integer> rows = new TreeMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            rows.put(keyValues[i], keyValues[i + 1]);
        }
        return rows;
    }

    private static Map<Integer, Integer> rows(FileStoreTable table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        Map<Integer, Integer> result = new TreeMap<>();
        try (RecordReader<InternalRow> reader = builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> assertNull(result.put(row.getInt(0), row.getInt(1))));
        }
        return result;
    }
}
