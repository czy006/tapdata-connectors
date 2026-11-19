package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.recovery.PreparedMessageStore;
import io.tapdata.connector.paimon.recovery.PublicationVerifier;
import io.tapdata.connector.paimon.schema.PaimonRecoveryAdmission;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractResolver;
import io.tapdata.connector.paimon.service.PaimonStartupRecoveryIntegrationTest;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyFactory;
import org.apache.commons.io.FileUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public final class DynamicRecoveryFixture implements AutoCloseable {
    enum Scenario { ORDERED, REVERSED_HASH_INDEX, EXPIRED_CROSS_PARTITION_INDEX }

    private static final Identifier ID = Identifier.create("demo", "events");
    private static final String USER = "recover-dynamic";
    private final Catalog catalog;
    private final Path root;
    final PaimonConfig config;

    private DynamicRecoveryFixture(Path root) throws Exception {
        this.root = root.toRealPath();
        this.config = new PaimonConfig().loadNodeConfig(Map.of("localRecoveryDir", this.root.resolve("state").toString()));
        catalog = CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(this.root.resolve("wh").toUri())));
    }

    DynamicRecoveryFixture(Path root, boolean partitioned, Map<String, String> options) throws Exception {
        this(root);
        catalog.createDatabase(ID.getDatabaseName(), false);
        Schema.Builder schema = Schema.newBuilder().column("k", DataTypes.INT()).column("p", DataTypes.INT()).column("v", DataTypes.INT())
                .primaryKey("k").option("bucket", "-1").option("dynamic-bucket.target-row-num", "2")
                .option("write-only", "true").option("write-buffer-size", partitioned ? "4 mb" : "96 kb")
                .option("page-size", "32 kb").option("write-buffer-spillable", "true");
        if (partitioned) {
            schema.partitionKeys("p");
        }
        options.forEach(schema::option);
        catalog.createTable(ID, schema.build(), false);
    }

    FileStoreTable table() throws Exception {
        return (FileStoreTable) catalog.getTable(ID);
    }

    public static void main(String[] args) throws Exception {
        try (DynamicRecoveryFixture fixture = new DynamicRecoveryFixture(Paths.get(args[0]))) {
            fixture.prepare(fixture.table(), true);
        }
    }

    private void prepare(FileStoreTable table, boolean crash) throws Exception {
        boolean verifySpill = crash && table.schema().partitionKeys().isEmpty();
        Path spill = Files.createDirectories(root.resolve("prepared-writer-spill"));
        try (PreparedMessageStore store = openState(); IOManager io = spy(new IOManagerImpl(spill.toString()));
                PaimonBucketWriterStrategy writer = writer(table, io)) {
            PreparedMessageStore.Table stored = store.register(ID.getDatabaseName(), ID.getObjectName(), table.uuid(),
                    USER, 1, PublicationVerifier.fingerprint(table));
            Map<Integer, Integer> updatesPerBucket = new HashMap<>();
            List<List<GenericRow>> batches = List.of(
                    List.of(row(1, 1, 10), row(2, 1, 20), row(3, 1, 30)),
                    List.of(row(4, 1, 40), row(1, 2, 11)),
                    List.of(delete(2, 1, 20), row(5, 2, 50)));
            if (verifySpill) {
                for (int i = 0; i < 8000; i++) {
                    writer.write(row(1, 1, 10));
                }
                verify(io, atLeastOnce()).createBufferFileWriter(any());
            }
            long id = 1;
            for (List<GenericRow> rows : batches) {
                for (GenericRow row : rows) {
                    writer.write(row);
                }
                List<CommitMessage> messages = writer.prepareCommit(id);
                for (CommitMessage m : messages) {
                    CommitMessageImpl nativeMessage = (CommitMessageImpl) m;
                    if (!nativeMessage.newFilesIncrement().newIndexFiles().isEmpty()) {
                        updatesPerBucket.merge(nativeMessage.bucket(), 1, Integer::sum);
                    }
                }
                stored.append(id++, false, Collections.singleton("source"), messages);
            }
            if (table.schema().partitionKeys().isEmpty()) {
                assertTrue(updatesPerBucket.values().stream().anyMatch(count -> count > 1),
                        "The input must publish successive cumulative indexes for the same bucket");
            }
            assertNull(table.snapshotManager().latestSnapshotId());
            if (crash) {
                Runtime.getRuntime().halt(42);
            }
        }
    }

    private void crashPreparedWriter() throws Exception {
        Path log = root.resolve("prepared-writer.log");
        String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/java.lang=ALL-UNNAMED", "-cp", cp,
                DynamicRecoveryFixture.class.getName(), root.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(child.waitFor(75, TimeUnit.SECONDS), "preparing JVM timed out");
            assertEquals(42, child.exitValue(), Files.readString(log));
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly().waitFor();
            }
        }
        Path spill = root.resolve("prepared-writer-spill");
        assertTrue(Files.isDirectory(spill));
        FileUtils.deleteDirectory(spill.toFile());
        assertFalse(Files.exists(spill));
    }

    List<String> run(Scenario scenario) throws Exception {
        FileStoreTable table = table();
        if (scenario != Scenario.EXPIRED_CROSS_PARTITION_INDEX) {
            PaimonRecoveryAdmission.validateTable(config, ID.getObjectName(), table);
        }
        if (scenario == Scenario.ORDERED) {
            crashPreparedWriter();
        } else {
            prepare(table, false);
        }
        table = table();
        try (PreparedMessageStore store = openState()) {
            PreparedMessageStore.Table stored = store.find(ID.getDatabaseName(), ID.getObjectName());
            stored.seal();
            PreparedMessageStore.Group group = stored.nextGroup();
            assertEquals(3, group.last());
            if (scenario == Scenario.REVERSED_HASH_INDEX) {
                // Negative control: Paimon's bucketed combiner keeps the last cumulative index.
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestFileHandler.java#L133
                List<CommitMessage> wrong = new ArrayList<>(group.messages());
                Collections.reverse(wrong);
                try (var commit = table.newStreamWriteBuilder().withCommitUser(USER).newCommit()) {
                    assertEquals(1, commit.filterAndCommit(Map.of(group.identifier(), wrong)));
                }
            } else {
                String head = PublicationVerifier.publish(table, USER, group.identifier(), group.messages(), stored.anchor());
                stored.confirm(group, head);
                stored.cleanup();
                assertNull(stored.nextGroup());
            }
        }
        assertEquals(List.of("1@2=11", "3@1=30", "4@1=40", "5@2=50"), read(table()));
        table = table();
        Set<Integer> previousBuckets = bucketsForKey(table, 4);
        assertEquals(1, previousBuckets.size());
        if (scenario == Scenario.EXPIRED_CROSS_PARTITION_INDEX) {
            // IndexBootstrap.filterSplit compares file.creationTimeEpochMillis(), not its mtime.
            Thread.sleep(table.coreOptions().crossPartitionUpsertIndexTtl().toMillis() + 2);
        }
        try (IOManager io = newIo(); PaimonBucketWriterStrategy writer = writer(table, io);
                var commit = table.newStreamWriteBuilder().withCommitUser(USER).newCommit()) {
            for (GenericRow row : List.of(row(6, 6, 60), row(7, 7, 70), row(4, 3, 44), row(5, 4, 55), delete(1, 2, 11))) {
                writer.write(row);
            }
            commit.commit(11, writer.prepareCommit(11));
        }
        table = table();
        if (table.schema().partitionKeys().isEmpty()) {
            Set<Integer> nextBuckets = bucketsForKey(table, 4);
            if (scenario == Scenario.REVERSED_HASH_INDEX) {
                assertTrue(nextBuckets.containsAll(previousBuckets));
                assertEquals(2, nextBuckets.size(), "an obsolete cumulative index routed the existing key to another bucket");
            } else {
                assertEquals(previousBuckets, nextBuckets);
            }
        }
        return read(table);
    }

    private PreparedMessageStore openState() throws Exception {
        return PreparedMessageStore.open(root.resolve("state"),
                PaimonStartupRecoveryIntegrationTest.taskState(root.resolve("task.properties")));
    }

    private IOManager newIo() throws Exception {
        return IOManager.create(Files.createTempDirectory(root, "spill-").toString());
    }

    private static PaimonBucketWriterStrategy writer(FileStoreTable table, IOManager io) throws Exception {
        var raw = (TableWriteImpl<?>) table.newStreamWriteBuilder().withCommitUser(USER).newWrite().withIOManager(io);
        return PaimonBucketWriterStrategyFactory.create(new PaimonBucketWriterStrategyContext(ID.getFullName(), table, raw,
                USER, io, PaimonWriteSemanticContractResolver.resolve(ID.getFullName(), table)));
    }

    private static GenericRow row(int k, int p, int v) {
        return GenericRow.of(k, p, v);
    }

    private static GenericRow delete(int k, int p, int v) {
        return GenericRow.ofKind(RowKind.DELETE, k, p, v);
    }

    private static List<String> read(FileStoreTable table) throws Exception {
        List<String> rows = new ArrayList<>();
        var builder = table.newReadBuilder();
        try (var reader = builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0) + "@" + row.getInt(1) + "=" + row.getInt(2)));
        }
        Collections.sort(rows);
        return rows;
    }

    private static Set<Integer> bucketsForKey(FileStoreTable table, int key) throws Exception {
        Set<Integer> buckets = new HashSet<>();
        var builder = table.newReadBuilder();
        for (var split : builder.newScan().plan().splits()) {
            int bucket = ((DataSplit) split).bucket();
            try (var reader = builder.newRead().createReader(split)) {
                reader.forEachRemaining(row -> {
                    if (row.getInt(0) == key) {
                        buckets.add(bucket);
                    }
                });
            }
        }
        return buckets;
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
