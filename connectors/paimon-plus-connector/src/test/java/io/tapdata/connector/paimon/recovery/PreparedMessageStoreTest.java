package io.tapdata.connector.paimon.recovery;

import io.tapdata.entity.utils.cache.KVMap;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class PreparedMessageStoreTest {
    @TempDir Path temp;
    private final NodeMap node = new NodeMap();
    private Path root() { return temp.resolve("recovery"); }

    private PreparedMessageStore open() throws IOException {
        return PreparedMessageStore.open(root(), node);
    }

    private static PreparedMessageStore.Table table(PreparedMessageStore store) throws IOException {
        return store.register("db", "orders", "physical/main/schema0", "writer", 11, "empty");
    }

    private static List<CommitMessage> buckets(int... buckets) {
        return Arrays.stream(buckets).mapToObj(bucket -> (CommitMessage) new CommitMessageImpl(
                BinaryRow.EMPTY_ROW, bucket, 8, DataIncrement.emptyIncrement(),
                CompactIncrement.emptyIncrement())).collect(Collectors.toList());
    }

    @Test
    void anyPersistedBindingPreventsFallingBackToUnprotectedWrites() throws Exception {
        assertFalse(PreparedMessageStore.hasBinding(node));
        try (PreparedMessageStore store = open()) {
            assertTrue(PreparedMessageStore.hasBinding(node));
        }
        node.value = "malformed";
        assertTrue(PreparedMessageStore.hasBinding(node));
    }

    @Test
    void recoversExactOrderedBytesAfterUnclosedJvmAndDoesNotPublishTwice() throws Exception {
        Path kvToken = temp.resolve("node-token");
        try (PreparedMessageStore initial = open()) {
            assertTrue(initial.tables().isEmpty());
        }
        Files.writeString(kvToken, (String) node.value);
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                CrashWriter.class.getName(), root().toString(), kvToken.toString())
                .redirectErrorStream(true).redirectOutput(temp.resolve("crash.log").toFile()).start();
        try {
            assertTrue(child.waitFor(45, TimeUnit.SECONDS), "child JVM timed out");
            assertEquals(42, child.exitValue(), () -> childOutput(temp));
        } finally {
            child.destroyForcibly();
        }
        try (PreparedMessageStore store = open()) {
            PreparedMessageStore.Table t = store.find("db", "orders");
            assertNotNull(t);
            assertEquals(2L, t.prepared());
            assertEquals(0L, t.published());
            assertNull(t.nextGroup());
            t.seal();
            PreparedMessageStore.Group g = t.nextGroup();
            assertEquals(1, g.first());
            assertEquals(2, g.last());
            assertEquals(12, g.identifier());
            assertEquals(buckets(3, 1, 4), g.messages());
            assertThrows(IOException.class, () -> t.append(13, true, Collections.emptySet(), buckets(0)));
            t.confirm(g, "confirmed-head");
            assertThrows(IOException.class, () -> t.confirm(g, "stale"));
            // Simulate crash after confirmation and before KV reconciliation / local GC.
        }
        // The published prefix is self-contained. A corrupt old message awaiting GC cannot
        // force a caller to publish again or prevent cleanup after KV reconciliation.
        Files.write(tableDir().resolve("prepare-1"), new byte[] {1});
        try (PreparedMessageStore store = open()) {
            PreparedMessageStore.Table t = store.find("db", "orders");
            assertNull(t.nextGroup());
            assertEquals("confirmed-head", t.anchor());
            assertEquals(13L, t.confirmedNextIdentifier());
            assertTrue(Files.exists(tableDir().resolve("prepare-1")));
            t.cleanup();
            assertFalse(Files.exists(tableDir().resolve("prepare-1")));
            assertEquals(3L, t.append(13, true, Collections.singleton("s"), buckets(5)));
            t.seal();
            assertEquals(Collections.singletonList(buckets(5).get(0)), t.nextGroup().messages());
        }
    }

    public static class CrashWriter {
        public static void main(String[] args) throws Exception {
            NodeMap kv = new NodeMap();
            kv.value = Files.readString(Path.of(args[1]));
            if (args.length == 3 && "contender".equals(args[2])) {
                try (PreparedMessageStore unexpected = PreparedMessageStore.open(Path.of(args[0]), kv)) {
                    throw new AssertionError("Second JVM acquired live recovery directory");
                } catch (IOException locked) {
                    if (!locked.getMessage().contains("already locked")) throw locked;
                    return;
                }
            }
            PreparedMessageStore store = PreparedMessageStore.open(Path.of(args[0]), kv);
            PreparedMessageStore.Table t = table(store);
            t.append(11, false, Collections.emptySet(), buckets(3, 1));
            t.append(12, true, Collections.singleton("lane"), buckets(4));
            Runtime.getRuntime().halt(42);
        }
    }

    @Test
    void liveOwnerInAnotherJvmPreventsBinding() throws Exception {
        try (PreparedMessageStore store = open()) {
            Path kvToken = temp.resolve("node-token");
            Files.writeString(kvToken, (String) node.value);
            Process contender = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    CrashWriter.class.getName(), root().toString(), kvToken.toString(), "contender")
                    .redirectErrorStream(true).redirectOutput(temp.resolve("crash.log").toFile()).start();
            try {
                assertTrue(contender.waitFor(45, TimeUnit.SECONDS));
                assertEquals(0, contender.exitValue(), () -> childOutput(temp));
            } finally {
                contender.destroyForcibly();
            }
            assertTrue(store.tables().isEmpty());
        }
    }

    @Test
    void completesFailedInitialReadyWriteWithoutChangingBinding() throws Exception {
        node.failReady = true;
        assertThrows(IllegalStateException.class, this::open);
        String pending = (String) node.value;
        assertTrue(pending.startsWith("pending/"));
        try (PreparedMessageStore retried = open()) {
            assertThrows(IllegalStateException.class, retried::requireFreshSourceSession);
            assertEquals(pending.replace("pending/", "ready/"), node.value);
            assertTrue(retried.tables().isEmpty());
        }
    }

    @Test
    void cannotAdoptClonedDirectoryOrLoseRegisteredTable() throws Exception {
        try (PreparedMessageStore owner = open()) {
            table(owner);
            assertThrows(IOException.class, this::open);
            assertThrows(IOException.class, () -> PreparedMessageStore.open(temp.resolve("copy"), node));
            assertThrows(IOException.class, () -> owner.register("db", "orders", "changed", "writer", 11, "empty"));
        }
        Path state = tableDir().resolve("state");
        Files.delete(state);
        assertThrows(IOException.class, this::open);
    }

    @Test
    void missingRootRegistryNeverLooksLikeAnEmptyTask() throws Exception {
        try (PreparedMessageStore store = open()) {
            table(store);
        }
        Files.delete(root().resolve("tables"));
        assertThrows(IOException.class, this::open);
    }

    @Test
    void validButObsoleteEmptyInventoryCannotHideADurableTable() throws Exception {
        byte[] initialInventory;
        try (PreparedMessageStore store = open()) {
            initialInventory = Files.readAllBytes(root().resolve("tables"));
            table(store).append(11, false, Collections.emptySet(), buckets(0));
        }
        Files.write(root().resolve("tables"), initialInventory);
        assertThrows(IOException.class, this::open);
        assertTrue(Files.exists(tableDir().resolve("prepare-1")));
    }

    @Test
    void stalePendingBindingCannotRecreateMarkerForPopulatedDirectory() throws Exception {
        try (PreparedMessageStore store = open()) {
            table(store).append(11, false, Collections.emptySet(), buckets(0));
        }
        node.value = ((String) node.value).replace("ready/", "pending/");
        Files.delete(root().resolve("binding"));
        assertThrows(IOException.class, this::open);
        assertFalse(Files.exists(root().resolve("binding")));
    }

    @Test
    void rowlessCompletionIsOneRecognizableGroupAndOversizedPreparePoisonsWriter() throws Exception {
        try (PreparedMessageStore store = open()) {
            PreparedMessageStore.Table t = table(store);
            t.append(11, false, Collections.emptySet(), Collections.emptyList());
            t.seal();
            PreparedMessageStore.Group empty = t.nextGroup();
            assertTrue(empty.messages().isEmpty());
            assertEquals(11, empty.identifier());
            t.confirm(empty, "after-empty");
            t.cleanup();
            List<CommitMessage> invalid = Collections.nCopies(PreparedBatch.MAX_MESSAGES + 1, null);
            assertThrows(IOException.class, () -> t.append(12, false, Collections.emptySet(), invalid));
            assertThrows(IOException.class, () -> t.append(12, false, Collections.emptySet(), buckets(1)));
        }
    }

    @Test
    void missingNodeBindingCannotReclaimLocalData() throws Exception {
        try (PreparedMessageStore store = open()) {
            table(store);
        }
        node.clear();
        assertThrows(IOException.class, this::open);
    }

    @Test
    void swappedRecordsAndChangedBytesFailBeforeOriginalGroupCanBeReleased() throws Exception {
        try (PreparedMessageStore store = open()) {
            PreparedMessageStore.Table t = table(store);
            t.append(11, false, Collections.emptySet(), buckets(7));
            t.append(12, false, Collections.emptySet(), buckets(6));
            t.seal();
            Path first = tableDir().resolve("prepare-1");
            Path second = tableDir().resolve("prepare-2");
            byte[] preserved = Files.readAllBytes(first);
            Files.write(first, Files.readAllBytes(second));
            assertThrows(IOException.class, t::nextGroup);
            preserved[preserved.length - 1] ^= 1;
            Files.write(first, preserved);
            assertThrows(IOException.class, t::nextGroup);
            assertEquals(0L, t.published());
        }
    }

    @ParameterizedTest
    @EnumSource(LocalRecoveryFiles.Phase.class)
    void failureAtAnySyncBoundaryDoesNotAcknowledgeOrReuseAmbiguousTail(LocalRecoveryFiles.Phase phase)
            throws Exception {
        AtomicBoolean inject = new AtomicBoolean();
        try (PreparedMessageStore store = PreparedMessageStore.open(root(), node, (current, file) -> {
            if (inject.get() && current == phase && "state".equals(file.getFileName().toString())) {
                throw new IOException("injected " + phase);
            }
        })) {
            PreparedMessageStore.Table t = table(store);
            inject.set(true);
            assertThrows(IOException.class, () -> t.append(11, false, Collections.emptySet(), buckets(7)));
            assertThrows(IOException.class, () -> t.append(11, false, Collections.emptySet(), buckets(8)));
            assertEquals(0L, t.published());
        }
        try (PreparedMessageStore reopened = open()) {
            PreparedMessageStore.Table t = reopened.find("db", "orders");
            assertEquals(0, t.published());
            if (phase == LocalRecoveryFiles.Phase.DIRECTORY_FORCE) {
                // Atomic rename succeeded. The retained complete but unacknowledged batch may be replayed.
                assertEquals(1, t.prepared());
                t.seal();
                assertEquals(buckets(7), t.nextGroup().messages());
            } else {
                assertEquals(0, t.prepared());
                t.append(11, false, Collections.emptySet(), buckets(8));
                t.seal();
                assertEquals(buckets(8), t.nextGroup().messages());
            }
        }
    }

    @Test
    void boundedGroupingReusesExactCutoffAndKAcrossPartialRecovery() throws Exception {
        List<CommitMessage> many = Collections.nCopies(PreparedBatch.MAX_MESSAGES, buckets(0).get(0));
        try (PreparedMessageStore store = open()) {
            PreparedMessageStore.Table t = table(store);
            t.append(11, false, Collections.emptySet(), many);
            t.append(12, false, Collections.emptySet(), many);
            t.append(13, false, Collections.emptySet(), buckets(1));
            t.seal();
            Path finalInput = tableDir().resolve("prepare-3");
            byte[] bytes = Files.readAllBytes(finalInput);
            Files.write(finalInput, new byte[0]);
            assertThrows(IOException.class, t::nextGroup, "never release group 1 with a corrupt later group");
            Files.write(finalInput, bytes);
            PreparedMessageStore.Group first = t.nextGroup();
            assertEquals(12L, first.identifier());
            assertEquals(8192, first.messages().size());
            t.confirm(first, "head12");
            t.cleanup();
        }
        try (PreparedMessageStore store = open()) {
            PreparedMessageStore.Table t = store.find("db", "orders");
            assertTrue(t.sealed());
            assertEquals("head12", t.anchor());
            PreparedMessageStore.Group last = t.nextGroup();
            assertEquals(13L, last.identifier());
            assertEquals(3L, last.first());
            assertEquals(buckets(1), last.messages());
        }
    }

    @Test
    void closedHandleCannotWriteAfterNewOwnerReopens() throws Exception {
        PreparedMessageStore.Table stale;
        try (PreparedMessageStore store = open()) {
            stale = table(store);
        }
        try (PreparedMessageStore store = open()) {
            assertThrows(IOException.class, () -> stale.append(11, false, Collections.emptySet(), buckets(4)));
            assertEquals(0L, store.find("db", "orders").prepared());
        }
    }

    private Path tableDir() throws IOException {
        try (Stream<Path> dirs = Files.list(root())) {
            return dirs.filter(p -> p.getFileName().toString().startsWith("table-")).findFirst()
                    .orElseThrow(() -> new IOException("missing registered test table"));
        }
    }

    private static String childOutput(Path temp) {
        try {
            return Files.readString(temp.resolve("crash.log"));
        } catch (IOException unavailable) {
            return unavailable.toString();
        }
    }

    static final class NodeMap implements KVMap<Object> {
        private Object value;
        private boolean failReady;
        @Override public Object get(String key) { return value; }
        @Override public void put(String key, Object val) {
            if (failReady) {
                failReady = false;
                throw new IllegalStateException("injected initial KV outage");
            }
            value = val;
        }
        @Override public Object putIfAbsent(String key, Object val) {
            Object old = value;
            if (old == null) value = val;
            return old;
        }
        @Override public void init(String name, Class<Object> type) {}
        @Override public Object remove(String key) { Object old = value; value = null; return old; }
        @Override public void clear() { value = null; }
        @Override public void reset() { clear(); }
    }
}
