package io.tapdata.connector.paimon.recovery;

import com.google.gson.Gson;
import io.tapdata.entity.utils.cache.KVMap;
import org.apache.paimon.table.sink.CommitMessage;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Task-owned inventory of native prepared messages. Each table has a durable, continuous tail.
 * Batch files are immutable while reachable. Only an atomic table-state replacement admits a batch
 * or confirms publication. A table registration is visible only after the root inventory is synced.
 */
public final class PreparedMessageStore implements AutoCloseable {
    private static final Gson GSON = new Gson();
    private static final int MAX_TABLES = 65536;
    private static final int GROUP_BYTES = 16 * 1024 * 1024;
    private static final int GROUP_MESSAGES = 8192;
    private static final int GROUP_BATCHES = 1024;
    private static final String INITIAL_DIGEST = LocalRecoveryFiles.hex(new byte[32]);
    private final LocalRecoveryBinding binding;
    private final LocalRecoveryFiles.BeforeSync beforeSync;
    private final Map<String, Table> tables = new LinkedHashMap<>();
    private volatile boolean closed;
    private boolean registrationFailed;

    private PreparedMessageStore(LocalRecoveryBinding binding, LocalRecoveryFiles.BeforeSync beforeSync) {
        this.binding = binding;
        this.beforeSync = beforeSync;
    }

    /** A pending or malformed marker also reserves the target and requires explicit recovery. */
    public static boolean hasBinding(KVMap<Object> nodeState) {
        return nodeState.get(LocalRecoveryBinding.STATE_KEY) != null;
    }

    public static PreparedMessageStore open(Path directory, KVMap<Object> nodeState) throws IOException {
        return open(directory, nodeState, (phase, target) -> {});
    }

    static PreparedMessageStore open(Path directory, KVMap<Object> nodeState,
            LocalRecoveryFiles.BeforeSync beforeSync) throws IOException {
        LocalRecoveryBinding binding = LocalRecoveryBinding.open(directory, nodeState);
        PreparedMessageStore store = new PreparedMessageStore(binding, beforeSync);
        try {
            store.loadTables();
            return store;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                binding.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void loadTables() throws IOException {
        byte[] registry = LocalRecoveryFiles.read(binding.directory.resolve("tables"),
                LocalRecoveryFiles.METADATA_LIMIT);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(registry))) {
            int count = in.readInt();
            if (count < 0 || count > MAX_TABLES) {
                throw new IOException("Invalid registered table count");
            }
            for (int i = 0; i < count; i++) {
                String database = in.readUTF();
                String name = in.readUTF();
                String epoch = in.readUTF();
                String key = tableKey(database, name);
                Path dir = binding.directory.resolve(key);
                if (!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Missing registered table directory: " + dir);
                }
                TableState state = readState(dir);
                if (!database.equals(state.database) || !name.equals(state.name)
                        || !epoch.equals(state.epoch) || tables.containsKey(key)) {
                    throw new IOException("Recovery table inventory and state disagree: " + dir);
                }
                Table table = new Table(this, dir, state);
                table.discardUnregisteredTail();
                tables.put(key, table);
            }
            if (in.read() != -1) {
                throw new IOException("Trailing recovery inventory data");
            }
        }
        try (DirectoryStream<Path> actual = Files.newDirectoryStream(binding.directory, "table-*")) {
            for (Path dir : actual) {
                if (!tables.containsKey(dir.getFileName().toString())) {
                    verifyUnregisteredDirectory(dir);
                }
            }
        }
    }

    private static void verifyUnregisteredDirectory(Path dir) throws IOException {
        // The sole legal orphan is a registration cut off before its inventory entry was durable.
        // No call can stage a batch for such a table. Never hide data by trusting an empty inventory.
        if (!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Invalid unregistered recovery directory: " + dir);
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
            for (Path file : entries) {
                String name = file.getFileName().toString();
                if ((!name.equals("state.tmp") && !name.equals("state"))
                        || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Unregistered table contains recovery data: " + dir);
                }
            }
        }
        if (Files.exists(dir.resolve("state"), LinkOption.NOFOLLOW_LINKS) && readState(dir).prepared != 0) {
            throw new IOException("Unregistered table contains a durable prefix: " + dir);
        }
    }

    private static TableState readState(Path dir) throws IOException {
        byte[] bytes = LocalRecoveryFiles.read(dir.resolve("state"), LocalRecoveryFiles.METADATA_LIMIT);
        try {
            TableState state = GSON.fromJson(new String(bytes, StandardCharsets.UTF_8), TableState.class);
            if (state == null || state.version != 1 || state.epoch == null || state.anchor == null
                    || state.identity == null || state.commitUser == null || state.firstId < 0
                    || state.gc < 0 || state.gc > state.published || state.published > state.prepared
                    || state.prepared > Long.MAX_VALUE - state.firstId
                    || (state.sealed != 0 && (state.sealed != state.prepared || state.sealed <= state.published))
                    || state.tailHash == null || state.tailHash.length() != 64
                    || state.publishedHash == null || state.publishedHash.length() != 64
                    || state.groupStart < 0 || state.groupStart > state.prepared
                    || state.groupBytes < 0 || state.groupBytes > GROUP_BYTES
                    || state.groupMessages < 0 || state.groupMessages > GROUP_MESSAGES) {
                throw new IOException("Invalid recovery table state: " + dir);
            }
            return state;
        } catch (RuntimeException malformed) {
            throw new IOException("Malformed recovery table state: " + dir, malformed);
        }
    }

    private void writeState(Path dir, TableState state) throws IOException {
        LocalRecoveryFiles.write(dir.resolve("state"), GSON.toJson(state).getBytes(StandardCharsets.UTF_8),
                LocalRecoveryFiles.METADATA_LIMIT, beforeSync);
    }

    private static String tableKey(String database, String name) throws IOException {
        byte[] bytes = LocalRecoveryFiles.encode(131074, out -> {
            out.writeUTF(Objects.requireNonNull(database, "database"));
            out.writeUTF(Objects.requireNonNull(name, "table name"));
        });
        return "table-" + LocalRecoveryFiles.hex(LocalRecoveryFiles.digest(bytes));
    }

    public synchronized List<Table> tables() {
        return Collections.unmodifiableList(new ArrayList<>(tables.values()));
    }

    public synchronized Table find(String database, String name) throws IOException {
        checkOpen();
        return tables.get(tableKey(database, name));
    }

    public synchronized Table register(String database, String name, String identity, String commitUser,
            long nextIdentifier, String initialAnchor) throws IOException {
        checkOpen();
        if (registrationFailed) {
            throw new IOException("Previous table registration failed; reopen recovery directory");
        }
        String key = tableKey(database, name);
        Table existing = tables.get(key);
        if (existing != null) {
            if (!existing.identity().equals(identity) || !existing.commitUser().equals(commitUser)
                    || !existing.anchor().equals(initialAnchor) || existing.nextIdentifier() != nextIdentifier) {
                throw new IOException("Registered table binding changed: " + database + "." + name);
            }
            return existing;
        }
        if (tables.size() >= MAX_TABLES || nextIdentifier < 0 || nextIdentifier == Long.MAX_VALUE) {
            throw new IOException("Recovery table count or commit identifier limit exceeded");
        }
        TableState state = new TableState();
        state.version = 1;
        state.epoch = UUID.randomUUID().toString();
        state.database = database;
        state.name = name;
        state.identity = Objects.requireNonNull(identity, "physical table identity");
        state.commitUser = Objects.requireNonNull(commitUser, "commit user");
        state.anchor = Objects.requireNonNull(initialAnchor, "initial anchor");
        state.firstId = nextIdentifier;
        state.publishedHash = INITIAL_DIGEST;
        state.tailHash = INITIAL_DIGEST;
        Path dir = binding.directory.resolve(key);
        try {
            LocalRecoveryFiles.createDirectory(dir);
            verifyUnregisteredDirectory(dir);
            // An interrupted registration can leave a state file but no inventory entry.
            if (Files.exists(dir.resolve("state"), LinkOption.NOFOLLOW_LINKS)) {
                TableState orphan = readState(dir);
                if (orphan.prepared != 0 || !database.equals(orphan.database) || !name.equals(orphan.name)) {
                    throw new IOException("Unregistered nonempty or unrelated table state: " + dir);
                }
            }
            writeState(dir, state);
            byte[] registry = LocalRecoveryFiles.encode(LocalRecoveryFiles.METADATA_LIMIT, out -> {
                out.writeInt(tables.size() + 1);
                for (Table table : tables.values()) {
                    out.writeUTF(table.database());
                    out.writeUTF(table.name());
                    out.writeUTF(table.epoch);
                }
                out.writeUTF(database);
                out.writeUTF(name);
                out.writeUTF(state.epoch);
            });
            LocalRecoveryFiles.write(binding.directory.resolve("tables"), registry,
                    LocalRecoveryFiles.METADATA_LIMIT, beforeSync);
            Table table = new Table(this, dir, state);
            tables.put(key, table);
            return table;
        } catch (IOException | RuntimeException | Error failure) {
            registrationFailed = true;
            throw failure;
        }
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("Recovery directory has been closed");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            for (Table table : tables.values()) {
                table.awaitOperations();
            }
            binding.close();
        }
    }

    private static final class TableState {
        int version;
        String database;
        String name;
        String epoch;
        String identity;
        String commitUser;
        String anchor;
        long firstId;
        long prepared;
        long published;
        long sealed;
        long gc;
        long groupStart;
        int groupBytes;
        int groupMessages;
        String publishedHash;
        String tailHash;

        TableState() {}

        TableState(TableState s) {
            version = s.version;
            database = s.database;
            name = s.name;
            epoch = s.epoch;
            identity = s.identity;
            commitUser = s.commitUser;
            anchor = s.anchor;
            firstId = s.firstId;
            prepared = s.prepared;
            published = s.published;
            sealed = s.sealed;
            gc = s.gc;
            groupStart = s.groupStart;
            groupBytes = s.groupBytes;
            groupMessages = s.groupMessages;
            publishedHash = s.publishedHash;
            tailHash = s.tailHash;
        }
    }

    public static final class Group {
        private final Table owner;
        private final long first;
        private final long last;
        private final long identifier;
        private final String digest;
        private final List<CommitMessage> messages;

        private Group(Table owner, long first, long last, long identifier, String digest,
                List<CommitMessage> messages) {
            this.owner = owner;
            this.first = first;
            this.last = last;
            this.identifier = identifier;
            this.digest = digest;
            this.messages = Collections.unmodifiableList(messages);
        }

        public long first() { return first; }
        public long last() { return last; }
        public long identifier() { return identifier; }
        public List<CommitMessage> messages() { return messages; }
    }

    public static final class Table {
        private final PreparedMessageStore owner;
        private final Path dir;
        private final String epoch;
        private TableState state;
        private long validatedCutoff;
        private boolean failed;

        private Table(PreparedMessageStore owner, Path dir, TableState state) {
            this.owner = owner;
            this.dir = dir;
            this.epoch = state.epoch;
            this.state = state;
        }

        public synchronized String database() { return state.database; }
        public synchronized String name() { return state.name; }
        public synchronized String identity() { return state.identity; }
        public synchronized String commitUser() { return state.commitUser; }
        public synchronized String anchor() { return state.anchor; }
        public synchronized long nextIdentifier() { return state.firstId + state.prepared; }
        public synchronized long confirmedNextIdentifier() { return state.firstId + state.published; }
        public synchronized long prepared() { return state.prepared; }
        public synchronized long published() { return state.published; }
        public synchronized boolean sealed() { return state.sealed != 0; }

        private synchronized void awaitOperations() {
            // Acquiring the table monitor after closing admission drains any in-flight disk operation.
        }

        private Path batch(long sequence) {
            return dir.resolve("prepare-" + sequence);
        }

        private void discardUnregisteredTail() throws IOException {
            // append is serial and poison-on-error: at most one batch can exist past the
            // effective tail. Never scan arbitrary directory entries or follow their targets.
            boolean changed = Files.deleteIfExists(dir.resolve("state.tmp"));
            if (state.prepared < Long.MAX_VALUE) {
                Path extra = batch(state.prepared + 1);
                changed |= Files.deleteIfExists(extra);
                changed |= Files.deleteIfExists(extra.resolveSibling(extra.getFileName() + ".tmp"));
            }
            if (changed) {
                LocalRecoveryFiles.syncDirectory(dir);
            }
        }

        private void checkWritable() throws IOException {
            owner.checkOpen();
            if (failed) {
                throw new IOException("Previous recovery write failed; reopen table state");
            }
        }

        private void update(TableState next) throws IOException {
            try {
                owner.writeState(dir, next);
                state = next;
            } catch (IOException | RuntimeException | Error failure) {
                // The rename can have succeeded before a failed directory sync. Never reuse the old tail.
                failed = true;
                throw failure;
            }
        }

        public synchronized long append(long prepareId, boolean cdc, Collection<String> sources,
                List<CommitMessage> orderedMessages) throws IOException {
            checkWritable();
            if (sealed() || prepareId != nextIdentifier() || prepareId == Long.MAX_VALUE) {
                throw new IOException("Out-of-sequence or sealed prepare for " + state.name);
            }
            long sequence = state.prepared + 1;
            try {
                return appendPrepared(prepareId, cdc, sources, orderedMessages, sequence);
            } catch (IOException | RuntimeException | Error failure) {
                // Native prepare already drained the writer, even if serialization failed before disk I/O.
                failed = true;
                throw failure;
            }
        }

        private long appendPrepared(long prepareId, boolean cdc, Collection<String> sources,
                List<CommitMessage> orderedMessages, long sequence) throws IOException {
            byte[] payload = PreparedBatch.payload(orderedMessages);
            // The group boundary is embedded in each immutable batch before its ACK. Sealing a
            // window fixes its final cutoff. Restarts never recompute grouping from new thresholds.
            boolean newGroup = state.prepared == state.published || state.groupStart == 0
                    || sequence - state.groupStart >= GROUP_BATCHES
                    || payload.length > GROUP_BYTES - state.groupBytes
                    || orderedMessages.size() > GROUP_MESSAGES - state.groupMessages;
            long start = newGroup ? sequence : state.groupStart;
            byte[] record = PreparedBatch.record(epoch, sequence, prepareId, start, state.tailHash,
                    cdc, sources, payload);
            TableState next = new TableState(state);
            next.prepared = sequence;
            next.groupStart = start;
            next.groupBytes = (newGroup ? 0 : state.groupBytes) + payload.length;
            next.groupMessages = (newGroup ? 0 : state.groupMessages) + orderedMessages.size();
            next.tailHash = LocalRecoveryFiles.hex(LocalRecoveryFiles.digest(record));
            LocalRecoveryFiles.write(batch(sequence), record, PreparedBatch.MAX_BYTES, owner.beforeSync);
            update(next);
            return sequence;
        }

        public synchronized void seal() throws IOException {
            checkWritable();
            if (!sealed() && state.prepared > state.published) {
                TableState next = new TableState(state);
                next.sealed = state.prepared;
                update(next);
            }
        }

        public synchronized Group nextGroup() throws IOException {
            checkWritable();
            if (!sealed()) {
                return null;
            }
            if (validatedCutoff != state.sealed) {
                verifySealedTail();
                validatedCutoff = state.sealed;
            }
            long first = state.published + 1;
            long last = state.published;
            String digest = state.publishedHash;
            List<CommitMessage> messages = new ArrayList<>();
            int totalBytes = 0;
            while (last < state.sealed) {
                long seq = last + 1;
                PreparedBatch entry = readOrdered(seq, digest);
                if (seq > first && entry.groupStart == seq) {
                    break;
                }
                if (entry.groupStart != first || seq - first >= GROUP_BATCHES
                        || entry.payload.length > GROUP_BYTES - totalBytes) {
                    throw new IOException("Invalid sealed publication group for " + state.name);
                }
                List<CommitMessage> added = entry.messages();
                if (added.size() > GROUP_MESSAGES - messages.size()) {
                    throw new IOException("Too many messages in sealed group");
                }
                messages.addAll(added);
                totalBytes += entry.payload.length;
                digest = entry.digest;
                last = seq;
            }
            if (last == state.sealed && !digest.equals(state.tailHash)) {
                throw new IOException("Recovery tail fingerprint mismatch for " + state.name);
            }
            return new Group(this, first, last, state.firstId + (last - 1), digest, messages);
        }

        private PreparedBatch readOrdered(long seq, String previous) throws IOException {
            PreparedBatch entry = PreparedBatch.decodeRecord(epoch, seq,
                    LocalRecoveryFiles.read(batch(seq), PreparedBatch.MAX_BYTES));
            if (!previous.equals(entry.previousDigest) || entry.prepareId != state.firstId + (seq - 1)) {
                throw new IOException("Broken ordered recovery chain for " + state.name);
            }
            return entry;
        }

        /** Check every sealed sub-group against the durable tail before returning the first one. */
        private void verifySealedTail() throws IOException {
            long seq = state.published;
            long group = 0;
            int bytes = 0;
            int messages = 0;
            String previous = state.publishedHash;
            while (seq < state.sealed) {
                PreparedBatch entry = readOrdered(++seq, previous);
                if (entry.groupStart == seq) {
                    group = seq;
                    bytes = 0;
                    messages = 0;
                }
                int count = entry.messageCount();
                if (entry.groupStart != group || seq - group >= GROUP_BATCHES
                        || entry.payload.length > GROUP_BYTES - bytes || count > GROUP_MESSAGES - messages) {
                    throw new IOException("Invalid immutable publication plan for " + state.name);
                }
                bytes += entry.payload.length;
                messages += count;
                previous = entry.digest;
            }
            if (!previous.equals(state.tailHash)) {
                throw new IOException("Sealed recovery chain does not reach its durable tail: " + state.name);
            }
        }

        public synchronized void confirm(Group group, String newAnchor) throws IOException {
            checkWritable();
            if (group == null || group.owner != this || !sealed() || group.first != state.published + 1
                    || group.last > state.sealed) {
                throw new IOException("Stale publication group for " + state.name);
            }
            TableState next = new TableState(state);
            next.anchor = Objects.requireNonNull(newAnchor, "confirmed snapshot anchor");
            next.published = group.last;
            next.publishedHash = group.digest;
            if (next.published == next.sealed) {
                next.sealed = 0;
            }
            update(next);
        }

        /** Called only after the caller has reconciled the durably confirmed native ID with task KV. */
        public synchronized void cleanup() throws IOException {
            checkWritable();
            if (state.gc == state.published) {
                return;
            }
            long seq = state.gc;
            while (seq < state.published) {
                Files.deleteIfExists(batch(++seq));
            }
            LocalRecoveryFiles.syncDirectory(dir);
            TableState next = new TableState(state);
            next.gc = next.published;
            update(next);
        }
    }
}
