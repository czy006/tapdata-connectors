package io.tapdata.connector.paimon.recovery;

import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/** Ordered Paimon messages for one completed PDK input, before it is acknowledged. */
final class PreparedBatch {
    static final int MAX_BYTES = 8 * 1024 * 1024;
    static final int MAX_MESSAGES = 4096;
    private static final int MAX_SOURCES = 1024;
    final long sequence;
    final long prepareId;
    final long groupStart;
    final String previousDigest;
    final String digest;
    final byte[] payload;

    private PreparedBatch(long sequence, long prepareId, long groupStart, String previousDigest,
            String digest, byte[] payload) {
        this.sequence = sequence;
        this.prepareId = prepareId;
        this.groupStart = groupStart;
        this.previousDigest = previousDigest;
        this.digest = digest;
        this.payload = payload;
    }

    // The 1.3.2 serializer includes both business and compaction increments and their indexes.
    // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessageSerializer.java#L72
    static byte[] payload(List<CommitMessage> messages) throws IOException {
        Objects.requireNonNull(messages, "ordered commit messages");
        if (messages.size() > MAX_MESSAGES) {
            throw new IOException("Too many commit messages in one prepare");
        }
        return LocalRecoveryFiles.encode(MAX_BYTES, out -> {
            CommitMessageSerializer codec = new CommitMessageSerializer();
            out.writeInt(codec.getVersion());
            codec.serializeList(messages, new DataOutputViewStreamWrapper(out));
        });
    }

    static byte[] record(String tableEpoch, long sequence, long prepareId, long groupStart,
            String previousDigest, boolean cdc, Collection<String> sources, byte[] payload)
            throws IOException {
        Objects.requireNonNull(sources, "source lanes");
        if (sources.size() > MAX_SOURCES) {
            throw new IOException("Too many source lanes in one prepare");
        }
        return LocalRecoveryFiles.encode(MAX_BYTES, out -> {
            out.writeUTF(tableEpoch);
            out.writeLong(sequence);
            out.writeLong(prepareId);
            out.writeLong(groupStart);
            out.writeUTF(previousDigest);
            out.writeBoolean(cdc);
            out.writeInt(sources.size());
            for (String source : sources) {
                out.writeUTF(Objects.requireNonNull(source, "source lane"));
            }
            out.write(payload);
        });
    }

    static PreparedBatch decodeRecord(String tableEpoch, long expected, byte[] bytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (!tableEpoch.equals(in.readUTF())) {
                throw new IOException("Recovery batch belongs to another table epoch");
            }
            long seq = in.readLong();
            long id = in.readLong();
            long start = in.readLong();
            String previous = in.readUTF();
            if (seq != expected || id < 0 || start <= 0 || start > seq || previous.length() != 64) {
                throw new IOException("Invalid recovery batch sequence or predecessor");
            }
            in.readBoolean(); // stage and lanes are durable; recovered prefixes publish before new ingress.
            int lanes = in.readInt();
            if (lanes < 0 || lanes > MAX_SOURCES) {
                throw new IOException("Invalid source-lane count");
            }
            for (int i = 0; i < lanes; i++) {
                in.readUTF();
            }
            byte[] nativeBytes = new byte[in.available()];
            in.readFully(nativeBytes);
            if (nativeBytes.length < 8) {
                throw new IOException("Missing native commit messages");
            }
            return new PreparedBatch(seq, id, start, previous,
                    LocalRecoveryFiles.hex(LocalRecoveryFiles.digest(bytes)), nativeBytes);
        }
    }

    int messageCount() throws IOException {
        ByteBuffer header = ByteBuffer.wrap(payload);
        int version = header.getInt();
        int count = header.getInt();
        if (version != CommitMessageSerializer.CURRENT_VERSION || count < 0 || count > MAX_MESSAGES
                || count > payload.length - 8) {
            throw new IOException("Unsupported native serializer version or invalid message count");
        }
        return count;
    }

    List<CommitMessage> messages() throws IOException {
        messageCount();
        CommitMessageSerializer codec = new CommitMessageSerializer();
        int version = ByteBuffer.wrap(payload).getInt();
        ByteArrayInputStream input = new ByteArrayInputStream(payload, 4, payload.length - 4);
        try {
            List<CommitMessage> result = codec.deserializeList(version, new DataInputViewStreamWrapper(input));
            if (input.available() != 0) {
                throw new IOException("Trailing data in native commit messages");
            }
            return result;
        } catch (RuntimeException malformed) {
            throw new IOException("Invalid native commit messages", malformed);
        }
    }
}
