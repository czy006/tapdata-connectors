package io.tapdata.connector.paimon.recovery;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PreparedMessageCodecTest {
    @TempDir Path tmp;

    @Test
    void rejectsUnknownNativeVersionAndUnboundedMessageCountBeforeDecode() throws Exception {
        byte[] nativeBytes = PreparedBatch.payload(Collections.emptyList());
        ByteBuffer.wrap(nativeBytes).putInt(999);
        assertThrows(IOException.class, () -> batch(nativeBytes).messages());
        byte[] enormousCount = PreparedBatch.payload(Collections.emptyList());
        ByteBuffer.wrap(enormousCount).putInt(4, Integer.MAX_VALUE);
        assertThrows(IOException.class, () -> batch(enormousCount).messages());
    }

    @Test
    void rejectsExtraNativeDataAndRefusesRecordsAboveBound() throws Exception {
        byte[] nativeBytes = PreparedBatch.payload(Collections.emptyList());
        byte[] padded = java.util.Arrays.copyOf(nativeBytes, nativeBytes.length + 1);
        assertThrows(IOException.class, () -> batch(padded).messages());
        List<CommitMessage> messages = Collections.nCopies(PreparedBatch.MAX_MESSAGES + 1, null);
        assertThrows(IOException.class, () -> PreparedBatch.payload(messages));
        Path record = tmp.resolve("limited");
        assertThrows(IOException.class, () -> LocalRecoveryFiles.write(record, new byte[33], 32));
        assertFalse(Files.exists(record));
    }

    @Test
    void truncatedHeaderOrForgedLengthDoesNotAllocateAnUnboundedBuffer() throws Exception {
        Path file = tmp.resolve("record");
        Files.write(file, new byte[5]);
        assertThrows(IOException.class, () -> LocalRecoveryFiles.read(file, 10));
        LocalRecoveryFiles.write(file, new byte[] {7}, 10);
        byte[] bytes = Files.readAllBytes(file);
        ByteBuffer.wrap(bytes).putInt(4, Integer.MAX_VALUE);
        Files.write(file, bytes);
        assertThrows(IOException.class, () -> LocalRecoveryFiles.read(file, 10));
    }

    @Test
    void rejectsSwappedEpochAndWrongBatchOrdinal() throws Exception {
        byte[] data = PreparedBatch.record("epoch", 1, 0, 1, zeroDigest(), false, Collections.emptySet(),
                PreparedBatch.payload(Collections.singletonList(new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW, 2, 4, DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement()))));
        assertThrows(IOException.class, () -> PreparedBatch.decodeRecord("another", 1, data));
        assertThrows(IOException.class, () -> PreparedBatch.decodeRecord("epoch", 2, data));
        assertEquals(2, PreparedBatch.decodeRecord("epoch", 1, data).messages().get(0).bucket());
    }

    private static PreparedBatch batch(byte[] nativeBytes) throws IOException {
        return PreparedBatch.decodeRecord("e", 1,
                PreparedBatch.record("e", 1, 11, 1, zeroDigest(), true, Collections.emptySet(), nativeBytes));
    }

    private static String zeroDigest() {
        return LocalRecoveryFiles.hex(new byte[32]);
    }
}
