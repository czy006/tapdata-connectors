package io.tapdata.connector.paimon.recovery;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Same-directory durable replacement, with a versioned, bounded and checksummed payload. */
final class LocalRecoveryFiles {
    private static final int MAGIC = 0x504C5231; // PLR1
    private static final int HEADER = 8;
    static final int DIGEST_SIZE = 32;
    static final int METADATA_LIMIT = 4 * 1024 * 1024;

    private LocalRecoveryFiles() {}

    static void createDirectory(Path dir) throws IOException {
        if (!Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
            Path parent = dir.getParent();
            if (parent == null) {
                throw new IOException("Cannot create filesystem root as recovery directory");
            }
            createDirectory(parent);
            try {
                if (dir.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                    Files.createDirectory(dir, PosixFilePermissions.asFileAttribute(
                            PosixFilePermissions.fromString("rwx------")));
                } else {
                    Files.createDirectory(dir);
                }
            } catch (java.nio.file.FileAlreadyExistsException concurrent) {
                if (!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
                    throw concurrent;
                }
            }
            syncDirectory(parent);
        }
        if (!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Invalid recovery directory: " + dir);
        }
    }

    static void syncDirectory(Path dir) throws IOException {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    static String hex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        final String digits = "0123456789abcdef";
        for (byte b : bytes) {
            hex.append(digits.charAt((b & 255) >>> 4)).append(digits.charAt(b & 15));
        }
        return hex.toString();
    }

    static byte[] read(Path file, int limit) throws IOException {
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Missing or invalid recovery record: " + file);
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ,
                LinkOption.NOFOLLOW_LINKS);
             DataInputStream in = new DataInputStream(java.nio.channels.Channels.newInputStream(channel))) {
            long size = channel.size();
            if (size < HEADER + DIGEST_SIZE || size > (long) limit + HEADER + DIGEST_SIZE) {
                throw new IOException("Invalid recovery record size: " + file);
            }
            int magic = in.readInt();
            int length = in.readInt();
            if (magic != MAGIC || length != size - HEADER - DIGEST_SIZE) {
                throw new IOException("Invalid recovery record header: " + file);
            }
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            byte[] actual = new byte[DIGEST_SIZE];
            in.readFully(actual);
            if (!Arrays.equals(digest(bytes), actual) || in.read() != -1) {
                throw new IOException("Corrupt recovery record: " + file);
            }
            return bytes;
        }
    }

    static void write(Path file, byte[] bytes, int limit) throws IOException {
        write(file, bytes, limit, (phase, target) -> {});
    }

    enum Phase { FILE_FORCE, RENAME, DIRECTORY_FORCE }

    interface BeforeSync {
        void check(Phase phase, Path target) throws IOException;
    }

    static void write(Path file, byte[] bytes, int limit, BeforeSync before) throws IOException {
        if (bytes.length > limit || Files.isSymbolicLink(file)) {
            throw new IOException("Recovery record exceeds bound or targets a link: " + file);
        }
        // Fixed temp path, exclusive directory owner; an interrupted temp write is never visible.
        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER);
            header.putInt(MAGIC).putInt(bytes.length);
            header.flip();
            writeFully(channel, header);
            writeFully(channel, ByteBuffer.wrap(bytes));
            writeFully(channel, ByteBuffer.wrap(digest(bytes)));
            before.check(Phase.FILE_FORCE, file);
            channel.force(true);
        }
        before.check(Phase.RENAME, file);
        Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        before.check(Phase.DIRECTORY_FORCE, file);
        syncDirectory(file.getParent());
    }

    private static void writeFully(FileChannel channel, ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            channel.write(bytes);
        }
    }

    interface Encoder {
        void write(DataOutputStream out) throws IOException;
    }

    static byte[] encode(int limit, Encoder encoder) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        OutputStream bounded = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                ensureRoom(1);
                bytes.write(b);
            }
            @Override
            public void write(byte[] data, int start, int count) throws IOException {
                ensureRoom(count);
                bytes.write(data, start, count);
            }
            private void ensureRoom(int additional) throws IOException {
                if (additional < 0 || additional > limit - bytes.size()) {
                    throw new IOException("Recovery record exceeds " + limit + " bytes");
                }
            }
        };
        try (DataOutputStream out = new DataOutputStream(bounded)) {
            encoder.write(out);
        }
        return bytes.toByteArray();
    }
}
