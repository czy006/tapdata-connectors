package io.tapdata.connector.paimon.recovery;

import io.tapdata.entity.utils.cache.KVMap;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;

/** Fixed-inode lease for the full lifetime of a target's local recovery directory. */
final class LocalRecoveryBinding implements AutoCloseable {
    static final String STATE_KEY = "paimonPlusLocalRecoveryBindingV1";
    private final FileChannel channel;
    final Path directory;

    private LocalRecoveryBinding(Path directory, FileChannel channel) {
        this.directory = directory;
        this.channel = channel;
    }

    static LocalRecoveryBinding open(Path requested, KVMap<Object> map) throws IOException {
        if (requested == null || !requested.isAbsolute() || map == null) {
            throw new IOException("Recovery requires an absolute task directory and a node state map");
        }
        Path root = requested.normalize();
        if (Files.isSymbolicLink(root) || root.getParent() == null) {
            throw new IOException("Unsafe local recovery directory: " + root);
        }
        // Resolve existing ancestors first (macOS /var and /tmp are system aliases). The
        // configured directory itself and all journal-managed descendants must not be links.
        Path ancestor = root;
        while (!Files.exists(ancestor, LinkOption.NOFOLLOW_LINKS)) {
            ancestor = ancestor.getParent();
        }
        root = ancestor.toRealPath().resolve(ancestor.relativize(root));
        LocalRecoveryFiles.createDirectory(root);
        String fsType = Files.getFileStore(root).type().toLowerCase(Locale.ROOT);
        if (fsType.startsWith("nfs") || fsType.contains("smb") || fsType.contains("sshfs")
                || Arrays.asList("cifs", "afpfs", "9p", "ceph", "tmpfs", "ramfs", "webdav").contains(fsType)) {
            throw new IOException("Recovery requires a persistent local filesystem, found " + fsType);
        }
        FileChannel channel = FileChannel.open(root.resolve("owner.lock"), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        LocalRecoveryBinding binding = new LocalRecoveryBinding(root, channel);
        try {
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException occupied) {
                throw new IOException("Recovery directory is already locked: " + root, occupied);
            }
            if (lock == null) {
                throw new IOException("Recovery directory is already locked: " + root);
            }
            LocalRecoveryFiles.syncDirectory(root);
            binding.bind(map);
            return binding;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                binding.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void bind(KVMap<Object> map) throws IOException {
        Path marker = directory.resolve("binding");
        Path registry = directory.resolve("tables");
        Object stored = map.get(STATE_KEY);
        Object fileKey = Files.readAttributes(directory, BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS).fileKey();
        Object lockKey = Files.readAttributes(directory.resolve("owner.lock"), BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS).fileKey();
        if (fileKey == null || lockKey == null) {
            throw new IOException("Recovery filesystem does not identify the directory and lock inodes");
        }
        String location = InetAddress.getLocalHost().getHostName() + "\n" + directory + "\n" + fileKey + "\n" + lockKey;
        String place = LocalRecoveryFiles.hex(LocalRecoveryFiles.digest(location.getBytes(StandardCharsets.UTF_8)));
        if (stored == null) {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
                for (Path file : files) {
                    if (!file.getFileName().toString().equals("owner.lock")) {
                        throw new IOException("Unbound recovery directory contains existing state: " + directory);
                    }
                }
            }
            String candidate = "pending/" + place + "/" + UUID.randomUUID();
            map.putIfAbsent(STATE_KEY, candidate);
            stored = map.get(STATE_KEY);
        }
        if (!(stored instanceof String)) {
            throw new IOException("Missing or malformed TapData recovery binding");
        }
        String value = (String) stored;
        String[] parts = value.split("/", -1);
        if (parts.length != 3 || !place.equals(parts[1])
                || !("pending".equals(parts[0]) || "ready".equals(parts[0]))) {
            throw new IOException("Local recovery binding belongs to another host, directory or task");
        }
        try {
            UUID.fromString(parts[2]);
        } catch (IllegalArgumentException malformed) {
            throw new IOException("Malformed recovery binding UUID", malformed);
        }
        String identity = place + "/" + parts[2];
        if ("pending".equals(parts[0])) {
            // A pending KV intent can only have initialized the empty inventory. An older
            // KV copy must never be used to regenerate binding evidence for a populated task.
            try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
                for (Path file : files) {
                    String name = file.getFileName().toString();
                    if (!Arrays.asList("owner.lock", "binding", "binding.tmp", "tables", "tables.tmp")
                            .contains(name) || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                        throw new IOException("Pending binding contains unexpected recovery data: " + directory);
                    }
                }
            }
            if (!Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) {
                LocalRecoveryFiles.write(marker, identity.getBytes(StandardCharsets.UTF_8), 256);
            }
            verifyMarker(marker, identity);
            if (!Files.exists(registry, LinkOption.NOFOLLOW_LINKS)) {
                LocalRecoveryFiles.write(registry, new byte[4], LocalRecoveryFiles.METADATA_LIMIT);
            } else if (!Arrays.equals(LocalRecoveryFiles.read(registry, LocalRecoveryFiles.METADATA_LIMIT),
                    new byte[4])) {
                throw new IOException("Pending binding has a populated recovery inventory: " + directory);
            }
            // READY is recorded only after both on-disk records and their directory are durable.
            String ready = "ready/" + identity;
            map.put(STATE_KEY, ready);
            if (!ready.equals(map.get(STATE_KEY))) {
                throw new IOException("Recovery binding was not visible in node state");
            }
        } else {
            verifyMarker(marker, identity);
        }
        // Caller reads the checksummed registry even when no table is pending.
    }

    private static void verifyMarker(Path marker, String identity) throws IOException {
        if (!identity.equals(new String(LocalRecoveryFiles.read(marker, 256), StandardCharsets.UTF_8))) {
            throw new IOException("Mismatch between local binding and TapData node state");
        }
    }

    @Override
    public void close() throws IOException {
        // Closing the channel also releases its lock. Never unlink the lock inode.
        channel.close();
    }
}
