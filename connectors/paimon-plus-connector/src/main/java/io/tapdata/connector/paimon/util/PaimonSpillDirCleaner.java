package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Owns Paimon spill-manager locks and removes stale managers only through stable local path
 * capabilities and descriptor-relative operations.
 */
public final class PaimonSpillDirCleaner {

    static final String SPILL_DIR_PREFIX = "paimon-io-";
    static final String OWNER_LOCK_SUFFIX = ".tapdata-owner.lock";
    private static final int MAX_SECURE_TREE_DEPTH = 256;

    public static final long DEFAULT_STALE_GRACE_MS = TimeUnit.MINUTES.toMillis(10);

    private static final Set<String> LIVE_DIRS = ConcurrentHashMap.newKeySet();
    private static final Map<String, OwnerLock> OWNER_LOCKS = new ConcurrentHashMap<>();
    private static final BeforeDeleteHook NOOP_BEFORE_DELETE = (root, managerName) -> {};
    private static final SecureRootOpener PLATFORM_SECURE_ROOT =
            PaimonSpillPathCapability.ApprovedRoot::openSecure;

    private PaimonSpillDirCleaner() {}

    public static long saturatedAdd(long left, long right) {
        if (right > 0L && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    public static String resolveTmpDirs(String configuredTmpDirs) {
        if (configuredTmpDirs == null || configuredTmpDirs.trim().isEmpty()) {
            return System.getProperty("java.io.tmpdir", new File(".").getAbsolutePath());
        }
        return configuredTmpDirs;
    }

    public static String[] splitTmpDirRoots(String resolvedTmpDirs) {
        return IOManagerImpl.splitPaths(resolvedTmpDirs);
    }

    /**
     * Creates an IOManager and acquires exact owner locks for every manager directory.
     *
     * <p>Registration failure closes the unreturned manager. No caller can receive an IOManager
     * whose spill directory lacks a stable approved-root capability.
     */
    public static IOManagerBuildResult resolveAndCreateIOManager(String configuredTmpDirs) {
        String[] roots = splitTmpDirRoots(resolveTmpDirs(configuredTmpDirs));
        IOManager ioManager = IOManager.create(roots);
        try {
            List<String> spillDirs = registerLiveDirs(ioManager);
            return new IOManagerBuildResult(ioManager, spillDirs);
        } catch (RuntimeException failure) {
            try {
                ioManager.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    public static final class IOManagerBuildResult {
        private final IOManager ioManager;
        private final List<String> spillDirs;

        private IOManagerBuildResult(IOManager ioManager, List<String> spillDirs) {
            this.ioManager = ioManager;
            this.spillDirs = spillDirs;
        }

        public IOManager ioManager() {
            return ioManager;
        }

        public List<String> spillDirs() {
            return spillDirs;
        }
    }

    /**
     * Registers only strict, direct-child Paimon UUID manager directories under a stable local root.
     */
    public static List<String> registerLiveDirs(IOManager ioManager) {
        Objects.requireNonNull(ioManager, "ioManager");
        List<Path> managers = spillDirPaths(ioManager);
        List<String> registered = new ArrayList<>();
        try {
            for (Path manager : managers) {
                OwnerLock ownerLock = OwnerLock.tryAcquireManager(manager, true);
                if (ownerLock == null) {
                    throw new IllegalStateException(
                            "Paimon spill manager lacks an exclusive stable owner capability");
                }
                String path = ownerLock.managerPath();
                OwnerLock raced = OWNER_LOCKS.putIfAbsent(path, ownerLock);
                if (raced != null) {
                    ownerLock.close(false);
                    throw new IllegalStateException(
                            "Paimon spill manager is already registered in this JVM");
                }
                LIVE_DIRS.add(path);
                registered.add(path);
            }
            return registered;
        } catch (RuntimeException failure) {
            try {
                unregisterLiveDirs(registered);
            } catch (RuntimeException rollbackFailure) {
                failure.addSuppressed(rollbackFailure);
            }
            throw failure;
        }
    }

    /** Releases only the exact owner locks named by the registration result. */
    public static void unregisterLiveDirs(List<String> spillDirs) {
        RuntimeException failure = null;
        if (spillDirs != null) {
            for (String path : spillDirs) {
                LIVE_DIRS.remove(path);
                OwnerLock ownerLock = OWNER_LOCKS.remove(path);
                if (ownerLock != null) {
                    try {
                        ownerLock.close(true);
                    } catch (RuntimeException closeFailure) {
                        if (failure == null) {
                            failure = closeFailure;
                        } else {
                            failure.addSuppressed(closeFailure);
                        }
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static List<Path> spillDirPaths(IOManager ioManager) {
        List<Path> paths = new ArrayList<>();
        if (ioManager instanceof IOManagerImpl) {
            File[] dirs = ((IOManagerImpl) ioManager).getSpillingDirectories();
            if (dirs != null) {
                for (File dir : dirs) {
                    paths.add(dir.toPath().toAbsolutePath().normalize());
                }
            }
        }
        return paths;
    }

    /**
     * Deletes stale managers and reports only the lowercase UUID manager id to the callback.
     *
     * <p>The callback never receives the configured root or a credential-bearing URI.
     */
    public static int cleanupStaleSpillDirs(
            String[] roots, long graceMs, BiConsumer<String, Long> onDeleted) {
        return cleanupStaleSpillDirs(roots, graceMs, onDeleted, null);
    }

    /**
     * Deletes stale managers while rejecting a temp root equal to the configured local warehouse.
     */
    public static int cleanupStaleSpillDirs(
            String[] roots,
            long graceMs,
            BiConsumer<String, Long> onDeleted,
            String warehouseRoot) {
        return cleanupStaleSpillDirs(
                roots,
                graceMs,
                onDeleted,
                warehouseRoot,
                NOOP_BEFORE_DELETE,
                PLATFORM_SECURE_ROOT);
    }

    static int cleanupStaleSpillDirs(
            String[] roots,
            long graceMs,
            BiConsumer<String, Long> onDeleted,
            String warehouseRoot,
            BeforeDeleteHook beforeDelete) {
        return cleanupStaleSpillDirs(
                roots,
                graceMs,
                onDeleted,
                warehouseRoot,
                beforeDelete,
                PLATFORM_SECURE_ROOT);
    }

    static int cleanupStaleSpillDirs(
            String[] roots,
            long graceMs,
            BiConsumer<String, Long> onDeleted,
            String warehouseRoot,
            BeforeDeleteHook beforeDelete,
            SecureRootOpener secureRootOpener) {
        if (roots == null) {
            return 0;
        }
        if (graceMs < 0L) {
            throw new IllegalArgumentException("graceMs must not be negative");
        }
        Objects.requireNonNull(beforeDelete, "beforeDelete");
        Objects.requireNonNull(secureRootOpener, "secureRootOpener");
        int deleted = 0;
        long now = System.currentTimeMillis();
        Path workspace = Paths.get(System.getProperty("user.dir", "."));
        for (String configuredRoot : roots) {
            PaimonSpillPathCapability.ApprovedRoot approvedRoot =
                    PaimonSpillPathCapability.approveRoot(
                            configuredRoot, workspace, warehouseRoot);
            if (approvedRoot == null) {
                continue;
            }
            SecureDirectoryStream<Path> scanRoot = null;
            try {
                scanRoot = secureRootOpener.open(approvedRoot);
                if (scanRoot == null) {
                    continue;
                }
                for (Path entry : scanRoot) {
                    Path managerName = entry.getFileName();
                    if (managerName == null
                            || !PaimonSpillPathCapability.isStrictManagerName(
                                    managerName.toString())) {
                        continue;
                    }
                    BasicFileAttributes observed =
                            PaimonSpillPathCapability.readRelative(scanRoot, managerName);
                    Object managerIdentity =
                            PaimonSpillPathCapability.stableIdentity(observed);
                    if (!observed.isDirectory()
                            || observed.isSymbolicLink()
                            || managerIdentity == null) {
                        continue;
                    }
                    String managerPath =
                            approvedRoot.realPath().resolve(managerName).toString();
                    if (LIVE_DIRS.contains(managerPath)) {
                        continue;
                    }
                    OwnerLock cleanupLock =
                            OwnerLock.tryAcquire(
                                    approvedRoot,
                                    managerName,
                                    false,
                                    secureRootOpener);
                    if (cleanupLock == null) {
                        continue;
                    }
                    boolean managerDeleted = false;
                    try {
                        TreeNode tree =
                                scanTree(
                                        cleanupLock.rootStream,
                                        managerName,
                                        cleanupLock.managerIdentity);
                        if (tree == null || now - tree.newestModified < graceMs) {
                            continue;
                        }
                        beforeDelete.run(approvedRoot.realPath(), managerName.toString());
                        if (!approvedRoot.isCurrent()
                                || !cleanupLock.identitiesCurrent()
                                || !deleteDirectoryTree(cleanupLock.rootStream, tree)) {
                            continue;
                        }
                        managerDeleted = true;
                        cleanupLock.deleteOwnerMarker = true;
                        deleted++;
                        if (onDeleted != null) {
                            onDeleted.accept(managerId(managerName), tree.regularFileBytes);
                        }
                    } catch (IOException | SecurityException changedOrUnavailable) {
                        // Fail closed. The exact lock and marker remain unless manager deletion
                        // completed, and no absolute-path fallback is attempted.
                    } finally {
                        cleanupLock.close(managerDeleted);
                    }
                }
            } catch (IOException | SecurityException unavailable) {
                // An unprovable root or provider capability is a skip, never deletion authority.
            } finally {
                closeDirectoryStream(scanRoot);
            }
        }
        return deleted;
    }

    private static String managerId(Path managerName) {
        return managerName.toString().substring(SPILL_DIR_PREFIX.length());
    }

    private static TreeNode scanTree(
            SecureDirectoryStream<Path> parent, Path name, Object expectedIdentity)
            throws IOException {
        BasicFileAttributes attributes =
                PaimonSpillPathCapability.readRelative(parent, name);
        Object identity = PaimonSpillPathCapability.stableIdentity(attributes);
        if (!attributes.isDirectory()
                || attributes.isSymbolicLink()
                || !PaimonSpillPathCapability.sameStableIdentity(
                        expectedIdentity, identity)) {
            return null;
        }
        DirectoryStream<Path> opened =
                parent.newDirectoryStream(name, LinkOption.NOFOLLOW_LINKS);
        SecureDirectoryStream<Path> directory =
                PaimonSpillPathCapability.requireSecure(opened);
        if (directory == null) {
            opened.close();
            return null;
        }
        try {
            return scanOpenedDirectory(
                    name,
                    identity,
                    attributes.lastModifiedTime().toMillis(),
                    directory,
                    0);
        } finally {
            directory.close();
        }
    }

    private static TreeNode scanOpenedDirectory(
            Path name,
            Object identity,
            long lastModified,
            SecureDirectoryStream<Path> directory,
            int depth)
            throws IOException {
        if (depth > MAX_SECURE_TREE_DEPTH) {
            return null;
        }
        List<TreeNode> children = new ArrayList<>();
        long bytes = 0L;
        long newest = lastModified;
        for (Path entry : directory) {
            Path childName = entry.getFileName();
            if (childName == null) {
                return null;
            }
            BasicFileAttributes attributes =
                    PaimonSpillPathCapability.readRelative(directory, childName);
            Object childIdentity =
                    PaimonSpillPathCapability.stableIdentity(attributes);
            if (attributes.isSymbolicLink() || childIdentity == null) {
                return null;
            }
            TreeNode child;
            if (attributes.isDirectory()) {
                DirectoryStream<Path> opened =
                        directory.newDirectoryStream(
                                childName, LinkOption.NOFOLLOW_LINKS);
                SecureDirectoryStream<Path> childDirectory =
                        PaimonSpillPathCapability.requireSecure(opened);
                if (childDirectory == null) {
                    opened.close();
                    return null;
                }
                try {
                    child =
                            scanOpenedDirectory(
                                    childName,
                                    childIdentity,
                                    attributes.lastModifiedTime().toMillis(),
                                    childDirectory,
                                    depth + 1);
                } finally {
                    childDirectory.close();
                }
                if (child == null) {
                    return null;
                }
            } else if (attributes.isRegularFile()) {
                child =
                        TreeNode.file(
                                childName,
                                childIdentity,
                                attributes.lastModifiedTime().toMillis(),
                                attributes.size());
            } else {
                return null;
            }
            children.add(child);
            bytes = saturatedAdd(bytes, child.regularFileBytes);
            if (child.newestModified > newest) {
                newest = child.newestModified;
            }
        }
        return TreeNode.directory(name, identity, newest, bytes, children);
    }

    private static boolean deleteDirectoryTree(
            SecureDirectoryStream<Path> parent, TreeNode directoryNode) {
        try {
            if (!matches(parent, directoryNode, true)) {
                return false;
            }
            DirectoryStream<Path> opened =
                    parent.newDirectoryStream(
                            directoryNode.name, LinkOption.NOFOLLOW_LINKS);
            SecureDirectoryStream<Path> directory =
                    PaimonSpillPathCapability.requireSecure(opened);
            if (directory == null) {
                opened.close();
                return false;
            }
            try {
                Map<String, TreeNode> expected = new HashMap<>();
                for (TreeNode child : directoryNode.children) {
                    expected.put(child.name.toString(), child);
                }
                int observedCount = 0;
                for (Path entry : directory) {
                    Path childName = entry.getFileName();
                    TreeNode child =
                            childName == null ? null : expected.get(childName.toString());
                    if (child == null || !matches(directory, child, child.directory)) {
                        return false;
                    }
                    observedCount++;
                }
                if (observedCount != expected.size()) {
                    return false;
                }
                for (TreeNode child : directoryNode.children) {
                    boolean childDeleted;
                    if (child.directory) {
                        childDeleted = deleteDirectoryTree(directory, child);
                    } else {
                        childDeleted = deleteFile(directory, child);
                    }
                    if (!childDeleted) {
                        return false;
                    }
                }
            } finally {
                directory.close();
            }
            if (!matches(parent, directoryNode, true)) {
                return false;
            }
            parent.deleteDirectory(directoryNode.name);
            return true;
        } catch (IOException | SecurityException changed) {
            return false;
        }
    }

    private static boolean deleteFile(
            SecureDirectoryStream<Path> parent, TreeNode fileNode) {
        try {
            if (!matches(parent, fileNode, false)) {
                return false;
            }
            parent.deleteFile(fileNode.name);
            return true;
        } catch (IOException | SecurityException changed) {
            return false;
        }
    }

    private static boolean matches(
            SecureDirectoryStream<Path> parent, TreeNode node, boolean directory)
            throws IOException {
        BasicFileAttributes current =
                PaimonSpillPathCapability.readRelative(parent, node.name);
        return !current.isSymbolicLink()
                && (directory ? current.isDirectory() : current.isRegularFile())
                && PaimonSpillPathCapability.sameStableIdentity(
                        node.identity,
                        PaimonSpillPathCapability.stableIdentity(current));
    }

    private static void closeDirectoryStream(DirectoryStream<Path> stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException closeFailure) {
                throw new IllegalStateException(
                        "Failed to close a spill directory capability", closeFailure);
            }
        }
    }

    @FunctionalInterface
    interface BeforeDeleteHook {
        void run(Path approvedRoot, String managerName) throws IOException;
    }

    @FunctionalInterface
    interface SecureRootOpener {
        SecureDirectoryStream<Path> open(
                PaimonSpillPathCapability.ApprovedRoot approvedRoot)
                throws IOException;
    }

    private static final class TreeNode {
        private final Path name;
        private final Object identity;
        private final boolean directory;
        private final long newestModified;
        private final long regularFileBytes;
        private final List<TreeNode> children;

        private TreeNode(
                Path name,
                Object identity,
                boolean directory,
                long newestModified,
                long regularFileBytes,
                List<TreeNode> children) {
            this.name = name;
            this.identity = identity;
            this.directory = directory;
            this.newestModified = newestModified;
            this.regularFileBytes = regularFileBytes;
            this.children = children;
        }

        private static TreeNode file(
                Path name, Object identity, long modified, long bytes) {
            return new TreeNode(
                    name,
                    identity,
                    false,
                    modified,
                    bytes,
                    java.util.Collections.emptyList());
        }

        private static TreeNode directory(
                Path name,
                Object identity,
                long newest,
                long bytes,
                List<TreeNode> children) {
            return new TreeNode(name, identity, true, newest, bytes, children);
        }
    }

    private static final class OwnerLock {
        private final PaimonSpillPathCapability.ApprovedRoot approvedRoot;
        private final SecureDirectoryStream<Path> rootStream;
        private final Path managerName;
        private final Path ownerName;
        private final Path ownerAbsolutePath;
        private final Object managerIdentity;
        private final Object ownerIdentity;
        private final FileChannel channel;
        private final FileLock lock;
        private boolean deleteOwnerMarker;

        private OwnerLock(
                PaimonSpillPathCapability.ApprovedRoot approvedRoot,
                SecureDirectoryStream<Path> rootStream,
                Path managerName,
                Path ownerName,
                Path ownerAbsolutePath,
                Object managerIdentity,
                Object ownerIdentity,
                FileChannel channel,
                FileLock lock) {
            this.approvedRoot = approvedRoot;
            this.rootStream = rootStream;
            this.managerName = managerName;
            this.ownerName = ownerName;
            this.ownerAbsolutePath = ownerAbsolutePath;
            this.managerIdentity = managerIdentity;
            this.ownerIdentity = ownerIdentity;
            this.channel = channel;
            this.lock = lock;
        }

        private static OwnerLock tryAcquireManager(Path manager, boolean createMarker) {
            Path normalized = manager.toAbsolutePath().normalize();
            Path parent = normalized.getParent();
            Path name = normalized.getFileName();
            if (parent == null
                    || name == null
                    || !PaimonSpillPathCapability.isStrictManagerName(
                            name.toString())) {
                return null;
            }
            PaimonSpillPathCapability.ApprovedRoot approved =
                    PaimonSpillPathCapability.approveRoot(
                            parent.toString(),
                            Paths.get(System.getProperty("user.dir", ".")),
                            null);
            if (approved == null
                    || !approved.realPath().resolve(name).normalize().equals(normalized)) {
                return null;
            }
            return tryAcquireWithoutDeletionCapability(approved, name, createMarker);
        }

        /**
         * Acquires the live-owner marker without requiring destructive directory capability.
         * Registration uses only exact absolute paths plus root/manager/marker identity checks;
         * platforms without {@link SecureDirectoryStream} can therefore still spill normally.
         */
        private static OwnerLock tryAcquireWithoutDeletionCapability(
                PaimonSpillPathCapability.ApprovedRoot approvedRoot,
                Path managerName,
                boolean createMarker) {
            FileChannel channel = null;
            FileLock fileLock = null;
            try {
                if (!approvedRoot.isCurrent()) {
                    return null;
                }
                Path managerPath = approvedRoot.realPath().resolve(managerName);
                BasicFileAttributes managerAttributes =
                        PaimonSpillPathCapability.readAbsolute(managerPath);
                Object managerIdentity =
                        PaimonSpillPathCapability.stableIdentity(managerAttributes);
                if (!managerAttributes.isDirectory()
                        || managerAttributes.isSymbolicLink()
                        || managerIdentity == null) {
                    return null;
                }
                Path ownerName = ownerName(managerName);
                Path ownerPath = approvedRoot.realPath().resolve(ownerName);
                Set<OpenOption> options = ownerOpenOptions(createMarker);
                channel = FileChannel.open(ownerPath, options);
                BasicFileAttributes ownerAttributes =
                        PaimonSpillPathCapability.readAbsolute(ownerPath);
                Object ownerIdentity =
                        PaimonSpillPathCapability.stableIdentity(ownerAttributes);
                if (!ownerAttributes.isRegularFile()
                        || ownerAttributes.isSymbolicLink()
                        || ownerIdentity == null) {
                    closeAcquisition(null, channel, null);
                    return null;
                }
                fileLock = channel.tryLock();
                if (fileLock == null) {
                    closeAcquisition(null, channel, null);
                    return null;
                }
                OwnerLock ownerLock =
                        new OwnerLock(
                                approvedRoot,
                                null,
                                managerName,
                                ownerName,
                                ownerPath,
                                managerIdentity,
                                ownerIdentity,
                                channel,
                                fileLock);
                if (!ownerLock.identitiesCurrent()) {
                    ownerLock.close(false);
                    return null;
                }
                return ownerLock;
            } catch (OverlappingFileLockException unavailable) {
                closeAcquisition(fileLock, channel, null);
                return null;
            } catch (IOException | RuntimeException unavailable) {
                closeAcquisition(fileLock, channel, null);
                return null;
            }
        }

        private static OwnerLock tryAcquire(
                PaimonSpillPathCapability.ApprovedRoot approvedRoot,
                Path managerName,
                boolean createMarker,
                SecureRootOpener secureRootOpener) {
            SecureDirectoryStream<Path> rootStream = null;
            SeekableByteChannel openedChannel = null;
            FileLock fileLock = null;
            try {
                rootStream = secureRootOpener.open(approvedRoot);
                if (rootStream == null || !approvedRoot.isCurrent()) {
                    closeDirectoryStream(rootStream);
                    return null;
                }
                BasicFileAttributes managerAttributes =
                        PaimonSpillPathCapability.readRelative(
                                rootStream, managerName);
                Object managerIdentity =
                        PaimonSpillPathCapability.stableIdentity(
                                managerAttributes);
                if (!managerAttributes.isDirectory()
                        || managerAttributes.isSymbolicLink()
                        || managerIdentity == null) {
                    closeDirectoryStream(rootStream);
                    return null;
                }
                Path ownerName = ownerName(managerName);
                Set<OpenOption> options = ownerOpenOptions(createMarker);
                openedChannel = rootStream.newByteChannel(ownerName, options);
                if (!(openedChannel instanceof FileChannel)) {
                    openedChannel.close();
                    closeDirectoryStream(rootStream);
                    return null;
                }
                BasicFileAttributes ownerAttributes =
                        PaimonSpillPathCapability.readRelative(
                                rootStream, ownerName);
                Object ownerIdentity =
                        PaimonSpillPathCapability.stableIdentity(ownerAttributes);
                if (!ownerAttributes.isRegularFile()
                        || ownerAttributes.isSymbolicLink()
                        || ownerIdentity == null) {
                    openedChannel.close();
                    closeDirectoryStream(rootStream);
                    return null;
                }
                FileChannel channel = (FileChannel) openedChannel;
                fileLock = channel.tryLock();
                if (fileLock == null) {
                    channel.close();
                    closeDirectoryStream(rootStream);
                    return null;
                }
                OwnerLock ownerLock =
                        new OwnerLock(
                                approvedRoot,
                                rootStream,
                                managerName,
                                ownerName,
                                approvedRoot.realPath().resolve(ownerName),
                                managerIdentity,
                                ownerIdentity,
                                channel,
                                fileLock);
                if (!ownerLock.identitiesCurrent()) {
                    ownerLock.close(false);
                    return null;
                }
                return ownerLock;
            } catch (OverlappingFileLockException unavailable) {
                closeAcquisition(fileLock, openedChannel, rootStream);
                return null;
            } catch (IOException | RuntimeException unavailable) {
                closeAcquisition(fileLock, openedChannel, rootStream);
                return null;
            }
        }

        private static Path ownerName(Path managerName) {
            return managerName.resolveSibling(
                    "." + managerName + OWNER_LOCK_SUFFIX);
        }

        private static Set<OpenOption> ownerOpenOptions(boolean createMarker) {
            Set<OpenOption> options =
                    new HashSet<>(
                            Arrays.asList(
                                    StandardOpenOption.READ,
                                    StandardOpenOption.WRITE,
                                    LinkOption.NOFOLLOW_LINKS));
            if (createMarker) {
                options.add(StandardOpenOption.CREATE);
            }
            return options;
        }

        private String managerPath() {
            return approvedRoot.realPath().resolve(managerName).toString();
        }

        private boolean identitiesCurrent() {
            try {
                BasicFileAttributes manager;
                BasicFileAttributes owner;
                if (rootStream == null) {
                    manager =
                            PaimonSpillPathCapability.readAbsolute(
                                    approvedRoot.realPath().resolve(managerName));
                    owner = PaimonSpillPathCapability.readAbsolute(ownerAbsolutePath);
                } else {
                    manager =
                            PaimonSpillPathCapability.readRelative(
                                    rootStream, managerName);
                    owner =
                            PaimonSpillPathCapability.readRelative(
                                    rootStream, ownerName);
                }
                return approvedRoot.isCurrent()
                        && manager.isDirectory()
                        && !manager.isSymbolicLink()
                        && owner.isRegularFile()
                        && !owner.isSymbolicLink()
                        && PaimonSpillPathCapability.sameStableIdentity(
                                managerIdentity,
                                PaimonSpillPathCapability.stableIdentity(manager))
                        && PaimonSpillPathCapability.sameStableIdentity(
                                ownerIdentity,
                                PaimonSpillPathCapability.stableIdentity(owner));
            } catch (IOException | RuntimeException changed) {
                return false;
            }
        }

        private void close(boolean managerDeleted) {
            IOException failure = null;
            try {
                lock.release();
            } catch (IOException releaseFailure) {
                failure = releaseFailure;
            }
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure = append(failure, closeFailure);
            }
            if ((managerDeleted || deleteOwnerMarker) && approvedRoot.isCurrent()) {
                try {
                    BasicFileAttributes owner =
                            rootStream == null
                                    ? PaimonSpillPathCapability.readAbsolute(ownerAbsolutePath)
                                    : PaimonSpillPathCapability.readRelative(
                                            rootStream, ownerName);
                    if (owner.isRegularFile()
                            && !owner.isSymbolicLink()
                            && PaimonSpillPathCapability.sameStableIdentity(
                                    ownerIdentity,
                                    PaimonSpillPathCapability.stableIdentity(owner))) {
                        if (rootStream == null) {
                            Files.delete(ownerAbsolutePath);
                        } else {
                            rootStream.deleteFile(ownerName);
                        }
                    }
                } catch (IOException deleteFailure) {
                    failure = append(failure, deleteFailure);
                }
            }
            if (rootStream != null) {
                try {
                    rootStream.close();
                } catch (IOException closeFailure) {
                    failure = append(failure, closeFailure);
                }
            }
            if (failure != null) {
                throw new IllegalStateException(
                        "Failed to close an exact spill owner capability", failure);
            }
        }

        private static void closeAcquisition(
                FileLock lock,
                SeekableByteChannel channel,
                DirectoryStream<Path> stream) {
            IOException failure = null;
            if (lock != null) {
                try {
                    lock.release();
                } catch (IOException releaseFailure) {
                    failure = releaseFailure;
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure = append(failure, closeFailure);
                }
            }
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException closeFailure) {
                    failure = append(failure, closeFailure);
                }
            }
            if (failure != null) {
                throw new IllegalStateException(
                        "Failed to roll back a spill owner capability", failure);
            }
        }

        private static IOException append(IOException first, IOException next) {
            if (first == null) {
                return next;
            }
            first.addSuppressed(next);
            return first;
        }
    }
}
