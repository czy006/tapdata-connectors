package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.IOManager;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonSpillDirCleanerTest {

    private static final PaimonSpillDirCleaner.SecureRootOpener TEST_SECURE_ROOT =
            approvedRoot -> new TestSecureDirectoryStream(approvedRoot.realPath());

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void freshUnlockedDirectoryMustRetainOwnerMarkerForLaterCleanup() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve(managerName(1))).toFile();
        File data = Files.write(spillDir.toPath().resolve("fresh.sst"), new byte[] {1})
                .toFile();
        File ownerFile = ownerFile(spillDir);
        assertTrue(ownerFile.createNewFile());

        assertEquals(
                0,
                cleanupSecure(
                        new String[] {tempDir.toString()}, 60_000L, null));
        assertTrue(spillDir.exists());
        assertTrue(ownerFile.exists());

        long old = System.currentTimeMillis() - 120_000L;
        assertTrue(data.setLastModified(old));
        assertTrue(spillDir.setLastModified(old));
        assertEquals(
                1,
                cleanupSecure(
                        new String[] {tempDir.toString()}, 60_000L, null));
        assertFalse(spillDir.exists());
        assertFalse(ownerFile.exists());
    }

    @Test
    void nestedSymbolicLinkMustNotDeleteExternalTarget() throws Exception {
        Path outsideDir = Files.createDirectory(tempDir.resolve("outside"));
        Path outsideData = Files.write(outsideDir.resolve("keep.sst"), new byte[] {1, 2, 3});
        File spillDir = Files.createDirectory(tempDir.resolve(managerName(2))).toFile();
        createSymbolicLinkOrSkip(spillDir.toPath().resolve("outside-link"), outsideDir);
        assertTrue(ownerFile(spillDir).createNewFile());

        assertEquals(
                0,
                cleanupSecure(
                        new String[] {tempDir.toString()}, 0L, null));

        assertTrue(spillDir.exists());
        assertTrue(ownerFile(spillDir).exists());
        assertTrue(Files.isDirectory(outsideDir));
        assertTrue(Files.exists(outsideData));
    }

    @Test
    void topLevelSymbolicLinkMustBeIgnored() throws Exception {
        Path outsideDir = Files.createDirectory(tempDir.resolve("top-level-outside"));
        Path outsideData = Files.write(outsideDir.resolve("keep.sst"), new byte[] {1});
        Path spillLink = tempDir.resolve(managerName(3));
        createSymbolicLinkOrSkip(spillLink, outsideDir);
        File ownerFile =
                new File(
                        tempDir.toFile(),
                        "." + managerName(3)
                                + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
        assertTrue(ownerFile.createNewFile());

        assertEquals(
                0,
                cleanupSecure(
                        new String[] {tempDir.toString()}, 0L, null));

        assertTrue(Files.isSymbolicLink(spillLink));
        assertTrue(Files.exists(outsideData));
        assertTrue(ownerFile.exists());
    }

    @Test
    void managerIdentitySwapAfterScanMustFailClosed() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve(managerName(4))).toFile();
        Path retainedFile = Files.write(spillDir.toPath().resolve("retain.sst"), new byte[] {1, 2});
        File ownerFile = ownerFile(spillDir);
        assertTrue(ownerFile.createNewFile());
        AtomicInteger callbackCount = new AtomicInteger();
        Path renamedOriginal = tempDir.resolve("retained-original-manager");

        int deleted =
                cleanupSecure(
                        new String[] {tempDir.toString()},
                        0L,
                        (managerId, bytes) -> callbackCount.incrementAndGet(),
                        (approvedRoot, manager) -> {
                            Files.move(approvedRoot.resolve(manager), renamedOriginal);
                            Files.createDirectory(approvedRoot.resolve(manager));
                            Files.write(
                                    approvedRoot.resolve(manager).resolve("replacement.sst"),
                                    new byte[] {9});
                        });

        assertEquals(0, deleted);
        assertEquals(0, callbackCount.get());
        assertTrue(Files.exists(renamedOriginal));
        assertTrue(Files.exists(renamedOriginal.resolve(retainedFile.getFileName())));
        assertTrue(Files.exists(spillDir.toPath().resolve("replacement.sst")));
        assertTrue(ownerFile.exists());
    }

    @Test
    void ownerIdentitySwapAfterScanMustFailClosed() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve(managerName(12))).toFile();
        Files.write(spillDir.toPath().resolve("retain.sst"), new byte[] {1, 2});
        Path owner = ownerFile(spillDir).toPath();
        assertTrue(owner.toFile().createNewFile());
        Path renamedOwner = tempDir.resolve("retained-original-owner-marker");
        AtomicInteger callbackCount = new AtomicInteger();

        int deleted =
                cleanupSecure(
                        new String[] {tempDir.toString()},
                        0L,
                        (managerId, bytes) -> callbackCount.incrementAndGet(),
                        (approvedRoot, manager) -> {
                            Files.move(owner, renamedOwner);
                            Files.createFile(owner);
                        });

        assertEquals(0, deleted);
        assertEquals(0, callbackCount.get());
        assertTrue(spillDir.exists());
        assertTrue(Files.exists(owner));
        assertTrue(Files.exists(renamedOwner));
    }

    @Test
    void partialDeletionFailureMustRetainOwnerMarkerAndAllowRetry() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve(managerName(13))).toFile();
        Path retainedFile =
                Files.write(spillDir.toPath().resolve("retain.sst"), new byte[] {1, 2});
        Files.write(spillDir.toPath().resolve("other.sst"), new byte[] {3});
        File ownerFile = ownerFile(spillDir);
        assertTrue(ownerFile.createNewFile());
        AtomicBoolean injectFailure = new AtomicBoolean(true);
        PaimonSpillDirCleaner.SecureRootOpener failingDelete =
                approvedRoot ->
                        new TestSecureDirectoryStream(
                                approvedRoot.realPath(),
                                path ->
                                        path.getFileName().toString().equals("retain.sst")
                                                && injectFailure.getAndSet(false));

        assertEquals(
                0,
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()},
                        0L,
                        null,
                        null,
                        (approvedRoot, managerName) -> {},
                        failingDelete));
        assertTrue(spillDir.exists());
        assertTrue(Files.exists(retainedFile));
        assertTrue(ownerFile.exists());

        assertEquals(
                1,
                cleanupSecure(new String[] {tempDir.toString()}, 0L, null));
        assertFalse(spillDir.exists());
        assertFalse(ownerFile.exists());
    }

    @Test
    void successfulDeletionMustReportOnlyRegularFileBytes() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve(managerName(5))).toFile();
        Files.write(spillDir.toPath().resolve("one.sst"), new byte[] {1, 2, 3});
        Files.write(spillDir.toPath().resolve("two.sst"), new byte[] {4, 5, 6, 7});
        assertTrue(ownerFile(spillDir).createNewFile());
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicLong deletedBytes = new AtomicLong(-1L);
        AtomicReference<String> deletedManagerId = new AtomicReference<>();

        assertEquals(
                1,
                cleanupSecure(
                        new String[] {tempDir.toString()},
                        0L,
                        (managerId, bytes) -> {
                            callbackCount.incrementAndGet();
                            deletedManagerId.set(managerId);
                            deletedBytes.set(bytes);
                        }));

        assertEquals(1, callbackCount.get());
        assertEquals(managerId(5), deletedManagerId.get());
        assertEquals(7L, deletedBytes.get());
    }

    @Test
    void deeplyNestedSecureTreeWithinLimitMustBeDeleted() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve(managerName(6))).toFile();
        Path nested = spillDir.toPath();
        for (int depth = 0; depth < 256; depth++) {
            nested = Files.createDirectory(nested.resolve("d"));
        }
        Files.write(nested.resolve("deep.sst"), new byte[] {1});
        assertTrue(ownerFile(spillDir).createNewFile());

        assertEquals(
                1,
                cleanupSecure(
                        new String[] {tempDir.toString()}, 0L, null));
        assertFalse(spillDir.exists());
    }

    @Test
    void cleanupMustNotDeleteDirectoryLockedByAnotherProcessOwner() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve(managerName(7))).toFile();
        File data = Files.write(spillDir.toPath().resolve("active.sst"), new byte[] {1, 2, 3})
                .toFile();
        long old = System.currentTimeMillis() - 60_000L;
        assertTrue(data.setLastModified(old));
        assertTrue(spillDir.setLastModified(old));

        File ownerFile = new File(
                tempDir.toFile(), "." + managerName(7) + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
        try (RandomAccessFile raf = new RandomAccessFile(ownerFile, "rw");
             FileLock ignored = raf.getChannel().lock()) {
            assertEquals(0, cleanupSecure(
                    new String[] {tempDir.toString()}, 0L, null));
            assertTrue(spillDir.exists());
        }

        assertEquals(1, cleanupSecure(
                new String[] {tempDir.toString()}, 0L, null));
        assertFalse(spillDir.exists());
        assertFalse(ownerFile.exists());
    }

    @Test
    void registeredIoManagerDirectoryMustRemainProtectedUntilUnregistered() throws Exception {
        IOManager ioManager = IOManager.create(new String[] {tempDir.toString()});
        List<String> spillDirs = PaimonSpillDirCleaner.registerLiveDirs(ioManager);
        try {
            assertFalse(spillDirs.isEmpty());
            assertEquals(0, cleanupSecure(
                    new String[] {tempDir.toString()}, 0L, null));
            for (String path : spillDirs) {
                assertTrue(new File(path).exists());
            }
        } finally {
            ioManager.close();
            PaimonSpillDirCleaner.unregisterLiveDirs(spillDirs);
        }

        for (String path : spillDirs) {
            File spillDir = new File(path);
            File ownerFile = new File(
                    spillDir.getParentFile(),
                    "." + spillDir.getName() + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
            assertFalse(ownerFile.exists());
        }
    }

    @Test
    void locklessLegacyDirectoryMustNotBeDeletedDuringRollingUpgrade() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve(managerName(8))).toFile();
        File data = Files.write(spillDir.toPath().resolve("possibly-active.sst"), new byte[] {1})
                .toFile();
        long old = System.currentTimeMillis() - 60_000L;
        assertTrue(data.setLastModified(old));
        assertTrue(spillDir.setLastModified(old));

        assertEquals(0, cleanupSecure(
                new String[] {tempDir.toString()}, 0L, null));
        assertTrue(spillDir.exists());
    }

    @Test
    void resolveTmpDirsMustFallBackToJavaIoTmpdirThenWorkingDir() {
        String workingDir = new File(".").getAbsolutePath();
        String ioTmpdir = System.getProperty("java.io.tmpdir", workingDir);

        // null / blank configured values fall back to java.io.tmpdir (then working dir).
        assertEquals(ioTmpdir, PaimonSpillDirCleaner.resolveTmpDirs(null));
        assertEquals(ioTmpdir, PaimonSpillDirCleaner.resolveTmpDirs(""));
        assertEquals(ioTmpdir, PaimonSpillDirCleaner.resolveTmpDirs("   "));

        // Non-blank configured values are returned verbatim (multi-path preserved).
        assertEquals("/custom", PaimonSpillDirCleaner.resolveTmpDirs("/custom"));
        assertEquals("/a,/b,/c", PaimonSpillDirCleaner.resolveTmpDirs("/a,/b,/c"));
    }

    @Test
    void splitTmpDirRootsMustDelegateToPaimonSplitPaths() {
        // splitTmpDirRoots is a thin wrapper over Paimon's IOManagerImpl.splitPaths: it splits on
        // comma/path-separator without trimming or dropping empty segments (callers that need clean
        // roots sanitize themselves). Verify the wrapper preserves that contract.
        String[] roots = PaimonSpillDirCleaner.splitTmpDirRoots("/a,/b,/c");
        assertEquals(3, roots.length);
        assertEquals("/a", roots[0]);
        assertEquals("/b", roots[1]);
        assertEquals("/c", roots[2]);

        // Empty input yields an empty array (splitPaths short-circuits length == 0).
        assertEquals(0, PaimonSpillDirCleaner.splitTmpDirRoots("").length);
    }

    @Test
    void spillPathCapabilityMustRejectPrefixOnlyNamesAndMissingIdentity() {
        assertFalse(PaimonSpillPathCapability.isStrictManagerName("paimon-io-prefix-only"));
        assertTrue(
                PaimonSpillPathCapability.isStrictManagerName(
                        "paimon-io-123e4567-e89b-12d3-a456-426614174000"));
        assertFalse(
                PaimonSpillPathCapability.isStrictManagerName(
                        "paimon-io-123E4567-e89b-12d3-a456-426614174000"));
        assertNull(PaimonSpillPathCapability.stableIdentity(null));
        assertFalse(PaimonSpillPathCapability.sameStableIdentity(null, new Object()));
        assertFalse(PaimonSpillPathCapability.sameStableIdentity(new Object(), null));
    }

    @Test
    void approvedRootMustRejectBlankRootWorkspaceAndWarehouse() throws Exception {
        assertNull(PaimonSpillPathCapability.approveRoot(" ", null, null));
        assertNull(
                PaimonSpillPathCapability.approveRoot(
                        tempDir.getRoot().toString(), null, null));
        assertNull(
                PaimonSpillPathCapability.approveRoot(
                        tempDir.toString(), tempDir, null));
        assertNull(
                PaimonSpillPathCapability.approveRoot(
                        tempDir.toString(), null, tempDir.toString()));

        Path approved = Files.createDirectory(tempDir.resolve("approved-spill-root"));
        assertNotNull(
                PaimonSpillPathCapability.approveRoot(
                        approved.toString(), tempDir, null));
    }

    @Test
    void prefixOnlyAndUppercaseUuidManagersMustNotBeDeleted() throws Exception {
        Path prefixOnly = Files.createDirectory(tempDir.resolve("paimon-io-prefix-only"));
        Path uppercase =
                Files.createDirectory(
                        tempDir.resolve("paimon-io-123E4567-e89b-12d3-a456-426614174000"));
        assertTrue(ownerFile(prefixOnly.toFile()).createNewFile());
        assertTrue(ownerFile(uppercase.toFile()).createNewFile());

        assertEquals(
                0,
                cleanupSecure(
                        new String[] {tempDir.toString()}, 0L, null));
        assertTrue(Files.exists(prefixOnly));
        assertTrue(Files.exists(uppercase));
    }

    @Test
    void symbolicOwnerMarkerMustNotGrantDeletionAuthority() throws Exception {
        Path manager = Files.createDirectory(tempDir.resolve(managerName(9)));
        Files.write(manager.resolve("keep.sst"), new byte[] {1});
        Path externalMarker = Files.write(tempDir.resolve("external-marker"), new byte[] {2});
        createSymbolicLinkOrSkip(ownerFile(manager.toFile()).toPath(), externalMarker);

        assertEquals(
                0,
                cleanupSecure(
                        new String[] {tempDir.toString()}, 0L, null));
        assertTrue(Files.exists(manager));
        assertTrue(Files.exists(externalMarker));
    }

    @Test
    void nonSecureDirectoryStreamMustNotBeAcceptedAsDeletionCapability() throws Exception {
        try (DirectoryStream<Path> delegate = Files.newDirectoryStream(tempDir);
                DirectoryStream<Path> ordinary =
                        new DirectoryStream<Path>() {
                            @Override
                            public Iterator<Path> iterator() {
                                return delegate.iterator();
                            }

                            @Override
                            public void close() throws IOException {
                                delegate.close();
                            }
                        }) {
            assertNull(PaimonSpillPathCapability.requireSecure(ordinary));
        }
    }

    @Test
    void deletionCallbackMustExposeOnlyManagerId() throws Exception {
        Path credentialNamedRoot =
                Files.createDirectory(tempDir.resolve("user-secret@host\nspill-root"));
        Path manager = Files.createDirectory(credentialNamedRoot.resolve(managerName(10)));
        Files.write(manager.resolve("data.sst"), new byte[] {1});
        assertTrue(ownerFile(manager.toFile()).createNewFile());
        AtomicReference<String> callbackValue = new AtomicReference<>();

        assertEquals(
                1,
                cleanupSecure(
                        new String[] {credentialNamedRoot.toString()},
                        0L,
                        (managerId, bytes) -> callbackValue.set(managerId)));

        assertEquals(managerId(10), callbackValue.get());
        assertFalse(callbackValue.get().contains("secret"));
        assertFalse(callbackValue.get().contains("\n"));
    }

    @Test
    void unavailableSecureDeletionCapabilityMustFailClosed() throws Exception {
        Path manager = Files.createDirectory(tempDir.resolve(managerName(11)));
        Files.write(manager.resolve("keep.sst"), new byte[] {1});
        assertTrue(ownerFile(manager.toFile()).createNewFile());

        int deleted =
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()},
                        0L,
                        null,
                        null,
                        (approvedRoot, managerName) -> {},
                        approvedRoot -> null);

        assertEquals(0, deleted);
        assertTrue(Files.exists(manager));
        assertTrue(ownerFile(manager.toFile()).exists());
    }

    private static int cleanupSecure(
            String[] roots,
            long graceMs,
            java.util.function.BiConsumer<String, Long> onDeleted) {
        return PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                roots,
                graceMs,
                onDeleted,
                null,
                (approvedRoot, managerName) -> {},
                TEST_SECURE_ROOT);
    }

    private static int cleanupSecure(
            String[] roots,
            long graceMs,
            java.util.function.BiConsumer<String, Long> onDeleted,
            PaimonSpillDirCleaner.BeforeDeleteHook beforeDelete) {
        return PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                roots, graceMs, onDeleted, null, beforeDelete, TEST_SECURE_ROOT);
    }

    private static File ownerFile(File spillDir) {
        return new File(
                spillDir.getParentFile(),
                "." + spillDir.getName() + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
    }

    private static String managerName(int value) {
        return PaimonSpillDirCleaner.SPILL_DIR_PREFIX + managerId(value);
    }

    private static String managerId(int value) {
        return String.format("00000000-0000-4000-8000-%012d", value);
    }

    private static void createSymbolicLinkOrSkip(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException | SecurityException failure) {
            Assumptions.assumeTrue(false, "Symbolic links are unavailable: " + failure);
        }
    }

    /** Deterministic descriptor-relative test provider for hosts without native SDS support. */
    private static final class TestSecureDirectoryStream
            implements SecureDirectoryStream<Path> {
        private final Path directory;
        private final DirectoryStream<Path> entries;
        private final java.util.function.Predicate<Path> failDelete;

        private TestSecureDirectoryStream(Path directory) throws IOException {
            this(directory, path -> false);
        }

        private TestSecureDirectoryStream(
                Path directory,
                java.util.function.Predicate<Path> failDelete)
                throws IOException {
            this.directory = directory;
            this.entries = Files.newDirectoryStream(directory);
            this.failDelete = failDelete;
        }

        @Override
        public SecureDirectoryStream<Path> newDirectoryStream(
                Path path, LinkOption... options) throws IOException {
            Path child = resolveDirectChild(path);
            BasicFileAttributes attributes =
                    Files.readAttributes(
                            child,
                            BasicFileAttributes.class,
                            LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isDirectory() || attributes.isSymbolicLink()) {
                throw new IOException("Test spill child is not a direct directory");
            }
            return new TestSecureDirectoryStream(child, failDelete);
        }

        @Override
        public SeekableByteChannel newByteChannel(
                Path path,
                Set<? extends OpenOption> options,
                FileAttribute<?>... attrs)
                throws IOException {
            return Files.newByteChannel(resolveDirectChild(path), options, attrs);
        }

        @Override
        public void deleteFile(Path path) throws IOException {
            Path child = resolveDirectChild(path);
            if (failDelete.test(child)) {
                throw new IOException("Injected descriptor-relative delete failure");
            }
            Files.delete(child);
        }

        @Override
        public void deleteDirectory(Path path) throws IOException {
            Files.delete(resolveDirectChild(path));
        }

        @Override
        public void move(
                Path srcpath,
                SecureDirectoryStream<Path> targetdir,
                Path targetpath)
                throws IOException {
            if (!(targetdir instanceof TestSecureDirectoryStream)) {
                throw new IOException("Target test directory capability is incompatible");
            }
            TestSecureDirectoryStream target = (TestSecureDirectoryStream) targetdir;
            Files.move(
                    resolveDirectChild(srcpath),
                    target.resolveDirectChild(targetpath));
        }

        @Override
        public <V extends FileAttributeView> V getFileAttributeView(
                Class<V> type) {
            return Files.getFileAttributeView(directory, type);
        }

        @Override
        public <V extends FileAttributeView> V getFileAttributeView(
                Path path, Class<V> type, LinkOption... options) {
            try {
                return Files.getFileAttributeView(
                        resolveDirectChild(path), type, options);
            } catch (IOException rejected) {
                return null;
            }
        }

        @Override
        public Iterator<Path> iterator() {
            return entries.iterator();
        }

        @Override
        public void close() throws IOException {
            entries.close();
        }

        private Path resolveDirectChild(Path path) throws IOException {
            if (path == null
                    || path.getNameCount() != 1
                    || !path.equals(path.getFileName())) {
                throw new IOException("Test spill entry is not a direct relative child");
            }
            return directory.resolve(path);
        }
    }
}
