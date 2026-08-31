package io.tapdata.connector.paimon.util;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.regex.Pattern;

/** Stable local-path capability required before spill ownership or destructive cleanup. */
final class PaimonSpillPathCapability {

    private static final Pattern MANAGER_NAME =
            Pattern.compile(
                    "^paimon-io-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

    private PaimonSpillPathCapability() {}

    static boolean isStrictManagerName(String name) {
        return name != null && MANAGER_NAME.matcher(name).matches();
    }

    static Object stableIdentity(BasicFileAttributes attributes) {
        return attributes == null ? null : attributes.fileKey();
    }

    static boolean sameStableIdentity(Object expected, Object current) {
        return expected != null && current != null && expected.equals(current);
    }

    static ApprovedRoot approveRoot(
            String configuredRoot, Path workspaceRoot, String warehouseRoot) {
        if (configuredRoot == null || configuredRoot.trim().isEmpty()) {
            return null;
        }
        try {
            Path configured = Paths.get(configuredRoot.trim());
            Path realRoot = configured.toRealPath(LinkOption.NOFOLLOW_LINKS);
            BasicFileAttributes attributes = readAbsolute(realRoot);
            Object identity = stableIdentity(attributes);
            if (!attributes.isDirectory()
                    || attributes.isSymbolicLink()
                    || identity == null
                    || realRoot.getParent() == null
                    || realRoot.equals(realRoot.getRoot())) {
                return null;
            }
            Path workspace = realLocalPath(workspaceRoot);
            if (workspace != null && realRoot.equals(workspace)) {
                return null;
            }
            Path warehouse = realLocalPath(warehouseRoot);
            if (warehouse != null && realRoot.equals(warehouse)) {
                return null;
            }
            return new ApprovedRoot(realRoot, identity);
        } catch (IOException | RuntimeException rejected) {
            return null;
        }
    }

    static BasicFileAttributes readRelative(
            SecureDirectoryStream<Path> parent, Path relativeName) throws IOException {
        Objects.requireNonNull(parent, "parent");
        Objects.requireNonNull(relativeName, "relativeName");
        if (relativeName.getNameCount() != 1 || !relativeName.equals(relativeName.getFileName())) {
            throw new IOException("Spill entry is not a direct relative child");
        }
        BasicFileAttributeView view =
                parent.getFileAttributeView(
                        relativeName,
                        BasicFileAttributeView.class,
                        LinkOption.NOFOLLOW_LINKS);
        if (view == null) {
            throw new IOException("Basic file attributes are unavailable");
        }
        return view.readAttributes();
    }

    static SecureDirectoryStream<Path> requireSecure(DirectoryStream<Path> stream) {
        if (!(stream instanceof SecureDirectoryStream)) {
            return null;
        }
        return (SecureDirectoryStream<Path>) stream;
    }

    static BasicFileAttributes readAbsolute(Path path) throws IOException {
        return Files.readAttributes(
                path,
                BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS);
    }

    private static Path realLocalPath(Path path) {
        if (path == null) {
            return null;
        }
        try {
            return path.toRealPath(LinkOption.NOFOLLOW_LINKS);
        } catch (IOException | RuntimeException ignored) {
            return null;
        }
    }

    private static Path realLocalPath(String configured) {
        if (configured == null || configured.trim().isEmpty()) {
            return null;
        }
        try {
            String candidate = configured.trim();
            if (candidate.regionMatches(true, 0, "file:", 0, "file:".length())) {
                return realLocalPath(Paths.get(java.net.URI.create(candidate)));
            }
            if (candidate.contains("://")) {
                return null;
            }
            return realLocalPath(Paths.get(candidate));
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    static final class ApprovedRoot {
        private final Path realPath;
        private final Object identity;

        private ApprovedRoot(Path realPath, Object identity) {
            this.realPath = realPath;
            this.identity = identity;
        }

        Path realPath() {
            return realPath;
        }

        boolean isCurrent() {
            try {
                BasicFileAttributes current = readAbsolute(realPath);
                return current.isDirectory()
                        && !current.isSymbolicLink()
                        && sameStableIdentity(identity, stableIdentity(current));
            } catch (IOException | RuntimeException changed) {
                return false;
            }
        }

        SecureDirectoryStream<Path> openSecure() throws IOException {
            DirectoryStream<Path> opened = Files.newDirectoryStream(realPath);
            SecureDirectoryStream<Path> secure = requireSecure(opened);
            if (secure == null) {
                opened.close();
            }
            return secure;
        }
    }
}
