package io.tapdata.connector.paimon.read;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * An immutable connector recovery envelope, not the native Paimon checkpoint serialization format.
 * Adds readEpoch, physical table identity and a fixed protection start to the native per-table next snapshot.
 * Only standard Map/String/Number values from toMap() cross the PDK boundary, avoiding private classloader dependencies.
 */
public final class PaimonReadOffset {
    public static final String KIND = "tapdata-paimon-stream";
    private final String readEpoch;
    private final Map<String, Position> tables;

    public static final class Position {
        public final String physicalTableHash;
        public final long startSnapshot;
        public final long nextSnapshot;

        public Position(String hash, long start, long next) {
            if (hash == null || !hash.matches("[0-9a-f]{64}") || start <= 0 || next < start) {
                throw invalid("Invalid table identity or start/next snapshot");
            }
            physicalTableHash = hash;
            startSnapshot = start;
            nextSnapshot = next;
        }
    }

    public PaimonReadOffset(String readEpoch, Map<String, Position> tables) {
        if (readEpoch == null || !UUID.fromString(readEpoch).toString().equals(readEpoch)) {
            throw invalid("readEpoch must be a canonical UUID");
        }
        if (tables == null || tables.isEmpty()) { throw invalid("No tables in offset"); }
        tables.forEach((name, position) -> {
            if (name == null || name.trim().isEmpty() || position == null) {
                throw invalid("Invalid table entry");
            }
        });
        this.readEpoch = readEpoch;
        this.tables = Collections.unmodifiableMap(new LinkedHashMap<>(tables));
    }

    public String readEpoch() { return readEpoch; }
    public Map<String, Position> tables() { return tables; }
    public Position position(String name) {
        Position position = tables.get(name);
        if (position == null) { throw invalid("Missing requested table: " + name); }
        return position;
    }

    // Apache Paimon release-1.3 @ 5c59e6cb01ed0b29563371f56e14fcade4597a2e
    // paimon-core/src/main/java/org/apache/paimon/table/source/StreamTableScan.java,
    // checkpoint()/restore(), lines 26-50: both use NEXT snapshot, never last consumed + 1.
    // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/StreamTableScan.java#L26
    public PaimonReadOffset withNext(String table, long next) {
        Position previous = position(table);
        if (next < previous.nextSnapshot) { throw invalid("Regressing candidate offset: " + table); }
        Map<String, Position> copy = new LinkedHashMap<>(tables);
        copy.put(table, new Position(previous.physicalTableHash, previous.startSnapshot, next));
        return new PaimonReadOffset(readEpoch, copy);
    }

    public Map<String, Object> toMap() {
        // Create independent nested maps for publication; encode snapshot ids as strings to avoid intermediate JSON precision loss.
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("kind", KIND);
        envelope.put("version", 1);
        envelope.put("readEpoch", readEpoch);
        Map<String, Object> entries = new LinkedHashMap<>();
        tables.forEach((name, position) -> {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("physicalTableHash", position.physicalTableHash);
            fields.put("startSnapshot", Long.toString(position.startSnapshot));
            fields.put("nextSnapshot", Long.toString(position.nextSnapshot));
            entries.put(name, fields);
        });
        envelope.put("tables", entries);
        return envelope;
    }

    public static PaimonReadOffset parse(Object value) {
        // Legacy maps do not distinguish consumed ids from next ids; reject them rather than guessing or adding one.
        Map<?, ?> envelope = map(value);
        keys(envelope, "kind", "version", "readEpoch", "tables");
        if (!KIND.equals(envelope.get("kind")) || !(envelope.get("version") instanceof Number)) {
            throw invalid("Unsupported stream offset kind/version");
        }
        try {
            if (new BigDecimal(envelope.get("version").toString()).compareTo(BigDecimal.ONE) != 0) {
                throw invalid("Unsupported stream offset version");
            }
        } catch (NumberFormatException e) { throw invalid("Invalid stream offset version"); }
        Map<String, Position> tables = new LinkedHashMap<>();
        map(envelope.get("tables")).forEach((name, entry) -> {
            Map<?, ?> fields = map(entry);
            keys(fields, "physicalTableHash", "startSnapshot", "nextSnapshot");
            tables.put(string(name), new Position(string(fields.get("physicalTableHash")),
                    snapshot(fields.get("startSnapshot")), snapshot(fields.get("nextSnapshot"))));
        });
        return new PaimonReadOffset(string(envelope.get("readEpoch")), tables);
    }

    public static String physicalTableHash(URI location, String branch) {
        // Binds normalized location/branch, not a creation generation; same-path recreation with colliding ids is not detected.
        try {
            // Connector policy: bind location/branch without leaking URI credentials into offsets.
            String authority = location.getAuthority();
            if (authority != null) {
                authority = authority.substring(authority.lastIndexOf('@') + 1).toLowerCase(java.util.Locale.ROOT);
            }
            URI clean = new URI(location.getScheme() == null ? null : location.getScheme().toLowerCase(java.util.Locale.ROOT),
                    authority, location.getPath(), null, null).normalize();
            String normalized = clean.toString();
            while (normalized.endsWith("/") && normalized.length() > 1) {
                normalized = normalized.substring(0, normalized.length() - 1);
            }
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    (normalized + "\n" + branch).getBytes(StandardCharsets.UTF_8));
            StringBuilder hash = new StringBuilder();
            for (byte b : digest) { hash.append(String.format(java.util.Locale.ROOT, "%02x", b & 255)); }
            return hash.toString();
        } catch (Exception e) { throw new IllegalArgumentException("Cannot bind Paimon table location", e); }
    }

    private static Map<?, ?> map(Object value) {
        if (!(value instanceof Map)) { throw invalid("A versioned stream offset Map is required"); }
        return (Map<?, ?>) value;
    }
    private static String string(Object value) {
        if (!(value instanceof String)) { throw invalid("Expected a string field"); }
        return (String) value;
    }
    private static long snapshot(Object value) {
        String text = string(value);
        if (!text.matches("[1-9][0-9]*")) { throw invalid("Snapshot must be a positive decimal string"); }
        try { return Long.parseLong(text); }
        catch (NumberFormatException e) { throw invalid("Snapshot overflows a signed long"); }
    }
    private static void keys(Map<?, ?> value, String... names) {
        Set<String> expected = new HashSet<>(Arrays.asList(names));
        if (!value.keySet().equals(expected)) { throw invalid("Unknown or missing offset fields"); }
    }
    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message + "; initialize a new Paimon stream position through TapData. Legacy offsets are ambiguous and are not restored.");
    }
}
