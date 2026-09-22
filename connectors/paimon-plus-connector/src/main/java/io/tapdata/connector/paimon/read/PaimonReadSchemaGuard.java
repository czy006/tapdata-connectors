package io.tapdata.connector.paimon.read;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** Rejects runtime structural changes; historical files remain the native reader's responsibility. */
public final class PaimonReadSchemaGuard {
    private final FileStoreTable table;
    private final TableSchema initial;
    private final List<Object> binding;
    private final PaimonReadPositionResolver resolver;

    public PaimonReadSchemaGuard(FileStoreTable table, TapTable tapTable, PaimonReadPositionResolver resolver) {
        this.table = table; this.initial = table.schema(); this.binding = fingerprint(initial); this.resolver = resolver;
        if (tapTable == null || tapTable.getNameFieldMap() == null
                || !tapTable.getNameFieldMap().keySet().equals(initial.nameToFieldMap().keySet())) {
            throw mismatch();
        }
        for (DataField field : initial.fields()) {
            TapField tap = tapTable.getNameFieldMap().get(field.name());
            // Matches this connector's discoverSchema() SQL-type representation, including complex types.
            if (tap == null || !normalize(field.type().asSQLString()).equals(normalize(tap.getDataType()))) {
                throw mismatch();
            }
        }
    }

    // ReadBuilderImpl binds its Table/readType at construction (65-82); it does not follow ALTER.
    // Apache Paimon release-1.3 @ 5c59e6cb01ed0b29563371f56e14fcade4597a2e,
    // paimon-core/src/main/java/org/apache/paimon/table/source/ReadBuilderImpl.java#readType.
    // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/ReadBuilderImpl.java#L65
    public void beforePlan() {
        check(table.schemaManager().latest().orElseThrow(PaimonReadSchemaGuard::mismatch));
    }

    // SnapshotReader.Plan.snapshotId() (130-141) is an internal, version-pinned seam.
    // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/SnapshotReader.java#L130
    public void beforeRead(TableScan.Plan plan) throws InterruptedException {
        if (plan.splits().isEmpty()) { return; }
        if (!(plan instanceof SnapshotReader.Plan) || ((SnapshotReader.Plan) plan).snapshotId() == null) {
            throw new IllegalStateException("Cannot verify schema of Paimon stream plan");
        }
        long schemaId = resolver.requiredMetadata(((SnapshotReader.Plan) plan).snapshotId()).schemaId();
        if (schemaId > initial.id()) { check(table.schemaManager().schema(schemaId)); }
    }

    private void check(TableSchema schema) {
        if (!binding.equals(fingerprint(schema))) { throw mismatch(); }
    }
    private static IllegalStateException mismatch() {
        return new IllegalStateException("Paimon stream schema or read semantics changed; reload TapData table metadata and restart");
    }
    private static String normalize(String type) {
        return type == null ? "" : type.replace("NOT NULL", "").replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
    }
    private static List<Object> fingerprint(TableSchema schema) {
        Map<String, String> readOptions = new TreeMap<>();
        schema.options().forEach((key, value) -> {
            if (key.startsWith("scan.") || key.startsWith("streaming-read") || key.startsWith("sequence.")
                    || key.startsWith("fields.") || key.equals("merge-engine") || key.equals("changelog-producer")
                    || key.equals("changelog.lifecycle-decoupled") || key.equals("branch")
                    || key.equals("bucket") || key.equals("bucket-key")) { readOptions.put(key, value); }
        });
        return Arrays.asList(fields(schema.fields()), schema.primaryKeys(), schema.partitionKeys(), readOptions);
    }
    private static List<Object> fields(List<DataField> fields) {
        List<Object> result = new ArrayList<>();
        for (DataField field : fields) {
            result.add(Arrays.asList(field.id(), field.name(), type(field.type()), field.defaultValue()));
        }
        return result;
    }
    private static Object type(DataType type) {
        if (type instanceof RowType) { return Arrays.asList("ROW", type.isNullable(), fields(((RowType) type).getFields())); }
        if (type instanceof ArrayType) { return Arrays.asList("ARRAY", type.isNullable(), type(((ArrayType) type).getElementType())); }
        if (type instanceof MapType) { return Arrays.asList("MAP", type.isNullable(), type(((MapType) type).getKeyType()), type(((MapType) type).getValueType())); }
        if (type instanceof MultisetType) { return Arrays.asList("MULTISET", type.isNullable(), type(((MultisetType) type).getElementType())); }
        return type.asSQLString();
    }
}
