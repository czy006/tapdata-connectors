package io.tapdata.connector.paimon.read;

import org.apache.paimon.data.*;
import org.apache.paimon.types.*;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;

/** A schema-bound decoder; no reader-owned objects escape into asynchronous TapData events. */
public final class PaimonSourceRowDecoder {
    private final String[] names;
    private final boolean[] complex;
    private final int mapCapacity;
    private final InternalRow.FieldGetter[] getters;
    private final List<Function<Object, Object>> converters;

    public PaimonSourceRowDecoder(List<DataField> fields) {
        this(fields, false);
    }

    private PaimonSourceRowDecoder(List<DataField> fields, boolean nested) {
        mapCapacity = (int) Math.min(Integer.MAX_VALUE, (long) fields.size() * 4 / 3 + 1);
        names = new String[fields.size()];
        complex = new boolean[fields.size()];
        getters = new InternalRow.FieldGetter[fields.size()];
        converters = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            names[i] = field.name();
            DataTypeRoot root = field.type().getTypeRoot();
            complex[i] = root == DataTypeRoot.ARRAY || root == DataTypeRoot.MAP || root == DataTypeRoot.ROW;
            // Paimon release-1.3 @ 5c59e6cb01ed0b29563371f56e14fcade4597a2e:
            // paimon-common/src/main/java/org/apache/paimon/data/InternalRow.java#createFieldGetter and
            // InternalArray.java#createElementGetter dispatch using native types (including LTZ).
            // Nullable getters also preserve nulls in delete/before images, independent of DDL.
            getters[i] = InternalRow.createFieldGetter(field.type().copy(true), i);
            converters.add(converter(field.type(), nested));
        }
    }

    public Map<String, Object> decode(InternalRow row) {
        Map<String, Object> result = new LinkedHashMap<>(mapCapacity);
        for (int i = 0; i < names.length; i++) {
            result.put(names[i], convertNullable(converters.get(i), getters[i].getFieldOrNull(row)));
        }
        return result;
    }

    public Map<String, Object> decodeForEvent(InternalRow row) {
        Map<String, Object> result = decode(row);
        // PDK 2.0.8-SNAPSHOT AllLayerMapIterator#iterate recursively wraps child Maps before
        // source codecs run; MySQL's target codec then JSON-encodes those children twice.
        // A standard String carrier crosses that traversal once. Our registered String codec
        // restores the declared TapMap/TapArray at the top level (also for schema-enforced batch).
        for (int i = 0; i < names.length; i++) {
            if (complex[i] && result.get(names[i]) != null) {
                result.put(names[i], io.tapdata.entity.simplify.TapSimplify.toJson(result.get(names[i])));
            }
        }
        return result;
    }

    private static Object convertNullable(Function<Object, Object> converter, Object value) {
        return value == null ? null : converter.apply(value);
    }

    private static Function<Object, Object> converter(DataType type, boolean nested) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return Function.identity();
            case FLOAT:
            case DOUBLE:
                return value -> {
                    // JSON has no NaN/Infinity numbers; serializers may silently write null.
                    if (nested && !Double.isFinite(((Number) value).doubleValue())) {
                        throw new IllegalArgumentException("Non-finite Paimon number cannot be represented in JSON");
                    }
                    return value;
                };
            case CHAR:
            case VARCHAR:
                return Object::toString;
            case DECIMAL:
                return value -> ((Decimal) value).toBigDecimal();
            case BINARY:
            case VARBINARY:
                // GenericRow can expose its backing byte[]; events outlive reader.releaseBatch().
                // Nested values follow a JSON contract: binary is standard Base64 text.
                return nested ? value -> Base64.getEncoder().encodeToString((byte[]) value)
                        : value -> ((byte[]) value).clone();
            case DATE:
                return nested ? value -> LocalDate.ofEpochDay((Integer) value).toString()
                        : value -> new java.sql.Date((Integer) value * 86_400_000L);
            case TIME_WITHOUT_TIME_ZONE:
                // Paimon 1.3 TimeType supports precision 0..3 and stores milliseconds of day.
                return nested ? value -> LocalTime.ofNanoOfDay((Integer) value * 1_000_000L).toString()
                        : value -> LocalTime.ofNanoOfDay((Integer) value * 1_000_000L).atDate(LocalDate.ofEpochDay(0));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return nested ? value -> ((Timestamp) value).toLocalDateTime().toString()
                        : value -> ((Timestamp) value).toLocalDateTime();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return nested ? value -> ((Timestamp) value).toInstant().toString()
                        : value -> ((Timestamp) value).toInstant().atZone(ZoneOffset.UTC);
            case ARRAY: {
                DataType element = ((ArrayType) type).getElementType();
                InternalArray.ElementGetter getter = InternalArray.createElementGetter(element.copy(true));
                Function<Object, Object> convert = converter(element, true);
                return value -> {
                    InternalArray array = (InternalArray) value;
                    List<Object> result = new ArrayList<>(array.size());
                    for (int i = 0; i < array.size(); i++) {
                        result.add(convertNullable(convert, getter.getElementOrNull(array, i)));
                    }
                    return result;
                };
            }
            case MAP: {
                MapType mapType = (MapType) type;
                DataTypeRoot keyRoot = mapType.getKeyType().getTypeRoot();
                // MySQL's existing TapMap codec writes a JSON object. Do not coerce keys and
                // collapse distinct keys (or null) into identical JSON member names.
                if (keyRoot != DataTypeRoot.CHAR && keyRoot != DataTypeRoot.VARCHAR) {
                    throw new IllegalArgumentException("Paimon source JSON maps require string keys: " + type.asSQLString());
                }
                InternalArray.ElementGetter getter = InternalArray.createElementGetter(mapType.getValueType().copy(true));
                Function<Object, Object> convert = converter(mapType.getValueType(), true);
                return value -> {
                    InternalMap map = (InternalMap) value;
                    InternalArray keys = map.keyArray();
                    InternalArray values = map.valueArray();
                    Map<String, Object> result = new LinkedHashMap<>();
                    for (int i = 0; i < map.size(); i++) {
                        if (keys.isNullAt(i)) {
                            throw new IllegalArgumentException("Paimon source JSON maps do not support null keys");
                        }
                        result.put(keys.getString(i).toString(), convertNullable(convert, getter.getElementOrNull(values, i)));
                    }
                    return result;
                };
            }
            case ROW: {
                PaimonSourceRowDecoder decoder = new PaimonSourceRowDecoder(((RowType) type).getFields(), true);
                return value -> decoder.decode((InternalRow) value);
            }
            default:
                throw new IllegalArgumentException("Unsupported Paimon source type: " + type.asSQLString());
        }
    }
}
