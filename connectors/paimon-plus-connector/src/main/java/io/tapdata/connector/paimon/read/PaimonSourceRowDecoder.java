package io.tapdata.connector.paimon.read;

import org.apache.paimon.data.*;
import org.apache.paimon.types.*;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;

/**
 * 按本次 schema 预构建字段 getter/转换器，逐行读取时不再解析类型。
 * 原生 InternalRow/Array/Map 属于 reader 的内部表示；输出只保留独立集合、不可变值或二进制副本，
 * 避免 TapData 异步消费时引用已被 releaseBatch 回收或复用的内存。
 * 类型读取遵循 Paimon；JSON 表示、Map key 限制和 TapData 时间载体是本连接器的适配约定。
 */
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
            // 使用内核按类型分派的 getter（包括 LTZ），不通过显示字符串猜类型。
            // copy(true) 只让 getter 检查 null，保留 delete/before image 中的空值；不修改表 DDL。
            // InternalRow#createFieldGetter / InternalArray#createElementGetter，固定 release-1.3：
            // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-common/src/main/java/org/apache/paimon/data/InternalRow.java#L168
            // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-common/src/main/java/org/apache/paimon/data/InternalArray.java#L76
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
        // 来源：io.tapdata:tapdata-api:2.0.8-SNAPSHOT sources.jar，
        // io/tapdata/entity/codec/filter/impl/AllLayerMapIterator.java#iterate(Map, EntryFilter):86。
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
                // 内核允许 releaseBatch 后复用批内对象；顶层必须复制 byte[]，嵌套值转换成独立文本。
                // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-common/src/main/java/org/apache/paimon/reader/RecordReader.java#L59
                return nested ? value -> Base64.getEncoder().encodeToString((byte[]) value)
                        : value -> ((byte[]) value).clone();
            case DATE:
                return nested ? value -> LocalDate.ofEpochDay((Integer) value).toString()
                        : value -> new java.sql.Date((Integer) value * 86_400_000L);
            case TIME_WITHOUT_TIME_ZONE:
                // InternalRow getter 的 TIME 路径读取 int，按一天内毫秒数转换。
                // 本连接器 spec.json 声明 TIME 精度 0..3；不是 Paimon TimeType 的声明上限：
                // 原生 TimeType 允许 0..9，本路径尚未验证 TIME(4..9) 的端到端精度。
                // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-api/src/main/java/org/apache/paimon/types/TimeType.java#L43
                return nested ? value -> LocalTime.ofNanoOfDay((Integer) value * 1_000_000L).toString()
                        : value -> LocalTime.ofNanoOfDay((Integer) value * 1_000_000L).atDate(LocalDate.ofEpochDay(0));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // 保留墙钟字段和纳秒，不在此套用 JVM/目标时区；PDK 载体由 PaimonSourceCodecs 处理。
                // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-common/src/main/java/org/apache/paimon/data/Timestamp.java#L92
                return nested ? value -> ((Timestamp) value).toLocalDateTime().toString()
                        : value -> ((Timestamp) value).toLocalDateTime();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // LTZ 表示 instant，输出携带 UTC；不能套用无时区 TIMESTAMP 的墙钟约定。
                // 固定版本 InternalRow 的 LTZ getter 仍返回 Timestamp；其 toInstant 已标 Deprecated，
                // 升级时需连同 LocalZoneTimestamp/getter 契约核对，不能仅替换返回值强转。
                // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-common/src/main/java/org/apache/paimon/data/Timestamp.java#L111
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
