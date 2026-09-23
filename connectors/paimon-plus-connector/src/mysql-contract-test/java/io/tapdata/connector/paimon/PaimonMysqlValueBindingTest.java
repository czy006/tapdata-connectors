package io.tapdata.connector.paimon;

import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.dml.MysqlWriteRecorder;
import io.tapdata.connector.paimon.read.PaimonSourceRowDecoder;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Opt-in contract from Paimon 1.3.2 native values through MySQL's real JDBC recorder. */
class PaimonMysqlValueBindingTest {

    @Test
    void decodedValuesMustReachMysqlPreparedStatementWithoutScalarLossAndWithJsonComplexValues()
            throws Exception {
        verifyBinding(false);
    }

    @Test
    void schemaEnforcedBatchMustPreserveTheSameJdbcValues() throws Exception {
        verifyBinding(true);
    }

    private void verifyBinding(boolean schemaEnforced) throws Exception {
        List<DataField> paimonFields = paimonFields();
        Map<String, Object> values = new PaimonSourceRowDecoder(paimonFields).decodeForEvent(paimonRow());
        TapTable sourceTable = sourceTable(paimonFields);
        TapTable targetTable = mysqlTargetTable();

        TapCodecsRegistry sourceCodecs = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), sourceCodecs);
        if (schemaEnforced) {
            java.util.Set<String> transformed = new io.tapdata.entity.codec.filter.TapCodecsFilterManagerSchemaEnforced(sourceCodecs)
                    .transformToTapValueMap(values, sourceTable);
            new io.tapdata.entity.codec.filter.TapCodecsFilterManagerSchemaEnforced(mysqlCodecs())
                    .transformFromTapValueMap(values, targetTable.getNameFieldMap(), transformed);
        } else {
            TapCodecsFilterManager.create(sourceCodecs).transformToTapValueMap(values, sourceTable.getNameFieldMap());
            TapCodecsFilterManager.create(mysqlCodecs()).transformFromTapValueMap(values, targetTable.getNameFieldMap());
        }

        Map<Integer, Object> bound = new LinkedHashMap<>();
        PreparedStatement statement = mock(PreparedStatement.class);
        doAnswer(invocation -> {
            bound.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(statement).setObject(anyInt(), org.mockito.ArgumentMatchers.any());
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        new ExposedMysqlWriteRecorder(connection, targetTable, "contract").bind(values);

        // mysql-core 1.0-SNAPSHOT MysqlWriteRecorder.java:49-57 binds every final value with
        // PreparedStatement.setObject; these assertions therefore cover the actual JDBC boundary.
        assertEquals(Long.MIN_VALUE, bound.get(1));
        assertEquals(Long.MAX_VALUE, bound.get(2));
        BigDecimal decimal = (BigDecimal) bound.get(3);
        assertEquals(new BigDecimal("12345678901234567890.123456789012345678"), decimal);
        assertEquals(18, decimal.scale());
        assertArrayEquals(new byte[]{0, 1, -1, 127}, (byte[]) bound.get(4));
        assertNull(bound.get(5));
        assertEquals("中文🙂 café", bound.get(6));
        assertEquals("[[1,null],[2,3]]", bound.get(7));
        assertEquals("{\"first\":{\"label\":\"值\",\"amount\":12345678901234567890.123456789012345678}}", bound.get(8));
        assertEquals("{\"items\":[7,null],\"attrs\":{\"语言\":\"中文\"}}", bound.get(9));
    }

    private static List<DataField> paimonFields() {
        RowType mapValue = new RowType(Arrays.asList(
                new DataField(0, "label", new VarCharType()), new DataField(1, "amount", new DecimalType(38, 18))));
        RowType nestedRow = new RowType(Arrays.asList(
                new DataField(0, "items", new ArrayType(new IntType())),
                new DataField(1, "attrs", new MapType(new VarCharType(), new VarCharType()))));
        return Arrays.asList(
                new DataField(0, "long_min", new BigIntType()),
                new DataField(1, "long_max", new BigIntType()),
                new DataField(2, "decimal_38_18", new DecimalType(38, 18)),
                new DataField(3, "binary_value", new VarBinaryType()),
                new DataField(4, "nullable_value", new VarCharType()),
                new DataField(5, "unicode_value", new VarCharType()),
                new DataField(6, "nested_array", new ArrayType(new ArrayType(new IntType()))),
                new DataField(7, "nested_map", new MapType(new VarCharType(), mapValue)),
                new DataField(8, "nested_row", nestedRow));
    }

    private static GenericRow paimonRow() {
        Map<Object, Object> nestedMap = new LinkedHashMap<>();
        nestedMap.put(BinaryString.fromString("first"),
                GenericRow.of(BinaryString.fromString("值"), Decimal.fromBigDecimal(new BigDecimal("12345678901234567890.123456789012345678"), 38, 18)));
        Map<Object, Object> attrs = new LinkedHashMap<>();
        attrs.put(BinaryString.fromString("语言"), BinaryString.fromString("中文"));
        return GenericRow.of(
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                Decimal.fromBigDecimal(
                        new BigDecimal("12345678901234567890.123456789012345678"), 38, 18),
                new byte[]{0, 1, -1, 127},
                null,
                BinaryString.fromString("中文🙂 café"),
                new GenericArray(new Object[]{
                        new GenericArray(new Integer[]{1, null}),
                        new GenericArray(new Integer[]{2, 3})}),
                new GenericMap(nestedMap),
                GenericRow.of(
                        new GenericArray(new Integer[]{7, null}),
                        new GenericMap(attrs)));
    }

    private static TapTable sourceTable(List<DataField> paimonFields) {
        TapNumber bigint = new TapNumber()
                .bit(64)
                .fixed(true)
                .scale(0)
                .minValue(BigDecimal.valueOf(Long.MIN_VALUE))
                .maxValue(BigDecimal.valueOf(Long.MAX_VALUE));
        TapTable table = new TapTable("source_values");
        table.add(sourceField(paimonFields.get(0), bigint, 1));
        table.add(sourceField(paimonFields.get(1), bigint, 2));
        table.add(sourceField(paimonFields.get(2),
                new TapNumber().precision(38).scale(18).fixed(true), 3));
        table.add(sourceField(paimonFields.get(3), new TapBinary(), 4));
        table.add(sourceField(paimonFields.get(4), new TapString(), 5));
        table.add(sourceField(paimonFields.get(5), new TapString(), 6));
        table.add(sourceField(paimonFields.get(6), new TapArray(), 7));
        table.add(sourceField(paimonFields.get(7), new TapMap(), 8));
        table.add(sourceField(paimonFields.get(8), new TapMap(), 9));
        return table;
    }

    private static TapField sourceField(DataField field, io.tapdata.entity.schema.type.TapType type,
                                        int position) {
        TapField tapField = new TapField(field.name(), field.type().asSQLString()).tapType(type);
        tapField.setPos(position);
        return tapField;
    }

    private static TapTable mysqlTargetTable() {
        TapTable table = new TapTable("target_values");
        addTargetField(table, "long_min", "bigint", 1);
        addTargetField(table, "long_max", "bigint", 2);
        addTargetField(table, "decimal_38_18", "decimal(38,18)", 3);
        addTargetField(table, "binary_value", "varbinary(4)", 4);
        addTargetField(table, "nullable_value", "varchar(64)", 5);
        addTargetField(table, "unicode_value", "varchar(64)", 6);
        addTargetField(table, "nested_array", "json", 7);
        addTargetField(table, "nested_map", "json", 8);
        addTargetField(table, "nested_row", "json", 9);
        return table;
    }

    private static void addTargetField(TapTable table, String name, String dataType, int position) {
        TapField field = new TapField(name, dataType);
        field.setPos(position);
        table.add(field);
    }

    private static TapCodecsRegistry mysqlCodecs() throws Exception {
        MysqlConnector connector = new MysqlConnector();
        MysqlConfig config = new MysqlConfig();
        config.setOldVersionTimezone(false);
        set(connector, "mysqlConfig", config);
        set(connector, "timeZone", TimeZone.getTimeZone(ZoneOffset.UTC));
        TapCodecsRegistry codecs = new TapCodecsRegistry();
        connector.registerCapabilities(new ConnectorFunctions(), codecs);
        return codecs;
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class ExposedMysqlWriteRecorder extends MysqlWriteRecorder {
        private ExposedMysqlWriteRecorder(Connection connection, TapTable table, String schema) {
            super(connection, table, schema);
        }

        private void bind(Map<String, Object> values) throws SQLException {
            upsert(values, new WriteListResult<TapRecordEvent>());
        }
    }
}
