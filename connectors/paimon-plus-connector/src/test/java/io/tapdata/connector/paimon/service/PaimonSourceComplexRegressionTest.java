package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.*;
import io.tapdata.connector.paimon.read.PaimonSourceRowDecoder;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class PaimonSourceComplexRegressionTest {
    @Test
    void nestedNativeValuesMustBecomeOwnedJavaValues() throws Exception {
        RowType nested = new RowType(Arrays.asList(new DataField(0, "label", new VarCharType())));
        List<DataField> fields = Arrays.asList(
                new DataField(0, "items", new ArrayType(new IntType())),
                new DataField(1, "attributes", new MapType(new VarCharType(), new IntType())),
                new DataField(2, "detail", nested));
        Map<Object, Object> attributes = new LinkedHashMap<>();
        attributes.put(BinaryString.fromString("answer"), 42);
        GenericRow row = GenericRow.of(new GenericArray(new Integer[]{1, null, 3}),
                new GenericMap(attributes), GenericRow.of(BinaryString.fromString("hello")));
        Map<?, ?> decoded = new PaimonSourceRowDecoder(fields).decode(row);
        assertEquals(Arrays.asList(1, null, 3), decoded.get("items"));
        assertEquals(Collections.singletonMap("answer", 42), decoded.get("attributes"));
        assertEquals(Collections.singletonMap("label", "hello"), decoded.get("detail"));
        // Paimon file readers commonly expose BinaryRow, not only GenericRow fixtures.
        org.apache.paimon.data.serializer.InternalRowSerializer serializer =
                new org.apache.paimon.data.serializer.InternalRowSerializer(new RowType(fields));
        assertEquals(decoded, new PaimonSourceRowDecoder(fields).decode(serializer.toBinaryRow(row)));
    }
    @Test
    void releasedNativeBuffersMustNotMutateEmittedValues() {
        byte[] bytes = {1, 2, 3};
        GenericArray array = new GenericArray(new Integer[]{1, 2});
        GenericRow nested = GenericRow.of(BinaryString.fromString("before"));
        List<DataField> fields = Arrays.asList(new DataField(0, "bytes", new VarBinaryType()),
                new DataField(1, "array", new ArrayType(new IntType())),
                new DataField(2, "row", new RowType(Collections.singletonList(new DataField(0, "name", new VarCharType())))));
        Map<String, Object> result = new PaimonSourceRowDecoder(fields)
                .decode(GenericRow.of(bytes, array, nested));
        bytes[0] = 99;
        nested.setField(0, BinaryString.fromString("after"));
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) result.get("bytes"));
        assertEquals(Collections.singletonMap("name", "before"), result.get("row"));
    }

    @Test
    void rejectAmbiguousJsonMapKeysAndUnsupportedTypesBeforeReading() {
        assertThrows(IllegalArgumentException.class, () -> new PaimonSourceRowDecoder(
                Collections.singletonList(new DataField(0, "m", new MapType(new IntType(), new VarCharType())))));
        assertThrows(IllegalArgumentException.class, () -> new PaimonSourceRowDecoder(
                Collections.singletonList(new DataField(0, "m", new MultisetType(new VarCharType())))));
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(null, 1);
        PaimonSourceRowDecoder decoder = new PaimonSourceRowDecoder(
                Collections.singletonList(new DataField(0, "m", new MapType(new VarCharType(), new IntType()))));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(GenericRow.of(new GenericMap(map))));
    }

    @Test
    void preserveTimestampNanosecondsNullsAndNestedJsonRepresentations() {
        java.time.LocalDateTime local = java.time.LocalDateTime.parse("1969-12-31T23:59:59.123456789");
        List<DataField> fields = Arrays.asList(new DataField(0, "ts", new TimestampType(9)),
                new DataField(1, "ltz", new LocalZonedTimestampType(9)), new DataField(2, "nullable", new IntType(false)),
                new DataField(3, "dates", new ArrayType(new DateType())),
                new DataField(4, "bytes", new ArrayType(new VarBinaryType())));
        Map<String, Object> result = new PaimonSourceRowDecoder(fields).decode(GenericRow.of(
                org.apache.paimon.data.Timestamp.fromLocalDateTime(local), org.apache.paimon.data.Timestamp.fromLocalDateTime(local), null,
                new GenericArray(new Integer[]{-1, 0}), new GenericArray(new Object[]{new byte[]{0, 1, 2}, null})));
        assertEquals(local, result.get("ts"));
        assertEquals(local.atZone(java.time.ZoneOffset.UTC), result.get("ltz"));
        assertNull(result.get("nullable"));
        assertEquals(Arrays.asList("1969-12-31", "1970-01-01"), result.get("dates"));
        assertEquals(Arrays.asList("AAEC", null), result.get("bytes"));
    }
    @Test
    void nativeScalarAccessorsMustRetainTypesAndValues() {
        List<DataField> fields = Arrays.asList(new DataField(0, "boolean", new BooleanType()),
                new DataField(1, "byte", new TinyIntType()), new DataField(2, "short", new SmallIntType()),
                new DataField(3, "int", new IntType()), new DataField(4, "float", new FloatType()),
                new DataField(5, "double", new DoubleType()));
        Map<String, Object> decoded = new PaimonSourceRowDecoder(fields).decode(GenericRow.of(
                true, Byte.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE, Float.MAX_VALUE, Double.MIN_VALUE));
        assertEquals(true, decoded.get("boolean"));
        assertEquals(Byte.MIN_VALUE, decoded.get("byte"));
        assertEquals(Short.MAX_VALUE, decoded.get("short"));
        assertEquals(Integer.MIN_VALUE, decoded.get("int"));
        assertEquals(Float.MAX_VALUE, decoded.get("float"));
        assertEquals(Double.MIN_VALUE, decoded.get("double"));
    }
    @Test
    void nonFiniteNestedNumbersMustFailInsteadOfSilentlyBecomingJsonNull() {
        PaimonSourceRowDecoder decoder = new PaimonSourceRowDecoder(Collections.singletonList(
                new DataField(0, "values", new ArrayType(new DoubleType()))));
        for (double value : new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class, () -> decoder.decodeForEvent(
                    GenericRow.of(new GenericArray(new Double[]{value}))));
        }
    }
}
