package io.tapdata.connector.paimon;

import com.google.gson.JsonParser;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.utils.DataMap;
import org.apache.paimon.types.*;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class PaimonSourceTypeMappingTest {
    static TapType mapped(DataType type) throws Exception {
        try (Reader reader = new InputStreamReader(PaimonSourceTypeMappingTest.class.getResourceAsStream("/spec.json"), StandardCharsets.UTF_8)) {
            DefaultExpressionMatchingMap mappings = DefaultExpressionMatchingMap.map(
                    new JsonParser().parse(reader).getAsJsonObject().get("dataTypes").toString());
            String sql = type.copy(true).asSQLString();
            TypeExprResult<DataMap> result = mappings.get(sql);
            assertNotNull(result, sql);
            return ((TapMapping) result.getValue().get(TapMapping.FIELD_TYPE_MAPPING)).toTapType(sql, result.getParams());
        }
    }
    @Test
    void nestedParameterizedComplexTypesMustMatch() throws Exception {
        RowType nested = new RowType(Arrays.asList(new DataField(0, "text", new VarCharType(false, 200)),
                new DataField(1, "amount", new DecimalType(38, 18))));
        assertTrue(mapped(new ArrayType(nested)) instanceof TapArray);
        assertTrue(mapped(new MapType(new VarCharType(), new ArrayType(nested))) instanceof TapMap);
        assertTrue(mapped(nested) instanceof TapMap);
    }
    @Test
    void timestampMetadataMustRetainNanosecondPrecisionAndFullRange() throws Exception {
        TapDateTime timestamp = (TapDateTime) mapped(new TimestampType(9));
        assertEquals(9, timestamp.getFraction());
        TapDateTime ltz = (TapDateTime) mapped(new LocalZonedTimestampType(9));
        assertEquals(9, ltz.getFraction());
        assertTrue(ltz.getWithTimeZone());
        // Both ends exceed MySQL TIMESTAMP's narrow 1970..2038 interval.
        assertTrue(timestamp.getMin().isBefore(java.time.Instant.EPOCH));
        assertTrue(timestamp.getMax().isAfter(java.time.Instant.ofEpochSecond(2147483647)));
    }
    @Test
    void scalarMetadataMustKeepNumericPrecisionAndDeclaredLengths() throws Exception {
        TapNumber bigint = (TapNumber) mapped(new BigIntType());
        assertEquals(64, bigint.getBit());
        assertEquals(new java.math.BigDecimal(Long.toString(Long.MIN_VALUE)), bigint.getMinValue());
        assertEquals(new java.math.BigDecimal(Long.toString(Long.MAX_VALUE)), bigint.getMaxValue());
        TapNumber decimal = (TapNumber) mapped(new DecimalType(38, 18));
        assertEquals(38, decimal.getPrecision());
        assertEquals(18, decimal.getScale());
        assertEquals(1000L, ((TapString) mapped(new VarCharType(1000))).getBytes());
        assertEquals(1000L, ((TapBinary) mapped(new VarBinaryType(1000))).getBytes());
    }
}
