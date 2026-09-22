package io.tapdata.connector.paimon;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.codec.filter.TapCodecsFilterManagerSchemaEnforced;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PaimonSourceNumberCodecTest {

    @Test
    void defaultFilterManagerMustPreservePaimonNumberValuesExactly() {
        Fixture fixture = fixture();
        TapCodecsFilterManager source = TapCodecsFilterManager.create(sourceRegistry());
        TapCodecsFilterManager target = TapCodecsFilterManager.create(new TapCodecsRegistry());

        // tapdata-api 2.0.8-SNAPSHOT TapCodecsFilterManager.java:92-121 and 127-180:
        // a schema-qualified TapNumber has no getValueCodec entry, so its original Number must pass
        // through instead of reaching ToTapNumberCodec.java:20-22 (Number -> Double).
        source.transformToTapValueMap(fixture.record, fixture.table.getNameFieldMap());
        target.transformFromTapValueMap(fixture.record, fixture.table.getNameFieldMap());

        assertValues(fixture.record);
    }

    @Test
    void schemaEnforcedBatchFilterManagerMustPreservePaimonNumberValuesExactly() {
        Fixture fixture = fixture();
        TapCodecsFilterManagerSchemaEnforced source =
                new TapCodecsFilterManagerSchemaEnforced(sourceRegistry());
        TapCodecsFilterManagerSchemaEnforced target =
                new TapCodecsFilterManagerSchemaEnforced(new TapCodecsRegistry());

        // Engine batch inserts use this overload. In tapdata-api 2.0.8-SNAPSHOT,
        // TapCodecsFilterManagerSchemaEnforced.java:101-143 applies the same schema-qualified
        // Number contract and returns no transformed field for values that remain native Numbers.
        Set<String> transformed = source.transformToTapValueMap(fixture.record, fixture.table);
        target.transformFromTapValueMap(
                fixture.record, fixture.table.getNameFieldMap(), transformed);

        assertEquals(0, transformed.size());
        assertValues(fixture.record);
    }

    private TapCodecsRegistry sourceRegistry() {
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new io.tapdata.pdk.apis.functions.ConnectorFunctions(), registry);
        return registry;
    }

    private Fixture fixture() {
        BigDecimal decimal = new BigDecimal("12345678901234567890.123456789012345678");
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("long_min", Long.MIN_VALUE);
        record.put("long_max", Long.MAX_VALUE);
        record.put("beyond_double_integer", 9_007_199_254_740_993L);
        record.put("decimal_38_18", decimal);

        TapNumber longType = new TapNumber()
                .bit(64)
                .fixed(true)
                .scale(0)
                .minValue(BigDecimal.valueOf(Long.MIN_VALUE))
                .maxValue(BigDecimal.valueOf(Long.MAX_VALUE));
        TapNumber decimalType = new TapNumber().precision(38).scale(18).fixed(true);
        TapTable table = new TapTable("number_contract");
        table.add(field("long_min", "BIGINT", longType));
        table.add(field("long_max", "BIGINT", longType));
        table.add(field("beyond_double_integer", "BIGINT", longType));
        table.add(field("decimal_38_18", "DECIMAL(38,18)", decimalType));
        return new Fixture(record, table);
    }

    private TapField field(String name, String dataType, TapNumber tapNumber) {
        return new TapField(name, dataType).tapType(tapNumber);
    }

    private void assertValues(Map<String, Object> record) {
        assertEquals(Long.MIN_VALUE, record.get("long_min"));
        assertEquals(Long.MAX_VALUE, record.get("long_max"));
        assertEquals(9_007_199_254_740_993L, record.get("beyond_double_integer"));
        BigDecimal decimal = (BigDecimal) record.get("decimal_38_18");
        assertEquals(new BigDecimal("12345678901234567890.123456789012345678"), decimal);
        assertEquals(18, decimal.scale());
    }

    private static final class Fixture {
        private final Map<String, Object> record;
        private final TapTable table;

        private Fixture(Map<String, Object> record, TapTable table) {
            this.record = record;
            this.table = table;
        }
    }
}
