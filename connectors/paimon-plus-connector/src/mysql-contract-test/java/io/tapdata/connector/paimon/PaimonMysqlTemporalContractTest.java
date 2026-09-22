package io.tapdata.connector.paimon;

import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Opt-in contract at the Paimon-source/MySQL-target codec boundary; no database is involved. */
class PaimonMysqlTemporalContractTest {

    @Test
    void utcTargetTimezoneKeepsLocalDateTimeWallClockAndDocumentsFractionPrecision() throws Exception {
        TapCodecsRegistry codecs = mysqlCodecs(ZoneOffset.UTC);
        LocalDateTime micros = LocalDateTime.of(2026, 9, 22, 10, 11, 12, 123_456_000);
        LocalDateTime nanos = LocalDateTime.of(2026, 9, 22, 10, 11, 12, 123_456_789);

        assertEquals("2026-09-22 10:11:12.123456", encodeDateTime(codecs, new DateTime(micros)));
        assertEquals("2026-09-22 10:11:12.123456", encodeDateTime(codecs, new DateTime(nanos)));
    }

    @Test
    void nonUtcTargetTimezoneShiftsTimezoneLessLocalDateTimeWallClock() throws Exception {
        LocalDateTime value = LocalDateTime.of(2026, 9, 22, 10, 11, 12, 123_456_789);

        assertEquals("2026-09-22 18:11:12.123456", encodeDateTime(mysqlCodecs(ZoneOffset.ofHours(8)), new DateTime(value)));
        assertEquals("2026-09-22 02:11:12.123456", encodeDateTime(mysqlCodecs(ZoneOffset.ofHours(-8)), new DateTime(value)));
    }

    @Test
    void zonedDateTimeIsNormalizedToUtcInstantRatherThanKeepingItsWallClock() throws Exception {
        TapCodecsRegistry codecs = mysqlCodecs(ZoneOffset.ofHours(8));

        DateTime utc = new DateTime(ZonedDateTime.of(2026, 9, 22, 10, 11, 12, 123_456_789, ZoneOffset.UTC));
        DateTime plusEight = new DateTime(ZonedDateTime.of(2026, 9, 22, 10, 11, 12, 123_456_789, ZoneOffset.ofHours(8)));
        DateTime minusEight = new DateTime(ZonedDateTime.of(2026, 9, 22, 10, 11, 12, 123_456_789, ZoneOffset.ofHours(-8)));

        assertEquals("2026-09-22 10:11:12.123456", encodeDateTime(codecs, utc));
        assertEquals("2026-09-22 02:11:12.123456", encodeDateTime(codecs, plusEight));
        assertEquals("2026-09-22 18:11:12.123456", encodeDateTime(codecs, minusEight));
        assertEquals("UTC", plusEight.getTimeZone().getID());
        assertEquals("UTC", minusEight.getTimeZone().getID());
    }

    @Test
    void dateKeepsSourceWallDateAndTimeKeepsMicrosecondsAndNanoseconds() throws Exception {
        TapCodecsRegistry codecs = mysqlCodecs(ZoneOffset.UTC);
        FromTapValueCodec<TapDateValue> dateCodec = codecs.getFromTapValueCodec(TapDateValue.class);
        FromTapValueCodec<TapTimeValue> timeCodec = codecs.getFromTapValueCodec(TapTimeValue.class);

        for (ZoneOffset offset : new ZoneOffset[]{ZoneOffset.UTC, ZoneOffset.ofHours(8), ZoneOffset.ofHours(-8)}) {
            DateTime date = new DateTime(ZonedDateTime.of(2026, 9, 22, 0, 0, 0, 0, offset));
            assertEquals("2026-09-22", dateCodec.fromTapValue(new TapDateValue(date)));
        }
        assertEquals("10:11:12.123456", timeCodec.fromTapValue(new TapTimeValue(
                new DateTime(10L * 3_600_000_000L + 11L * 60_000_000L + 12_123_456L, 6))));
        assertEquals("10:11:12.123456789", timeCodec.fromTapValue(new TapTimeValue(
                new DateTime(10L * 3_600_000_000_000L + 11L * 60_000_000_000L + 12_123_456_789L, 9))));
    }

    @Test
    void paimonSourceCodecMustPreserveWallClockThroughRealFilters() throws Exception {
        for (ZoneOffset targetOffset : new ZoneOffset[]{ZoneOffset.UTC, ZoneOffset.ofHours(8), ZoneOffset.ofHours(-8)}) {
            TapCodecsRegistry source = new TapCodecsRegistry();
            new PaimonConnector().registerCapabilities(new ConnectorFunctions(), source);
            java.util.Map<String, Object> record = new java.util.LinkedHashMap<>();
            record.put("ts", LocalDateTime.of(2040, 9, 22, 10, 11, 12, 123_456_000));
            io.tapdata.entity.schema.TapTable table = new io.tapdata.entity.schema.TapTable("types");
            table.add(new io.tapdata.entity.schema.TapField("ts", "TIMESTAMP(6)").tapType(
                    new io.tapdata.entity.schema.type.TapDateTime().fraction(6).withTimeZone(false)));
            io.tapdata.entity.codec.filter.TapCodecsFilterManager.create(source)
                    .transformToTapValueMap(record, table.getNameFieldMap());
            TapDateTimeValue encoded = (TapDateTimeValue) record.get("ts");
            assertEquals(false, encoded.getTapType().getWithTimeZone());
            io.tapdata.entity.codec.filter.TapCodecsFilterManager.create(mysqlCodecs(targetOffset))
                    .transformFromTapValueMap(record, table.getNameFieldMap());
            assertEquals("2040-09-22 10:11:12.123456", record.get("ts"));
        }
    }

    @Test
    void nativePaimonDateAndTimeMustSurviveJvmTimezoneChanges() throws Exception {
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : new String[]{"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                TapCodecsRegistry source = new TapCodecsRegistry();
                new PaimonConnector().registerCapabilities(new ConnectorFunctions(), source);
                java.util.List<org.apache.paimon.types.DataField> fields = java.util.Arrays.asList(
                        new org.apache.paimon.types.DataField(0, "date", new org.apache.paimon.types.DateType()),
                        new org.apache.paimon.types.DataField(1, "time", new org.apache.paimon.types.TimeType()));
                for (int day : new int[]{-1, 0, 20000}) {
                    java.util.Map<String, Object> record = new io.tapdata.connector.paimon.read.PaimonSourceRowDecoder(fields)
                            .decode(org.apache.paimon.data.GenericRow.of(day, 86_399_123));
                    io.tapdata.entity.schema.TapTable table = new io.tapdata.entity.schema.TapTable("types");
                    table.add(new io.tapdata.entity.schema.TapField("date", "DATE").tapType(new io.tapdata.entity.schema.type.TapDate()));
                    table.add(new io.tapdata.entity.schema.TapField("time", "TIME(3)").tapType(new io.tapdata.entity.schema.type.TapTime().fraction(3)));
                    io.tapdata.entity.codec.filter.TapCodecsFilterManager.create(source).transformToTapValueMap(record, table.getNameFieldMap());
                    io.tapdata.entity.codec.filter.TapCodecsFilterManager.create(mysqlCodecs(ZoneOffset.ofHours(8))).transformFromTapValueMap(record, table.getNameFieldMap());
                    assertEquals(java.time.LocalDate.ofEpochDay(day).toString(), record.get("date"));
                    assertEquals("23:59:59.123", record.get("time"));
                }
            }
        } finally { TimeZone.setDefault(original); }
    }

    private static TapCodecsRegistry mysqlCodecs(ZoneOffset targetOffset) throws Exception {
        MysqlConnector connector = new MysqlConnector();
        MysqlConfig config = new MysqlConfig();
        config.setOldVersionTimezone(false);
        set(connector, "mysqlConfig", config);
        set(connector, "timeZone", TimeZone.getTimeZone(targetOffset));
        TapCodecsRegistry codecs = new TapCodecsRegistry();
        connector.registerCapabilities(new ConnectorFunctions(), codecs);
        return codecs;
    }

    private static Object encodeDateTime(TapCodecsRegistry codecs, DateTime value) {
        return codecs.getFromTapValueCodec(TapDateTimeValue.class).fromTapValue(new TapDateTimeValue(value));
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
