package io.tapdata.connector.paimon.schema;

import com.alibaba.fastjson.JSONAware;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.time.LocalTime;

/** The PDK TIME codec runs before field lookup. Carry full precision to the typed writer. */
public final class PaimonRecoveryTime implements JSONAware, Serializable {
    private static final long serialVersionUID = 1L;
    private final long nanosOfDay;

    public PaimonRecoveryTime(LocalTime time) {
        nanosOfDay = time.toNanoOfDay();
    }

    boolean isMillisecondAligned() {
        return nanosOfDay % 1_000_000L == 0;
    }

    @JsonValue
    public int millisOfDay() {
        return Math.toIntExact(nanosOfDay / 1_000_000L);
    }

    // TapData's JsonParserImpl uses FastJSON when embedding a TIME in a JSON business column.
    // https://github.com/tapdata/tapdata-common-lib/blob/b09e683c27808598c01d85ab53cb29d500956d8f/plugin-kit/tapdata-pdk-runner/src/main/java/io/tapdata/pdk/core/api/impl/JsonParserImpl.java#L74-L94
    @Override
    public String toJSONString() {
        return Integer.toString(millisOfDay());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PaimonRecoveryTime && nanosOfDay == ((PaimonRecoveryTime) other).nanosOfDay;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(nanosOfDay);
    }
}
