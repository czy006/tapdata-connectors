package io.tapdata.connector.paimon.read;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.codec.impl.ToTapMapCodec;
import io.tapdata.entity.codec.impl.ToTapArrayCodec;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import java.time.LocalDateTime;
import java.util.TimeZone;

/**
 * 在既有 PDK 扩展点注册源端 ToTapValue 适配，不修改 Engine 或 MySQL 的 FromTapValue。
 * 本类处理 TapData 表示边界；Paimon 原生取值依据见 PaimonSourceRowDecoder。
 * 复杂值的外层恢复与时间载体修正均是连接器策略，不是 Paimon 内核提供的 codec。
 */
public final class PaimonSourceCodecs {
    private PaimonSourceCodecs() {}

    public static void register(TapCodecsRegistry registry) {
        registry.registerToTapValue(String.class, (value, type) -> {
            // PaimonSourceRowDecoder supplies standard JSON strings for complex event fields.
            // Restore only the outer TapValue so nested maps/arrays remain native JSON values.
            if (type instanceof TapMap) {
                return new ToTapMapCodec().toTapValue(value, type);
            }
            if (type instanceof TapArray) {
                return new ToTapArrayCodec().toTapValue(value, type);
            }
            return new TapStringValue((String) value);
        });
        // 来源：io.tapdata:tapdata-api:2.0.8-SNAPSHOT sources.jar，
        // io/tapdata/entity/schema/value/DateTime.java#DateTime(LocalDateTime):234。
        // 对照 connectors/mysql-connector/src/main/java/io/tapdata/connector/mysql/MysqlConnector.java。
        // tapdata-api 2.0.8-SNAPSHOT DateTime.java#DateTime(LocalDateTime):234-240 encodes
        // wall-clock fields as UTC seconds, with a null timeZone. MysqlConnector.java
        // #registerCapabilities:248-265 at connectors@7c40958f then applies the target zone,
        // shifting those fields. Explicit UTC describes the PDK carrier, NOT a source time zone;
        // TapDateTime.withTimeZone remains false and DateTime origin remains LOCAL_DATETIME.
        // Do not apply this override to ZonedDateTime: Paimon LTZ represents an actual instant.
        registry.registerToTapValue(LocalDateTime.class, (value, type) -> {
            DateTime carrier = new DateTime((LocalDateTime) value);
            carrier.setTimeZone(TimeZone.getTimeZone("UTC"));
            return type instanceof TapTime ? new TapTimeValue(carrier) : new TapDateTimeValue(carrier);
        });
    }
}
