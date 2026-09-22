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

/** Source-only representations within the existing PDK codec extension point. */
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
