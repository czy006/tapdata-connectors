package io.tapdata.connector.paimon.read;

import io.tapdata.pdk.core.api.impl.serialize.ObjectSerializableImplV2;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class PaimonReadOffsetTest {
    static final String HASH = PaimonReadOffset.physicalTableHash(URI.create("file:/warehouse/orders"), "main");
    static PaimonReadOffset offset() {
        Map<String, PaimonReadOffset.Position> positions = new LinkedHashMap<>();
        positions.put("orders", new PaimonReadOffset.Position(HASH, 5, 8));
        positions.put("unrequested", new PaimonReadOffset.Position(HASH, 1, 2));
        return new PaimonReadOffset(UUID.randomUUID().toString(), positions);
    }
    @Test void pdkBinaryCodecRoundTripKeepsNextAndUnrequestedTables() {
        // Actual PDK runner 2.8-SNAPSHOT ObjectSerializableImplV2.fromObject/toObject,
        // not a JSON mock: offsets consist only of maps, strings and version integer.
        ObjectSerializableImplV2 codec = new ObjectSerializableImplV2();
        PaimonReadOffset original = offset();
        PaimonReadOffset decoded = PaimonReadOffset.parse(codec.toObject(codec.fromObject(original.toMap())));
        assertEquals(original.toMap(), decoded.toMap());
        assertEquals(8, decoded.position("orders").nextSnapshot);
    }
    @Test void advancingOneTableDoesNotMutatePublishedMapsOrOtherTables() {
        PaimonReadOffset original = offset();
        Map<String, Object> published = original.toMap();
        PaimonReadOffset advanced = original.withNext("orders", 10);
        assertEquals(8, PaimonReadOffset.parse(published).position("orders").nextSnapshot);
        assertEquals(5, advanced.position("orders").startSnapshot);
        assertEquals(2, advanced.position("unrequested").nextSnapshot);
        published.clear();
        assertEquals(8, original.position("orders").nextSnapshot);
        assertThrows(IllegalArgumentException.class, () -> advanced.withNext("orders", 9));
    }
    @Test void rejectsLegacyNullUnknownFieldsVersionsAndMissingTable() {
        assertThrows(IllegalArgumentException.class, () -> PaimonReadOffset.parse(null));
        assertThrows(IllegalArgumentException.class, () -> PaimonReadOffset.parse(Collections.singletonMap("orders", 8L)));
        Map<String, Object> map = offset().toMap();
        map.put("unknown", true);
        assertThrows(IllegalArgumentException.class, () -> PaimonReadOffset.parse(map));
        map.remove("unknown"); map.put("version", 1.5);
        assertThrows(IllegalArgumentException.class, () -> PaimonReadOffset.parse(map));
        assertThrows(IllegalArgumentException.class, () -> offset().position("missing"));
    }
    @SuppressWarnings("unchecked")
    @Test void rejectsAmbiguousOrOverflowingSnapshots() {
        for (Object invalid : new Object[]{null, 1L, "0", "-1", "01", "1.0", "9223372036854775808"}) {
            Map<String, Object> map = offset().toMap();
            Map<String, Object> table = (Map<String, Object>) ((Map<?, ?>) map.get("tables")).get("orders");
            table.put("nextSnapshot", invalid);
            assertThrows(IllegalArgumentException.class, () -> PaimonReadOffset.parse(map));
        }
        assertThrows(IllegalArgumentException.class, () -> new PaimonReadOffset.Position(HASH, 9, 8));
    }
    @Test void identityIgnoresCredentialsButSeparatesBranchesAndLocations() {
        assertEquals(PaimonReadOffset.physicalTableHash(URI.create("s3://user:secret@bucket/a/../b/?key=secret#x"), "main"),
                PaimonReadOffset.physicalTableHash(URI.create("s3://bucket/b"), "main"));
        assertNotEquals(PaimonReadOffset.physicalTableHash(URI.create("s3://bucket_a/b"), "main"),
                PaimonReadOffset.physicalTableHash(URI.create("s3://bucket_b/b"), "main"));
        assertNotEquals(HASH, PaimonReadOffset.physicalTableHash(URI.create("file:/warehouse/orders"), "other"));
    }
}
