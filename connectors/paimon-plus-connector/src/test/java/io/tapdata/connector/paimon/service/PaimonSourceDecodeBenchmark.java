package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.read.PaimonSourceRowDecoder;
import org.apache.paimon.data.*;
import org.apache.paimon.types.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

/** Standalone bounded microbenchmark; never a throughput assertion in unit tests.
 * Baseline is the scalar decode loop from PaimonService at 29995888, including default Map sizing.
 * Run without JaCoCo, with fixed heap, then repeat in fresh JVMs. No storage/network is timed.
 */
public final class PaimonSourceDecodeBenchmark {
    private static volatile long sink;
    public static void main(String[] args) {
        for (int width : new int[]{8, 32, 128}) {
            List<DataField> fields = new ArrayList<>();
            GenericRow row = new GenericRow(width);
            for (int i = 0; i < width; i++) {
                DataType type;
                Object value;
                switch (i % 4) {
                    case 0: type = new BigIntType(); value = 9007199254740993L; break;
                    case 1: type = new DecimalType(38, 18); value = Decimal.fromBigDecimal(new BigDecimal("12345678901234567890.123456789012345678"), 38, 18); break;
                    case 2: type = new TimestampType(9); value = Timestamp.fromLocalDateTime(LocalDateTime.parse("2026-09-22T12:34:56.123456789")); break;
                    default: type = new VarCharType(); value = BinaryString.fromString("paimon-mysql-value");
                }
                fields.add(new DataField(i, "f" + i, type)); row.setField(i, value);
            }
            PaimonSourceRowDecoder decoder = new PaimonSourceRowDecoder(fields);
            int rows = 200_000;
            for (int i = 0; i < 3; i++) { measure(row, fields, decoder, rows / 4, true); measure(row, fields, decoder, rows / 4, false); }
            for (int run = 0; run < 6; run++) {
                boolean first = run % 2 == 0;
                long a = measure(row, fields, decoder, rows, first);
                long b = measure(row, fields, decoder, rows, !first);
                System.out.printf(Locale.ROOT, "decode,width=%d,run=%d,rows=%d,baseline_ns=%d,candidate_ns=%d%n", width, run, rows, first ? a : b, first ? b : a);
            }
        }
        System.out.println("checksum=" + sink);
    }
    private static long measure(GenericRow row, List<DataField> fields, PaimonSourceRowDecoder decoder, int count, boolean baseline) {
        long checksum = 0;
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            Map<String, Object> result = baseline ? legacy(row, fields) : decoder.decode(row);
            checksum += result.size() + ((Long) result.get("f0") & 0xffff);
        }
        long elapsed = System.nanoTime() - start;
        sink += checksum;
        return elapsed;
    }
    // Exact relevant scalar branches of the pre-review method; field list cached in both sides
    // to conservatively exclude AbstractFileStoreTable.rowType() reconstruction from the baseline.
    private static Map<String, Object> legacy(InternalRow row, List<DataField> fields) {
        Map<String, Object> data = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (row.isNullAt(i)) { data.put(field.name(), null); continue; }
            DataType type = field.type();
            Object value;
            switch (type.getTypeRoot().name()) {
                case "BIGINT": value = row.getLong(i); break;
                case "DECIMAL": DecimalType decimal = (DecimalType) type; value = row.getDecimal(i, decimal.getPrecision(), decimal.getScale()).toBigDecimal(); break;
                case "TIMESTAMP_WITHOUT_TIME_ZONE": value = row.getTimestamp(i, ((TimestampType) type).getPrecision()).toLocalDateTime(); break;
                default: value = row.getString(i).toString();
            }
            data.put(field.name(), value);
        }
        return data;
    }
}
