package io.tapdata.connector.paimon.write.bucket;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.write.PaimonPreparedWriterRuntime;

import org.apache.paimon.table.BucketMode;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Exhaustive Paimon 1.3.1 bucket-mode strategy selector.
 *
 * <p>The explicit mapping intentionally follows Paimon's own exhaustive sink selection and never
 * falls back to native writes for an unknown future mode:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/FlinkSinkBuilder.java#L219-L308
 */
public final class PaimonBucketWriterStrategyFactory {

    private static final Set<BucketMode> SUPPORTED_MODES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            BucketMode.HASH_FIXED,
                            BucketMode.HASH_DYNAMIC,
                            BucketMode.KEY_DYNAMIC,
                            BucketMode.POSTPONE_MODE,
                            BucketMode.BUCKET_UNAWARE));

    private PaimonBucketWriterStrategyFactory() {
    }

    public static Set<BucketMode> supportedModes() {
        return SUPPORTED_MODES;
    }

    public static boolean requiresIoManager(BucketMode mode) {
        return Objects.requireNonNull(mode, "mode") == BucketMode.KEY_DYNAMIC;
    }

    public static boolean requiresOrderedSingleWriterIngress(BucketMode mode) {
        BucketMode checked = Objects.requireNonNull(mode, "mode");
        return checked == BucketMode.HASH_DYNAMIC || checked == BucketMode.KEY_DYNAMIC;
    }

    /** Production construction entry; an unprepared raw writer cannot cross this boundary. */
    public static PaimonBucketWriterStrategy create(
            PaimonPreparedWriterRuntime preparedWriter,
            String tableKey,
            String commitUser,
            PaimonWriteSemanticContract writeSemanticContract,
            PaimonBucketWriterRuntimeFactory runtimeFactory)
            throws Exception {
        Objects.requireNonNull(preparedWriter, "preparedWriter");
        Objects.requireNonNull(tableKey, "tableKey");
        Objects.requireNonNull(commitUser, "commitUser");
        Objects.requireNonNull(writeSemanticContract, "writeSemanticContract");
        Objects.requireNonNull(runtimeFactory, "runtimeFactory");
        PaimonBucketWriterStrategyContext context =
                new PaimonBucketWriterStrategyContext(
                        tableKey,
                        preparedWriter.runtimeTable(),
                        preparedWriter.transferWriterToStrategy(),
                        commitUser,
                        preparedWriter.ioManager(),
                        writeSemanticContract);
        BucketMode mode = context.table().bucketMode();
        switch (mode) {
            case HASH_FIXED:
                return new HashFixedBucketWriterStrategy(context);
            case HASH_DYNAMIC:
                return new HashDynamicBucketWriterStrategy(context, runtimeFactory);
            case KEY_DYNAMIC:
                return new KeyDynamicBucketWriterStrategy(context, runtimeFactory);
            case POSTPONE_MODE:
                return new PostponeBucketWriterStrategy(context);
            case BUCKET_UNAWARE:
                return new BucketUnawareWriterStrategy(context);
            default:
                throw new UnsupportedOperationException("Unsupported Paimon bucket mode: " + mode);
        }
    }
}
