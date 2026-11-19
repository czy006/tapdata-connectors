package io.tapdata.connector.paimon.schema;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.entity.schema.TapTable;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collection;
import java.util.HashSet;

/**
 * Checks the effective physical table on initial write and on recovery. Source replayability,
 * exclusive ownership and warehouse retention are deployment properties outside the table schema.
 */
public final class PaimonRecoveryAdmission {
    private PaimonRecoveryAdmission() {}

    public static void validateNewTable(PaimonConfig config, TapTable source, Schema effectiveSchema) {
        if (!config.isLocalRecoveryEnabled()) {
            return;
        }
        CoreOptions options = CoreOptions.fromMap(effectiveSchema.options());
        TableSchema schema = TableSchema.create(0L, effectiveSchema);
        BucketMode mode = PaimonWriteSemanticContractResolver.deriveBucketMode(schema, options.bucket());
        validateSemantics(config, source.getName(), schema, options, mode);
        validateMappedKeys(source, schema);
    }

    public static void validateInput(PaimonConfig config, TapTable source, FileStoreTable target) {
        if (!config.isLocalRecoveryEnabled()) {
            return;
        }
        validateTable(config, source.getName(), target);
        validateMappedKeys(source, target.schema());
    }

    private static void validateMappedKeys(TapTable source, TableSchema schema) {
        Collection<String> mappedKeys = source.primaryKeys(true);
        require(mappedKeys != null && new HashSet<>(mappedKeys).equals(new HashSet<>(schema.primaryKeys())),
                source.getName(), "the mapped TapData primary key must match the physical primary key");
    }

    public static void validateTable(PaimonConfig config, String tableName, FileStoreTable table) {
        if (!config.isLocalRecoveryEnabled()) {
            return;
        }
        validateSemantics(config, tableName, table.schema(), table.coreOptions(), table.bucketMode());
    }

    private static void validateSemantics(PaimonConfig config, String tableName, TableSchema schema,
            CoreOptions options, BucketMode mode) {
        require(!schema.primaryKeys().isEmpty(), tableName, "a stable primary key is required");
        require(mode == BucketMode.HASH_FIXED || mode == BucketMode.HASH_DYNAMIC || mode == BucketMode.KEY_DYNAMIC,
                tableName, "unsupported bucket mode " + mode);
        require(options.mergeEngine() == CoreOptions.MergeEngine.DEDUPLICATE, tableName,
                "unsupported merge-engine " + options.mergeEngine());
        require(!options.ignoreDelete() && !options.ignoreUpdateBefore(), tableName,
                "ignore-delete and ignore-update-before must be false");
        require(config.getEnableDeduplicateUpdateAfterCompletenessCheck(tableName), tableName,
                "enableDeduplicateUpdateAfterCompletenessCheck must be true");
        require(!(Boolean.TRUE.equals(config.getHashKey(tableName)) && schema.fieldNames().contains("_hash_key")),
                tableName, "hashKey must be disabled for a physical _hash_key column; synthetic keys can collide");
        // IndexBootstrap filters by the creation time of data files, including keys unchanged for years.
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/IndexBootstrap.java#L129
        require(mode != BucketMode.KEY_DYNAMIC || options.crossPartitionUpsertIndexTtl() == null, tableName,
                "cross-partition-upsert.index-ttl must be unset");
        // DataTableBatchScan may hide level-zero business data until maintenance. Row tracking
        // also assigns metadata during commit, outside the verified manifest equivalence contract.
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableBatchScan.java#L60
        require(!options.deletionVectorsEnabled(), tableName, "deletion-vectors.enabled must be false");
        require(!options.rowTrackingEnabled(), tableName, "row-tracking.enabled must be false");
        require(options.changelogProducer() == CoreOptions.ChangelogProducer.NONE, tableName,
                "changelog-producer must be none");
        // A commit may append both APPEND and COMPACT. Keep the previously verified baseline
        // until both new snapshots have been verified, even after snapshot.time-retained elapses.
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/ExpireSnapshotsImpl.java#L114-L136
        require(options.writeOnly() || options.snapshotNumRetainMin() >= 3, tableName,
                "snapshot.num-retained.min must be at least 3 when write-only is false");
        require(options.sequenceField().isEmpty(), tableName, "sequence.field requires an independently verified ordering contract");
        require(!options.rowkindField().isPresent(), tableName, "rowkind.field changes the mapped business row");
        require(options.recordLevelExpireTime() == null && options.partitionExpireTime() == null, tableName,
                "record-level.expire-time and partition.expiration-time must be unset");
        require(options.startupMode() == CoreOptions.StartupMode.LATEST_FULL, tableName,
                "the effective scan.mode must read the latest full snapshot");
    }

    private static void require(boolean supported, String tableName, String reason) {
        if (!supported) {
            throw new PaimonFatalWriteException("PAIMON_LOCAL_RECOVERY_UNSUPPORTED: " + tableName + ": " + reason);
        }
    }
}
