package io.tapdata.connector.paimon.read;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;
import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * Uses a Paimon Consumer to retain history at this read epoch's fixed startSnapshot.
 * This is connector fixed-start policy: a TapData source callback is not a durable target ack and cannot advance the Consumer.
 * Native DataTableStreamScan#notifyCheckpointComplete writes its nextSnapshot argument; this adapter does not call it.
 * @see <a href="https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java#L334">Native Paimon checkpoint completion notification</a>
 */
public final class PaimonConsumerRegistration implements PaimonConsumerLease.Target {
    private final FileStoreTable table;
    private final PaimonReadOffset.Position position;
    private final PaimonReadPositionResolver resolver;
    private final ConsumerManager manager;
    private final String id;
    private Long ttlNanos;
    private boolean registered;

    public PaimonConsumerRegistration(FileStoreTable table, String name, PaimonReadOffset offset,
            String attempt, BooleanSupplier active) {
        UUID.fromString(attempt);
        this.table = table; this.position = offset.position(name);
        if (!PaimonReadPositionResolver.identity(table).equals(position.physicalTableHash)) {
            throw new IllegalArgumentException("Consumer table identity mismatch: " + name);
        }
        this.resolver = new PaimonReadPositionResolver(table, name, active);
        this.manager = table.consumerManager();
        this.id = "tapdata-" + offset.readEpoch() + "-" + attempt + "-" + position.physicalTableHash;
    }
    public String id() { return id; }

    @Override
    public long readTtlNanos() {
        // Read TTL from the latest persisted branch schema; a dynamic copy must not fabricate an expiration policy.
        // FileStoreTable#copyWithLatestSchema may overwrite new options with old ones, as its TODO explains:
        // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/FileStoreTable.java#L113
        // CoreOptions#CONSUMER_EXPIRATION_TIME has no default; the 10-minute minimum is a connector renewal budget.
        // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L1234
        Duration ttl = new CoreOptions(table.schemaManager().latest()
                .orElseThrow(() -> new IllegalStateException("Consumer table schema missing")).options()).consumerExpireTime();
        if (ttl == null || ttl.compareTo(Duration.ofMinutes(10)) < 0) {
            throw new IllegalStateException("fixed-start requires persisted consumer.expiration-time >= 10 minutes");
        }
        long nanos;
        try { nanos = ttl.toNanos(); }
        catch (ArithmeticException overflow) { throw new IllegalStateException("Consumer TTL exceeds safe nanosecond deadline range", overflow); }
        // Keep deadline differences within the same safe quarter-range as the Service stop controller.
        if (nanos > Long.MAX_VALUE / 4) { throw new IllegalStateException("Consumer TTL exceeds safe nanosecond deadline range"); }
        if (ttlNanos != null && ttlNanos != nanos) { throw new IllegalStateException("Consumer TTL changed; restart required"); }
        ttlNanos = nanos;
        return nanos;
    }

    @Override
    public void writeAndVerify() throws InterruptedException {
        resolver.validate(position.startSnapshot);
        resolver.validate(position.nextSnapshot);
        // ConsumerManager.resetConsumer overwrites and consumer() reads back (66-76). There is no CAS
        // with expire() (95-108), so verify history as well; never recreate a missing renewal file.
        // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java#L66
        if (registered && manager.consumer(id).map(Consumer::nextSnapshot).orElse(-1L) != position.startSnapshot) {
            throw new IllegalStateException("Consumer protection disappeared or changed: " + id);
        }
        // Both native expiration paths cap deletion at minNextSnapshot; fixed-start must write start, not current progress.
        // Consumer retention in ExpireSnapshotsImpl / ExpireChangelogImpl:
        // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/ExpireSnapshotsImpl.java#L119
        // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/ExpireChangelogImpl.java#L122
        manager.resetConsumer(id, new Consumer(position.startSnapshot));
        if (manager.consumer(id).map(Consumer::nextSnapshot).orElse(-1L) != position.startSnapshot) {
            throw new IllegalStateException("Consumer readback mismatch: " + id);
        }
        resolver.validate(position.startSnapshot);
        resolver.validate(position.nextSnapshot);
        registered = true;
    }
}
