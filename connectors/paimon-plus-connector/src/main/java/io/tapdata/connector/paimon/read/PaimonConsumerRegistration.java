package io.tapdata.connector.paimon.read;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;
import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/** Version-pinned adapter; Consumer always protects the epoch's original start, never delivered progress. */
public final class PaimonConsumerRegistration {
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

    public long readTtlNanos() {
        // Persisted latest branch schema, not FileStoreTable.copyWithLatestSchema(), whose old
        // dynamic options may override new schema options (FileStoreTable.java:113).
        // Paimon release-1.3 @ 5c59e6cb01ed0b29563371f56e14fcade4597a2e:
        // paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#CONSUMER_EXPIRATION_TIME (1234-1240).
        Duration ttl = new CoreOptions(table.schemaManager().latest()
                .orElseThrow(() -> new IllegalStateException("Consumer table schema missing")).options()).consumerExpireTime();
        if (ttl == null || ttl.compareTo(Duration.ofMinutes(10)) < 0) {
            throw new IllegalStateException("fixed-start requires persisted consumer.expiration-time >= 10 minutes");
        }
        long nanos = ttl.toNanos();
        if (ttlNanos != null && ttlNanos != nanos) { throw new IllegalStateException("Consumer TTL changed; restart required"); }
        ttlNanos = nanos;
        return nanos;
    }

    public void writeAndVerify() throws InterruptedException {
        resolver.validate(position.startSnapshot);
        resolver.validate(position.nextSnapshot);
        // ConsumerManager.resetConsumer overwrites and consumer() reads back (74-85). There is no CAS
        // with expire() (109-124), so verify history as well; never recreate a missing renewal file.
        // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java#L74
        if (registered && manager.consumer(id).map(Consumer::nextSnapshot).orElse(-1L) != position.startSnapshot) {
            throw new IllegalStateException("Consumer protection disappeared or changed: " + id);
        }
        manager.resetConsumer(id, new Consumer(position.startSnapshot));
        if (manager.consumer(id).map(Consumer::nextSnapshot).orElse(-1L) != position.startSnapshot) {
            throw new IllegalStateException("Consumer readback mismatch: " + id);
        }
        resolver.validate(position.startSnapshot);
        resolver.validate(position.nextSnapshot);
        registered = true;
    }
}
