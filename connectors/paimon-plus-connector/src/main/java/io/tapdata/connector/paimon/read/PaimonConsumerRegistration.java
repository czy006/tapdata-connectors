package io.tapdata.connector.paimon.read;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;
import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * 将 Paimon Consumer 用作本读取 epoch 的历史保留边界，固定保护 startSnapshot。
 * 这是连接器的 fixed-start 策略：TapData 源回调没有下游持久化确认，不能据此推进 Consumer。
 * 原生 DataTableStreamScan#notifyCheckpointComplete 会写入传入的 nextSnapshot；本适配器不调用它。
 * @see <a href="https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java#L334">Paimon 原生 checkpoint 完成通知</a>
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
        // TTL 必须来自分支最新持久化 schema，不能用动态 copy 临时“配置”出过期策略。
        // FileStoreTable#copyWithLatestSchema 的旧 options 可能覆盖最新值，源码有对应 TODO：
        // https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/FileStoreTable.java#L113
        // CoreOptions#CONSUMER_EXPIRATION_TIME 无默认值；至少 10 分钟是连接器续租安全预算。
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
        // 两条原生过期路径均以 minNextSnapshot 限制删除边界，故必须始终写 start 而非当前 next。
        // ExpireSnapshotsImpl / ExpireChangelogImpl 的 Consumer 保留逻辑：
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
