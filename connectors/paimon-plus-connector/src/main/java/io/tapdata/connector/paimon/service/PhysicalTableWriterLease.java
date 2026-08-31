package io.tapdata.connector.paimon.service;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Immutable, generation-scoped ownership token for one physical Paimon table.
 *
 * <p>The process-wide registry deliberately keys by physical-table hash rather than logical table
 * name. Every release is an expected-value transition, so a delayed close from an old generation
 * cannot remove a replacement generation (the classic ABA failure in the former string owner
 * model).
 */
final class PhysicalTableWriterLease {

    enum Purpose {
        WRITER,
        DDL_ONLY
    }

    private static final Registry JVM_REGISTRY = new Registry();

    private final String physicalTableHash;
    private final String logicalTableKey;
    private final String serviceOwnerId;
    private final String generationId;
    private final Purpose purpose;
    private final String ownerToken;

    private PhysicalTableWriterLease(
            String physicalTableHash,
            String logicalTableKey,
            String serviceOwnerId,
            String generationId,
            Purpose purpose) {
        this.physicalTableHash = requireText(physicalTableHash, "physicalTableHash");
        this.logicalTableKey = requireText(logicalTableKey, "logicalTableKey");
        this.serviceOwnerId = requireText(serviceOwnerId, "serviceOwnerId");
        this.generationId = requireText(generationId, "generationId");
        this.purpose = Objects.requireNonNull(purpose, "purpose");
        this.ownerToken =
                this.serviceOwnerId
                        + ':'
                        + this.logicalTableKey
                        + ':'
                        + this.generationId
                        + ':'
                        + this.purpose;
    }

    static PhysicalTableWriterLease newGeneration(
            String physicalTableHash,
            String logicalTableKey,
            String serviceOwnerId,
            Purpose purpose) {
        return of(
                physicalTableHash,
                logicalTableKey,
                serviceOwnerId,
                UUID.randomUUID().toString(),
                purpose);
    }

    static PhysicalTableWriterLease of(
            String physicalTableHash,
            String logicalTableKey,
            String serviceOwnerId,
            String generationId,
            Purpose purpose) {
        return new PhysicalTableWriterLease(
                physicalTableHash, logicalTableKey, serviceOwnerId, generationId, purpose);
    }

    static Registry jvmRegistry() {
        return JVM_REGISTRY;
    }

    String physicalTableHash() {
        return physicalTableHash;
    }

    String logicalTableKey() {
        return logicalTableKey;
    }

    String serviceOwnerId() {
        return serviceOwnerId;
    }

    String generationId() {
        return generationId;
    }

    Purpose purpose() {
        return purpose;
    }

    String ownerToken() {
        return ownerToken;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PhysicalTableWriterLease)) {
            return false;
        }
        PhysicalTableWriterLease that = (PhysicalTableWriterLease) other;
        return physicalTableHash.equals(that.physicalTableHash)
                && logicalTableKey.equals(that.logicalTableKey)
                && serviceOwnerId.equals(that.serviceOwnerId)
                && generationId.equals(that.generationId)
                && purpose == that.purpose;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalTableHash, logicalTableKey, serviceOwnerId, generationId, purpose);
    }

    @Override
    public String toString() {
        return "PhysicalTableWriterLease{"
                + "physicalTableHash='"
                + physicalTableHash
                + '\''
                + ", logicalTableKey='"
                + logicalTableKey
                + '\''
                + ", generationId='"
                + generationId
                + '\''
                + ", purpose="
                + purpose
                + '}';
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    /**
     * Exact active/retained lease registry.
     *
     * <p>There is intentionally no broad clear/remove-by-owner operation. A failed DDL action is
     * atomically converted in the same physical-table slot, so no acquire window exists between
     * active ownership and retained ownership.
     */
    static final class Registry {

        private final ConcurrentMap<String, Slot> leases = new ConcurrentHashMap<>();

        boolean tryAcquire(PhysicalTableWriterLease lease) {
            Objects.requireNonNull(lease, "lease");
            return leases.putIfAbsent(lease.physicalTableHash, Slot.active(lease)) == null;
        }

        boolean releaseActive(PhysicalTableWriterLease expected) {
            Objects.requireNonNull(expected, "expected");
            AtomicBoolean released = new AtomicBoolean();
            leases.computeIfPresent(
                    expected.physicalTableHash,
                    (ignored, current) -> {
                        if (current.retained == null && current.lease.equals(expected)) {
                            released.set(true);
                            return null;
                        }
                        return current;
                    });
            return released.get();
        }

        RetainedDdlActionLease retainAfterDdlFailure(
                PhysicalTableWriterLease expected, Throwable actionFailure) {
            Objects.requireNonNull(expected, "expected");
            Objects.requireNonNull(actionFailure, "actionFailure");
            AtomicReference<RetainedDdlActionLease> retained = new AtomicReference<>();
            leases.compute(
                    expected.physicalTableHash,
                    (ignored, current) -> {
                        if (current == null
                                || current.retained != null
                                || !current.lease.equals(expected)) {
                            return current;
                        }
                        RetainedDdlActionLease marker =
                                RetainedDdlActionLease.create(expected, actionFailure);
                        retained.set(marker);
                        return Slot.retained(marker);
                    });
            RetainedDdlActionLease marker = retained.get();
            if (marker == null) {
                throw new IllegalStateException(
                        "Cannot retain a DDL lease that is not the exact active generation");
            }
            return marker;
        }

        boolean releaseRetained(RetainedDdlActionLease expected) {
            Objects.requireNonNull(expected, "expected");
            AtomicBoolean released = new AtomicBoolean();
            leases.computeIfPresent(
                    expected.sourceLease().physicalTableHash,
                    (ignored, current) -> {
                        if (current.retained == expected) {
                            released.set(true);
                            return null;
                        }
                        return current;
                    });
            return released.get();
        }

        PhysicalTableWriterLease currentLease(String physicalTableHash) {
            Slot slot = leases.get(requireText(physicalTableHash, "physicalTableHash"));
            return slot == null ? null : slot.lease;
        }

        RetainedDdlActionLease currentRetained(String physicalTableHash) {
            Slot slot = leases.get(requireText(physicalTableHash, "physicalTableHash"));
            return slot == null ? null : slot.retained;
        }

        private static final class Slot {
            private final PhysicalTableWriterLease lease;
            private final RetainedDdlActionLease retained;

            private Slot(
                    PhysicalTableWriterLease lease, RetainedDdlActionLease retained) {
                this.lease = Objects.requireNonNull(lease, "lease");
                this.retained = retained;
            }

            private static Slot active(PhysicalTableWriterLease lease) {
                return new Slot(lease, null);
            }

            private static Slot retained(RetainedDdlActionLease retained) {
                return new Slot(retained.sourceLease(), retained);
            }
        }
    }
}
