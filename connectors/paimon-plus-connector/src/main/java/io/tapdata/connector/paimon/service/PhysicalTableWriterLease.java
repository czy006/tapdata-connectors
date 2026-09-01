package io.tapdata.connector.paimon.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

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

        /*
         * One monitor makes the per-Service snapshot a real linearization point. A weakly
         * consistent ConcurrentHashMap.values() traversal is insufficient for the global-close
         * barrier because it could miss an acquire racing on another physical-table key.
         * Coordinator code must never enter this monitor while holding its admission lock.
         */
        private final Object registryLock = new Object();
        private final Map<String, Slot> leases = new HashMap<>();
        private final TransitionObserver transitionObserver;

        Registry() {
            this(TransitionObserver.NOOP);
        }

        Registry(TransitionObserver transitionObserver) {
            this.transitionObserver =
                    Objects.requireNonNull(transitionObserver, "transitionObserver");
        }

        boolean tryAcquire(PhysicalTableWriterLease lease) {
            Objects.requireNonNull(lease, "lease");
            synchronized (registryLock) {
                if (leases.containsKey(lease.physicalTableHash)) {
                    return false;
                }
                leases.put(lease.physicalTableHash, Slot.active(lease));
                return true;
            }
        }

        boolean releaseActive(PhysicalTableWriterLease expected) {
            Objects.requireNonNull(expected, "expected");
            boolean released;
            synchronized (registryLock) {
                Slot current = leases.get(expected.physicalTableHash);
                if (current == null
                        || current.retained != null
                        || current.releaseInProgress
                        || current.lease != expected) {
                    released = false;
                } else {
                    // Keep the slot occupied while result publication is in flight. No other
                    // generation may acquire the physical table before release is fully known.
                    leases.put(expected.physicalTableHash, Slot.releasing(expected));
                    released = true;
                }
            }
            try {
                transitionObserver.afterReleaseActive(expected, released);
            } catch (RuntimeException | Error transitionFailure) {
                if (released) {
                    synchronized (registryLock) {
                        Slot current = leases.get(expected.physicalTableHash);
                        if (current != null
                                && current.lease == expected
                                && current.releaseInProgress) {
                            leases.put(expected.physicalTableHash, Slot.active(expected));
                        }
                    }
                }
                throw new RegistryTransitionException(null, transitionFailure);
            }
            if (released) {
                synchronized (registryLock) {
                    Slot current = leases.get(expected.physicalTableHash);
                    if (current == null
                            || current.lease != expected
                            || !current.releaseInProgress) {
                        throw new IllegalStateException(
                                "Physical lease release reservation was lost");
                    }
                    leases.remove(expected.physicalTableHash);
                }
            }
            return released;
        }

        RetainedDdlActionLease retainAfterDdlFailure(
                PhysicalTableWriterLease expected, Throwable actionFailure) {
            Objects.requireNonNull(expected, "expected");
            Objects.requireNonNull(actionFailure, "actionFailure");
            RetainedDdlActionLease marker;
            synchronized (registryLock) {
                Slot current = leases.get(expected.physicalTableHash);
                if (current == null
                        || current.retained != null
                        || current.releaseInProgress
                        || current.lease != expected) {
                    throw new IllegalStateException(
                            "Cannot retain a DDL lease that is not the exact active generation");
                }
                marker =
                        RetainedDdlActionLease.create(expected, actionFailure);
                leases.put(expected.physicalTableHash, Slot.retained(marker));
            }
            try {
                transitionObserver.afterRetainDdl(expected, marker);
            } catch (RuntimeException | Error transitionFailure) {
                throw new RegistryTransitionException(marker, transitionFailure);
            }
            return marker;
        }

        boolean releaseRetained(RetainedDdlActionLease expected) {
            Objects.requireNonNull(expected, "expected");
            synchronized (registryLock) {
                String physicalTableHash = expected.sourceLease().physicalTableHash;
                Slot current = leases.get(physicalTableHash);
                if (current == null || current.retained != expected) {
                    return false;
                }
                leases.remove(physicalTableHash);
                return true;
            }
        }

        PhysicalTableWriterLease currentLease(String physicalTableHash) {
            synchronized (registryLock) {
                Slot slot = leases.get(requireText(physicalTableHash, "physicalTableHash"));
                return slot == null ? null : slot.lease;
            }
        }

        RetainedDdlActionLease currentRetained(String physicalTableHash) {
            synchronized (registryLock) {
                Slot slot = leases.get(requireText(physicalTableHash, "physicalTableHash"));
                return slot == null ? null : slot.retained;
            }
        }

        ServiceLeaseSnapshot snapshotOwnedBy(String serviceOwnerId) {
            String expectedOwner = requireText(serviceOwnerId, "serviceOwnerId");
            int active = 0;
            int retained = 0;
            synchronized (registryLock) {
                for (Slot slot : leases.values()) {
                    if (!slot.lease.serviceOwnerId.equals(expectedOwner)) {
                        continue;
                    }
                    if (slot.retained == null) {
                        active++;
                    } else {
                        retained++;
                    }
                }
            }
            return new ServiceLeaseSnapshot(active, retained);
        }

        static final class ServiceLeaseSnapshot {
            private final int activeCount;
            private final int retainedCount;

            private ServiceLeaseSnapshot(int activeCount, int retainedCount) {
                this.activeCount = activeCount;
                this.retainedCount = retainedCount;
            }

            int activeCount() {
                return activeCount;
            }

            int retainedCount() {
                return retainedCount;
            }

            int totalCount() {
                return activeCount + retainedCount;
            }
        }

        interface TransitionObserver {
            TransitionObserver NOOP = new TransitionObserver() {};

            default void afterReleaseActive(
                    PhysicalTableWriterLease expected, boolean released) {}

            default void afterRetainDdl(
                    PhysicalTableWriterLease expected, RetainedDdlActionLease retained) {}
        }

        /** Reports retained publication failure or a release publication that was rolled back. */
        static final class RegistryTransitionException extends RuntimeException {
            private final RetainedDdlActionLease retainedLease;

            private RegistryTransitionException(
                    RetainedDdlActionLease retainedLease, Throwable transitionFailure) {
                super("Physical lease registry transitioned but result publication failed",
                        transitionFailure);
                this.retainedLease = retainedLease;
            }

            Throwable transitionFailure() {
                return getCause();
            }

            RetainedDdlActionLease retainedLease() {
                return retainedLease;
            }
        }

        private static final class Slot {
            private final PhysicalTableWriterLease lease;
            private final RetainedDdlActionLease retained;
            private final boolean releaseInProgress;

            private Slot(
                    PhysicalTableWriterLease lease,
                    RetainedDdlActionLease retained,
                    boolean releaseInProgress) {
                this.lease = Objects.requireNonNull(lease, "lease");
                this.retained = retained;
                this.releaseInProgress = releaseInProgress;
            }

            private static Slot active(PhysicalTableWriterLease lease) {
                return new Slot(lease, null, false);
            }

            private static Slot releasing(PhysicalTableWriterLease lease) {
                return new Slot(lease, null, true);
            }

            private static Slot retained(RetainedDdlActionLease retained) {
                return new Slot(retained.sourceLease(), retained, false);
            }
        }
    }
}
