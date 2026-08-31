package io.tapdata.connector.paimon.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Linearizes service read admission, per-table DDL read fences, and operation joins.
 *
 * <p>The admission lock protects only coordinator state. It is never held while waiting, closing a
 * resource, invoking Paimon, or calling user code. A caller publishes a service/table fence and
 * obtains an immutable operation snapshot under the short critical section, then requests stop and
 * joins that snapshot after the lock has been released.
 *
 * <p>Canonical lock order is: service ingress, per-table commit lock, and then a short coordinator
 * state check. A caller must release every per-table lock before joining an operation. This class
 * never acquires a per-table, operation, physical-lease, Catalog, or FileIO lock.
 */
final class PaimonServiceResourceCoordinator {

    enum Completion {
        CLOSED_SUCCESS,
        RETAINED
    }

    private final Object admissionLock = new Object();
    private final Map<ReadIdentity, ReadAdmission> activeReads = new LinkedHashMap<>();
    private final Map<ReadIdentity, ReadAdmission> retainedReads = new LinkedHashMap<>();
    private final Map<String, LinkedHashMap<Long, ReadBorrow>> activeBorrows =
            new LinkedHashMap<>();
    private final Map<Long, ReadBorrow> retainedBorrows = new LinkedHashMap<>();
    private final Map<String, TableReadFence> tableReadFences = new LinkedHashMap<>();
    private long nextRegistrationId;
    private long nextBorrowId;
    private long nextFenceId;
    private ServiceReadFence serviceReadFence;

    ReadAdmission admitRead(
            String readOperationId, String serviceOwnerId, Collection<String> requestedTableKeys) {
        requireNonBlank(readOperationId, "Read operation id");
        requireNonBlank(serviceOwnerId, "Service owner id");
        List<String> tableKeys = normalizeTableKeys(requestedTableKeys);
        ReadIdentity identity = new ReadIdentity(readOperationId, serviceOwnerId);

        synchronized (admissionLock) {
            if (serviceReadFence != null) {
                throw new AdmissionRejectedException(
                        "Paimon service read admission is fenced for " + identity);
            }
            if (activeReads.containsKey(identity) || retainedReads.containsKey(identity)) {
                throw new AdmissionRejectedException(
                        "Paimon read operation identity is already registered: " + identity);
            }
            for (String tableKey : tableKeys) {
                if (tableReadFences.containsKey(tableKey)) {
                    throw new AdmissionRejectedException(
                            "Paimon table read admission is fenced for " + tableKey);
                }
            }

            long registrationId = ++nextRegistrationId;
            ReadAdmission admission =
                    new ReadAdmission(this, registrationId, identity, tableKeys);
            activeReads.put(identity, admission);
            for (ReadBorrow borrow : admission.borrows.values()) {
                activeBorrows
                        .computeIfAbsent(borrow.tableKey, ignored -> new LinkedHashMap<>())
                        .put(registrationId, borrow);
            }
            return admission;
        }
    }

    ServiceReadFence beginServiceReadFence() {
        synchronized (admissionLock) {
            if (serviceReadFence == null) {
                serviceReadFence =
                        new ServiceReadFence(
                                this,
                                ++nextFenceId,
                                new ArrayList<>(activeReads.values()));
            }
            return serviceReadFence;
        }
    }

    TableReadFence beginTableDdlFence(String tableKey) {
        requireNonBlank(tableKey, "Table key");
        synchronized (admissionLock) {
            TableReadFence existing = tableReadFences.get(tableKey);
            if (existing != null) {
                return existing;
            }
            if (serviceReadFence != null) {
                throw new AdmissionRejectedException(
                        "Paimon service read admission is already fenced");
            }
            Map<Long, ReadBorrow> borrowers = activeBorrows.get(tableKey);
            List<ReadBorrow> snapshot =
                    borrowers == null
                            ? Collections.emptyList()
                            : new ArrayList<>(borrowers.values());
            TableReadFence fence =
                    new TableReadFence(this, ++nextFenceId, tableKey, snapshot);
            tableReadFences.put(tableKey, fence);
            return fence;
        }
    }

    void releaseTableDdlFence(TableReadFence expectedFence) {
        if (expectedFence == null) {
            throw new IllegalArgumentException("Expected table read fence must not be null");
        }
        if (expectedFence.coordinator != this) {
            throw new IllegalArgumentException("Table read fence belongs to another coordinator");
        }
        synchronized (admissionLock) {
            TableReadFence actual = tableReadFences.get(expectedFence.tableKey);
            if (actual != expectedFence) {
                throw new IllegalStateException(
                        "Table read fence identity no longer matches " + expectedFence.tableKey);
            }
            if (activeBorrowCountLocked(expectedFence.tableKey) != 0) {
                throw new IllegalStateException(
                        "Active read borrowers still exist for " + expectedFence.tableKey);
            }
            if (hasRetainedBorrowLocked(expectedFence.tableKey)
                    || !allClosedSuccessfully(expectedFence.operations())) {
                throw new IllegalStateException(
                        "Retained read borrowers prevent releasing the fence for "
                                + expectedFence.tableKey);
            }
            tableReadFences.remove(expectedFence.tableKey);
        }
    }

    int activeReadCount() {
        synchronized (admissionLock) {
            return activeReads.size();
        }
    }

    int activeBorrowCount(String tableKey) {
        requireNonBlank(tableKey, "Table key");
        synchronized (admissionLock) {
            return activeBorrowCountLocked(tableKey);
        }
    }

    int retainedReadCount() {
        synchronized (admissionLock) {
            return retainedReads.size();
        }
    }

    int retainedBorrowCount() {
        synchronized (admissionLock) {
            return retainedBorrows.size();
        }
    }

    boolean canCloseGlobalResources() {
        synchronized (admissionLock) {
            return serviceReadFence != null
                    && activeReads.isEmpty()
                    && activeBorrows.isEmpty()
                    && retainedReads.isEmpty()
                    && retainedBorrows.isEmpty()
                    && tableReadFences.isEmpty();
        }
    }

    private boolean completeBorrow(ReadBorrow expectedBorrow, Completion completion) {
        requireCompletion(completion);
        synchronized (admissionLock) {
            if (expectedBorrow.coordinator != this) {
                throw new IllegalArgumentException("Read borrower belongs to another coordinator");
            }
            if (expectedBorrow.completion() != null) {
                if (expectedBorrow.completion() != completion) {
                    throw new IllegalStateException(
                            "Read borrower already completed as " + expectedBorrow.completion());
                }
                return false;
            }
            ReadAdmission parent = activeReads.get(expectedBorrow.parent.identity);
            if (parent != expectedBorrow.parent) {
                throw new IllegalStateException("Read borrower parent is no longer active");
            }
            Map<Long, ReadBorrow> tableBorrowers = activeBorrows.get(expectedBorrow.tableKey);
            if (tableBorrowers == null
                    || tableBorrowers.get(expectedBorrow.parent.registrationId)
                            != expectedBorrow) {
                throw new IllegalStateException(
                        "Read borrower identity no longer matches " + expectedBorrow.tableKey);
            }

            tableBorrowers.remove(expectedBorrow.parent.registrationId);
            if (tableBorrowers.isEmpty()) {
                activeBorrows.remove(expectedBorrow.tableKey);
            }
            expectedBorrow.publishCompletion(completion);
            if (completion == Completion.RETAINED) {
                retainedBorrows.put(expectedBorrow.borrowId, expectedBorrow);
            }
        }
        expectedBorrow.signalCompletion();
        return true;
    }

    private boolean completeRead(ReadAdmission expectedAdmission, Completion completion) {
        requireCompletion(completion);
        synchronized (admissionLock) {
            if (expectedAdmission.coordinator != this) {
                throw new IllegalArgumentException("Read admission belongs to another coordinator");
            }
            if (expectedAdmission.completion() != null) {
                if (expectedAdmission.completion() != completion) {
                    throw new IllegalStateException(
                            "Read admission already completed as "
                                    + expectedAdmission.completion());
                }
                return false;
            }
            if (activeReads.get(expectedAdmission.identity) != expectedAdmission) {
                throw new IllegalStateException("Read admission identity is no longer active");
            }
            boolean hasRetainedChild = false;
            for (ReadBorrow borrow : expectedAdmission.borrows.values()) {
                if (borrow.completion() == null) {
                    throw new IllegalStateException(
                            "Read parent cannot complete before child " + borrow.tableKey);
                }
                hasRetainedChild |= borrow.completion() == Completion.RETAINED;
            }
            if (completion == Completion.CLOSED_SUCCESS && hasRetainedChild) {
                throw new IllegalStateException(
                        "Read parent cannot publish success with a retained child");
            }

            activeReads.remove(expectedAdmission.identity);
            expectedAdmission.publishCompletion(completion);
            if (completion == Completion.RETAINED) {
                retainedReads.put(expectedAdmission.identity, expectedAdmission);
            }
        }
        expectedAdmission.signalCompletion();
        return true;
    }

    private void requireOutsideAdmissionLock(String operation) {
        if (Thread.holdsLock(admissionLock)) {
            throw new IllegalStateException(
                    operation + " must execute after releasing the coordinator admission lock");
        }
    }

    private int activeBorrowCountLocked(String tableKey) {
        Map<Long, ReadBorrow> borrowers = activeBorrows.get(tableKey);
        return borrowers == null ? 0 : borrowers.size();
    }

    private boolean hasRetainedBorrowLocked(String tableKey) {
        for (ReadBorrow borrow : retainedBorrows.values()) {
            if (borrow.tableKey.equals(tableKey)) {
                return true;
            }
        }
        return false;
    }

    private static boolean allClosedSuccessfully(List<? extends JoinableOperation> operations) {
        for (JoinableOperation operation : operations) {
            if (operation.completion() != Completion.CLOSED_SUCCESS) {
                return false;
            }
        }
        return true;
    }

    private static List<String> normalizeTableKeys(Collection<String> requestedTableKeys) {
        if (requestedTableKeys == null || requestedTableKeys.isEmpty()) {
            throw new IllegalArgumentException("Requested table keys must not be empty");
        }
        Set<String> normalized = new TreeSet<>();
        for (String tableKey : requestedTableKeys) {
            requireNonBlank(tableKey, "Table key");
            normalized.add(tableKey);
        }
        return Collections.unmodifiableList(new ArrayList<>(normalized));
    }

    private static void requireCompletion(Completion completion) {
        if (completion == null) {
            throw new IllegalArgumentException("Read completion must not be null");
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    static final class ReadAdmission extends JoinableOperation {
        private final PaimonServiceResourceCoordinator coordinator;
        private final long registrationId;
        private final ReadIdentity identity;
        private final List<String> tableKeys;
        private final Map<String, ReadBorrow> borrows;

        private ReadAdmission(
                PaimonServiceResourceCoordinator coordinator,
                long registrationId,
                ReadIdentity identity,
                List<String> tableKeys) {
            super(coordinator);
            this.coordinator = coordinator;
            this.registrationId = registrationId;
            this.identity = identity;
            this.tableKeys = tableKeys;
            Map<String, ReadBorrow> mutableBorrows = new LinkedHashMap<>();
            for (String tableKey : tableKeys) {
                mutableBorrows.put(
                        tableKey,
                        new ReadBorrow(
                                coordinator, this, ++coordinator.nextBorrowId, tableKey));
            }
            this.borrows = Collections.unmodifiableMap(mutableBorrows);
        }

        List<String> tableKeys() {
            return tableKeys;
        }

        ReadBorrow borrow(String tableKey) {
            requireNonBlank(tableKey, "Table key");
            ReadBorrow borrow = borrows.get(tableKey);
            if (borrow == null) {
                throw new IllegalArgumentException(
                        "Read admission does not borrow table " + tableKey);
            }
            return borrow;
        }

        @Override
        void requestStop() {
            coordinator.requireOutsideAdmissionLock("Read stop request");
            super.requestStop();
            for (ReadBorrow borrow : borrows.values()) {
                borrow.requestStop();
            }
        }

        boolean complete(Completion completion) {
            return coordinator.completeRead(this, completion);
        }
    }

    static final class ReadBorrow extends JoinableOperation {
        private final PaimonServiceResourceCoordinator coordinator;
        private final ReadAdmission parent;
        private final long borrowId;
        private final String tableKey;

        private ReadBorrow(
                PaimonServiceResourceCoordinator coordinator,
                ReadAdmission parent,
                long borrowId,
                String tableKey) {
            super(coordinator);
            this.coordinator = coordinator;
            this.parent = parent;
            this.borrowId = borrowId;
            this.tableKey = tableKey;
        }

        boolean complete(Completion completion) {
            return coordinator.completeBorrow(this, completion);
        }
    }

    static final class ServiceReadFence extends OperationSnapshot<ReadAdmission> {
        private ServiceReadFence(
                PaimonServiceResourceCoordinator coordinator,
                long fenceId,
                List<ReadAdmission> operations) {
            super(coordinator, fenceId, operations);
        }
    }

    static final class TableReadFence extends OperationSnapshot<ReadBorrow> {
        private final PaimonServiceResourceCoordinator coordinator;
        private final String tableKey;

        private TableReadFence(
                PaimonServiceResourceCoordinator coordinator,
                long fenceId,
                String tableKey,
                List<ReadBorrow> operations) {
            super(coordinator, fenceId, operations);
            this.coordinator = coordinator;
            this.tableKey = tableKey;
        }
    }

    abstract static class OperationSnapshot<T extends JoinableOperation> {
        private final PaimonServiceResourceCoordinator coordinator;
        private final long fenceId;
        private final List<T> operations;

        private OperationSnapshot(
                PaimonServiceResourceCoordinator coordinator,
                long fenceId,
                List<T> operations) {
            this.coordinator = coordinator;
            this.fenceId = fenceId;
            this.operations =
                    Collections.unmodifiableList(new ArrayList<>(operations));
        }

        void requestStop() {
            coordinator.requireOutsideAdmissionLock("Read stop request");
            for (T operation : operations) {
                operation.requestStop();
            }
        }

        void awaitCompletion() throws InterruptedException {
            coordinator.requireOutsideAdmissionLock("Read operation join");
            for (T operation : operations) {
                operation.awaitCompletion();
            }
        }

        int operationCount() {
            return operations.size();
        }

        boolean allClosedSuccessfully() {
            return PaimonServiceResourceCoordinator.allClosedSuccessfully(operations);
        }

        final List<T> operations() {
            return operations;
        }

        long fenceId() {
            return fenceId;
        }
    }

    abstract static class JoinableOperation {
        private final PaimonServiceResourceCoordinator coordinator;
        private final AtomicBoolean stopRequested = new AtomicBoolean();
        private final CountDownLatch completionLatch = new CountDownLatch(1);
        private volatile Completion completion;

        private JoinableOperation(PaimonServiceResourceCoordinator coordinator) {
            this.coordinator = coordinator;
        }

        void requestStop() {
            coordinator.requireOutsideAdmissionLock("Read stop request");
            stopRequested.set(true);
        }

        boolean stopRequested() {
            return stopRequested.get();
        }

        void awaitCompletion() throws InterruptedException {
            coordinator.requireOutsideAdmissionLock("Read operation join");
            completionLatch.await();
        }

        Completion completion() {
            return completion;
        }

        final void publishCompletion(Completion completion) {
            this.completion = completion;
        }

        final void signalCompletion() {
            completionLatch.countDown();
        }
    }

    static final class AdmissionRejectedException extends IllegalStateException {
        private AdmissionRejectedException(String message) {
            super(message);
        }
    }

    private static final class ReadIdentity {
        private final String readOperationId;
        private final String serviceOwnerId;

        private ReadIdentity(String readOperationId, String serviceOwnerId) {
            this.readOperationId = readOperationId;
            this.serviceOwnerId = serviceOwnerId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ReadIdentity)) {
                return false;
            }
            ReadIdentity that = (ReadIdentity) other;
            return readOperationId.equals(that.readOperationId)
                    && serviceOwnerId.equals(that.serviceOwnerId);
        }

        @Override
        public int hashCode() {
            int result = readOperationId.hashCode();
            return 31 * result + serviceOwnerId.hashCode();
        }

        @Override
        public String toString() {
            return serviceOwnerId + "/" + readOperationId;
        }
    }
}
