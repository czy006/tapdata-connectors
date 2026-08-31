package io.tapdata.connector.paimon.write;

import org.apache.paimon.table.sink.TableCommitMaintenance;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Connector-owned adapter for Paimon's opaque structured maintenance lifecycle. */
public final class PaimonMaintenanceAdapter {

    public enum Status {
        SUCCESS,
        FAILED_DRAINED,
        WAITING
    }

    public static final class Outcome {
        private final Status status;
        private final Throwable failure;

        private Outcome(Status status, Throwable failure) {
            this.status = status;
            this.failure = failure;
        }

        public Status status() {
            return status;
        }

        public Throwable failure() {
            return failure;
        }
    }

    private final TableCommitMaintenance delegate;

    public PaimonMaintenanceAdapter(TableCommitMaintenance delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /** Starts or rejoins the same graceful shutdown operation. */
    public Outcome shutdownAndAwait(long timeout, TimeUnit unit) {
        return map(delegate.shutdownAndAwait(timeout, Objects.requireNonNull(unit, "unit")));
    }

    /** Joins the same operation without a deadline. */
    public Outcome closeAndDrain() {
        return map(delegate.closeAndDrain());
    }

    private static Outcome map(TableCommitMaintenance.Outcome outcome) {
        Objects.requireNonNull(outcome, "maintenance outcome");
        switch (outcome.status()) {
            case SUCCESS:
                if (outcome.failure() != null) {
                    throw new IllegalStateException("Successful maintenance carried a failure.");
                }
                return new Outcome(Status.SUCCESS, null);
            case FAILED_DRAINED:
                if (outcome.failure() == null) {
                    throw new IllegalStateException("FAILED_DRAINED maintenance lost its failure.");
                }
                return new Outcome(Status.FAILED_DRAINED, outcome.failure());
            case WAITING:
                if (outcome.failure() != null) {
                    throw new IllegalStateException("WAITING maintenance carried a failure.");
                }
                return new Outcome(Status.WAITING, null);
            default:
                throw new IllegalStateException(
                        "Unknown Paimon maintenance outcome: " + outcome.status());
        }
    }
}
