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
        private final InterruptedException interruption;

        private Outcome(
                Status status, Throwable failure, InterruptedException interruption) {
            this.status = status;
            this.failure = failure;
            this.interruption = interruption;
        }

        public Status status() {
            return status;
        }

        public Throwable failure() {
            return failure;
        }

        /** Exact interruption returned by Paimon for a WAITING join, otherwise {@code null}. */
        public InterruptedException interruption() {
            return interruption;
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
                if (outcome.failure() != null || outcome.interruption() != null) {
                    throw new IllegalStateException(
                            "Successful maintenance carried failure or interruption evidence.");
                }
                return new Outcome(Status.SUCCESS, null, null);
            case FAILED_DRAINED:
                if (outcome.failure() == null || outcome.interruption() != null) {
                    throw new IllegalStateException(
                            "FAILED_DRAINED maintenance carried inconsistent evidence.");
                }
                return new Outcome(Status.FAILED_DRAINED, outcome.failure(), null);
            case WAITING:
                if (outcome.failure() != null) {
                    throw new IllegalStateException("WAITING maintenance carried a failure.");
                }
                return new Outcome(Status.WAITING, null, outcome.interruption());
            default:
                throw new IllegalStateException(
                        "Unknown Paimon maintenance outcome: " + outcome.status());
        }
    }
}
