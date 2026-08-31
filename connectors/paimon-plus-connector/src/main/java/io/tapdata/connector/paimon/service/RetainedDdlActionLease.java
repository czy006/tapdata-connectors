package io.tapdata.connector.paimon.service;

import java.util.Objects;
import java.util.UUID;

/** Strong retained marker for a WRITER or DDL_ONLY lease after a DDL action has failed. */
final class RetainedDdlActionLease {

    private final String retentionId;
    private final PhysicalTableWriterLease sourceLease;
    private final Throwable actionFailure;

    private RetainedDdlActionLease(
            String retentionId,
            PhysicalTableWriterLease sourceLease,
            Throwable actionFailure) {
        this.retentionId = Objects.requireNonNull(retentionId, "retentionId");
        this.sourceLease = Objects.requireNonNull(sourceLease, "sourceLease");
        this.actionFailure = Objects.requireNonNull(actionFailure, "actionFailure");
    }

    static RetainedDdlActionLease create(
            PhysicalTableWriterLease sourceLease, Throwable actionFailure) {
        return new RetainedDdlActionLease(
                UUID.randomUUID().toString(), sourceLease, actionFailure);
    }

    String retentionId() {
        return retentionId;
    }

    PhysicalTableWriterLease sourceLease() {
        return sourceLease;
    }

    Throwable actionFailure() {
        return actionFailure;
    }

    PhysicalTableWriterLease.Purpose sourcePurpose() {
        return sourceLease.purpose();
    }

    String sourceOwnerToken() {
        return sourceLease.ownerToken();
    }
}
