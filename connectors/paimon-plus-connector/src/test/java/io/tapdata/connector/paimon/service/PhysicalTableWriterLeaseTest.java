package io.tapdata.connector.paimon.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PhysicalTableWriterLeaseTest {

    @Test
    void delayedReleaseFromOldGenerationCannotRemoveReplacementGeneration() {
        PhysicalTableWriterLease.Registry registry = new PhysicalTableWriterLease.Registry();
        PhysicalTableWriterLease first = writer("generation-1");
        PhysicalTableWriterLease second = writer("generation-2");

        assertTrue(registry.tryAcquire(first));
        assertTrue(registry.releaseActive(first));
        assertTrue(registry.tryAcquire(second));

        assertFalse(registry.releaseActive(first));
        assertSame(second, registry.currentLease("physical-hash"));
        assertTrue(registry.releaseActive(second));
    }

    @Test
    void generatedTokensAreGenerationScopedAndPurposeScoped() {
        PhysicalTableWriterLease writer =
                PhysicalTableWriterLease.newGeneration(
                        "physical-hash",
                        "database.table",
                        "service",
                        PhysicalTableWriterLease.Purpose.WRITER);
        PhysicalTableWriterLease ddl =
                PhysicalTableWriterLease.newGeneration(
                        "physical-hash",
                        "database.table",
                        "service",
                        PhysicalTableWriterLease.Purpose.DDL_ONLY);

        assertNotEquals(writer.generationId(), ddl.generationId());
        assertNotEquals(writer.ownerToken(), ddl.ownerToken());
        assertTrue(writer.ownerToken().endsWith(":WRITER"));
        assertTrue(ddl.ownerToken().endsWith(":DDL_ONLY"));
    }

    @Test
    void physicalIdentityConflictsEvenWhenLogicalKeysDiffer() {
        PhysicalTableWriterLease.Registry registry = new PhysicalTableWriterLease.Registry();
        PhysicalTableWriterLease owner = writer("generation-1");
        PhysicalTableWriterLease alias =
                PhysicalTableWriterLease.of(
                        "physical-hash",
                        "database.alias",
                        "another-service",
                        "generation-2",
                        PhysicalTableWriterLease.Purpose.WRITER);

        assertTrue(registry.tryAcquire(owner));
        assertFalse(registry.tryAcquire(alias));
        assertSame(owner, registry.currentLease("physical-hash"));
    }

    @Test
    void failedDdlAtomicallyRetainsWriterLeaseUntilExactMarkerRelease() {
        PhysicalTableWriterLease.Registry registry = new PhysicalTableWriterLease.Registry();
        PhysicalTableWriterLease writer = writer("generation-1");
        RuntimeException actionFailure = new RuntimeException("ddl failed");
        assertTrue(registry.tryAcquire(writer));

        RetainedDdlActionLease retained =
                registry.retainAfterDdlFailure(writer, actionFailure);

        assertSame(writer, retained.sourceLease());
        assertSame(actionFailure, retained.actionFailure());
        assertEquals(PhysicalTableWriterLease.Purpose.WRITER, retained.sourcePurpose());
        assertEquals(writer.ownerToken(), retained.sourceOwnerToken());
        assertSame(retained, registry.currentRetained("physical-hash"));
        assertFalse(registry.releaseActive(writer));
        assertFalse(registry.tryAcquire(writer("generation-2")));
        assertTrue(registry.releaseRetained(retained));
        assertFalse(registry.releaseRetained(retained));
        assertTrue(registry.tryAcquire(writer("generation-2")));
    }

    @Test
    void ddlOnlyLeaseUsesTheSameRetainedAndExactReleaseProtocol() {
        PhysicalTableWriterLease.Registry registry = new PhysicalTableWriterLease.Registry();
        PhysicalTableWriterLease ddl =
                PhysicalTableWriterLease.of(
                        "physical-hash",
                        "database.table",
                        "service",
                        "ddl-generation",
                        PhysicalTableWriterLease.Purpose.DDL_ONLY);
        assertTrue(registry.tryAcquire(ddl));

        RetainedDdlActionLease retained =
                registry.retainAfterDdlFailure(ddl, new RuntimeException("drop failed"));

        assertEquals(PhysicalTableWriterLease.Purpose.DDL_ONLY, retained.sourcePurpose());
        assertFalse(registry.releaseActive(ddl));
        assertTrue(registry.releaseRetained(retained));
    }

    @Test
    void retainingAnyLeaseOtherThanTheExactActiveGenerationFailsClosed() {
        PhysicalTableWriterLease.Registry registry = new PhysicalTableWriterLease.Registry();
        PhysicalTableWriterLease current = writer("generation-2");
        assertTrue(registry.tryAcquire(current));

        assertThrows(
                IllegalStateException.class,
                () ->
                        registry.retainAfterDdlFailure(
                                writer("generation-1"), new RuntimeException("stale")));
        assertSame(current, registry.currentLease("physical-hash"));
    }

    private static PhysicalTableWriterLease writer(String generationId) {
        return PhysicalTableWriterLease.of(
                "physical-hash",
                "database.table",
                "service",
                generationId,
                PhysicalTableWriterLease.Purpose.WRITER);
    }
}
