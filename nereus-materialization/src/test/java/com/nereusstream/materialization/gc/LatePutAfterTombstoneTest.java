/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LatePutAfterTombstoneTest {
    @Test
    void lostLateDeleteResponseConvergesOnExactHeadAbsenceButKeepsTheRoot() {
        PhysicalRootTombstoneRetirementTest support =
                new PhysicalRootTombstoneRetirementTest();
        try {
            PhysicalRootTombstoneRetirementTest.Fixture fixture =
                    support.fixture(false);
            fixture.coordinator().retire(fixture.deletedRoot()).join();
            var observed = fixture.currentRoot();
            fixture.objectStore().exists = true;
            fixture.objectStore().loseDeleteResponse = true;
            fixture.clock().setMillis(7_000);

            TombstoneRetirementResult result =
                    fixture.coordinator().retire(observed).join();

            assertThat(result.status())
                    .isEqualTo(TombstoneRetirementStatus.OBJECT_PRESENT);
            assertThat(fixture.objectStore().deleteCalls).hasValue(1);
            assertThat(fixture.objectStore().exists).isFalse();
            assertThat(fixture.currentRoot().value().tombstoneFirstAbsentAtMillis())
                    .isZero();
        } finally {
            support.closeScheduler();
        }
    }

    @Test
    void ownerAppearingBeforeProviderDeleteInvalidatesTheLatePutAttempt() {
        PhysicalRootTombstoneRetirementTest support =
                new PhysicalRootTombstoneRetirementTest();
        try {
            PhysicalRootTombstoneRetirementTest.Fixture fixture =
                    support.fixture(false);
            fixture.coordinator().retire(fixture.deletedRoot()).join();
            var observed = fixture.currentRoot();
            fixture.objectStore().exists = true;
            fixture.domain().becomeOwnerBeforeRevalidation = true;
            fixture.clock().setMillis(7_000);

            TombstoneRetirementResult result =
                    fixture.coordinator().retire(observed).join();

            assertThat(result.status())
                    .isEqualTo(TombstoneRetirementStatus.DOMAIN_VETO);
            assertThat(fixture.objectStore().deleteCalls).hasValue(0);
            assertThat(fixture.objectStore().exists).isTrue();
            assertThat(fixture.currentRoot().value().tombstoneFirstAbsentAtMillis())
                    .isZero();
        } finally {
            support.closeScheduler();
        }
    }
}
