/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.pulsar.offload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Capabilities;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PulsarOffloadP0ContractTest {
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    @Test
    void derivesTheExactAttemptScopedDataAndRootKeys() {
        PulsarOffloadKeysV1 keys = PulsarOffloadKeysV1.derive("cells/pulsar-a", 42, ATTEMPT);

        assertThat(keys.attemptPrefix())
                .isEqualTo("cells/pulsar-a/pulsar-offload/v1/ledger-42/attempt-123e4567-e89b-12d3-a456-426614174000");
        assertThat(keys.dataKey()).isEqualTo(keys.attemptPrefix() + "/data");
        assertThat(keys.rootKey()).isEqualTo(keys.attemptPrefix() + "/root");
    }

    @Test
    void rejectsNonCanonicalScopeAndLedgerIdentity() {
        for (String scope : List.of("", "/cell", "cell/", "cell//x", "cell/../x", "cell\\x")) {
            assertThatThrownBy(() -> PulsarOffloadKeysV1.derive(scope, 1, ATTEMPT))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> PulsarOffloadKeysV1.derive("cell", -1, ATTEMPT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void keepsAdrNumbersAsCandidatesRatherThanSelectedDefaults() {
        PulsarOffloadLimitCandidateV1 candidate = PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();

        assertThat(candidate.maxDataObjectBytes()).isEqualTo(4L * 1_024 * 1_024 * 1_024);
        assertThat(candidate.maxMultipartParts()).isEqualTo(1_024);
        assertThat(candidate.blockTargetBytes()).containsExactly(1_048_576, 4_194_304, 8_388_608, 16_777_216);
    }

    @Test
    void rejectsAChangedBlockCandidateSetOrInconsistentCap() {
        assertThatThrownBy(() -> new PulsarOffloadLimitCandidateV1(100, 1, 10, 10, 1, List.of(1, 2, 3, 4)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PulsarOffloadLimitCandidateV1(
                        100,
                        1,
                        2L * PulsarOffloadLimitCandidateV1.MIB,
                        8L * PulsarOffloadLimitCandidateV1.MIB,
                        1,
                        List.of(
                                PulsarOffloadLimitCandidateV1.MIB,
                                4 * PulsarOffloadLimitCandidateV1.MIB,
                                8 * PulsarOffloadLimitCandidateV1.MIB,
                                16 * PulsarOffloadLimitCandidateV1.MIB)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void admitsOnlyAProviderThatCoversEveryMandatoryCapabilityAndCandidateBound() {
        PulsarOffloadLimitCandidateV1 candidate = PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();
        Capabilities admitted = new Capabilities(
                candidate.maxDataObjectBytes(),
                5L * PulsarOffloadLimitCandidateV1.MIB,
                64L * PulsarOffloadLimitCandidateV1.MIB,
                candidate.maxMultipartParts(),
                true,
                true,
                true,
                true,
                true);

        PulsarOffloadProfileAdmissionV1.requireAdmitted(candidate, admitted);
    }

    @Test
    void rejectsMissingProviderCapabilityObjectCapacityAndMultipartCapacity() {
        PulsarOffloadLimitCandidateV1 candidate = PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();
        Capabilities missingStreaming = new Capabilities(
                candidate.maxDataObjectBytes(),
                1,
                candidate.maxDataObjectBytes(),
                1_024,
                true,
                false,
                true,
                true,
                true);
        Capabilities shortObject = new Capabilities(1_024, 1, 1_024, 1_024, true, true, true, true, true);
        Capabilities shortMultipart =
                new Capabilities(candidate.maxDataObjectBytes(), 1, 1_024, 1_024, true, true, true, true, true);

        assertThatThrownBy(() -> PulsarOffloadProfileAdmissionV1.requireAdmitted(candidate, missingStreaming))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PulsarOffloadProfileAdmissionV1.requireAdmitted(candidate, shortObject))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PulsarOffloadProfileAdmissionV1.requireAdmitted(candidate, shortMultipart))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validatesOneNonEmptySealedLedgerAndItsNativeAuthorityFacts() {
        PulsarSealedLedgerAttemptV1 attempt = attempt(RetentionClass.DELETE_AFTER_VERIFIED);

        assertThat(attempt.entryCount()).isEqualTo(10);
        assertThat(attempt.keys().dataKey()).endsWith("/data");
        assertThat(attempt.bookkeeperDeleted()).isFalse();
    }

    @Test
    void rejectsEmptyInconsistentAndNegativeSealedLedgerFacts() {
        assertThatThrownBy(() -> new PulsarSealedLedgerAttemptV1(
                        1,
                        ATTEMPT,
                        -1,
                        0,
                        0,
                        0,
                        0,
                        "cell",
                        RetentionClass.RETAIN_BK,
                        DeleteState.BK_DELETE_NONE,
                        false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PulsarSealedLedgerAttemptV1(
                        1,
                        ATTEMPT,
                        9,
                        9,
                        10,
                        0,
                        0,
                        "cell",
                        RetentionClass.RETAIN_BK,
                        DeleteState.BK_DELETE_NONE,
                        false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void enforcesCompatibilityBooleanAndRetentionClassAgainstDeleteState() {
        assertThatThrownBy(() -> new PulsarSealedLedgerAttemptV1(
                        1,
                        ATTEMPT,
                        9,
                        10,
                        100,
                        0,
                        0,
                        "cell",
                        RetentionClass.DELETE_AFTER_VERIFIED,
                        DeleteState.BK_DELETE_INTENT,
                        false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PulsarSealedLedgerAttemptV1(
                        1,
                        ATTEMPT,
                        9,
                        10,
                        100,
                        0,
                        0,
                        "cell",
                        RetentionClass.RETAIN_BK,
                        DeleteState.BK_DELETE_INTENT,
                        true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void advancesDeleteStateWithoutSkipOrRollbackAndBumpsMetadataVersion() {
        PulsarSealedLedgerAttemptV1 none = attempt(RetentionClass.DELETE_AFTER_VERIFIED);
        PulsarSealedLedgerAttemptV1 intent = none.transitionDeleteState(DeleteState.BK_DELETE_INTENT);
        PulsarSealedLedgerAttemptV1 done = intent.transitionDeleteState(DeleteState.BK_DELETE_DONE);

        assertThat(intent.bookkeeperDeleted()).isTrue();
        assertThat(done.deleteState()).isEqualTo(DeleteState.BK_DELETE_DONE);
        assertThat(done.metadataVersion()).isEqualTo(none.metadataVersion() + 2);
        assertThatThrownBy(() -> none.transitionDeleteState(DeleteState.BK_DELETE_DONE))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> done.transitionDeleteState(DeleteState.BK_DELETE_INTENT))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void retainBkNeverCreatesDeleteIntent() {
        PulsarSealedLedgerAttemptV1 retained = attempt(RetentionClass.RETAIN_BK);

        assertThatThrownBy(() -> retained.transitionDeleteState(DeleteState.BK_DELETE_INTENT))
                .isInstanceOf(IllegalStateException.class);
    }

    private static PulsarSealedLedgerAttemptV1 attempt(RetentionClass retentionClass) {
        return new PulsarSealedLedgerAttemptV1(
                42, ATTEMPT, 9, 10, 1_024, 100, 7, "cells/pulsar-a", retentionClass, DeleteState.BK_DELETE_NONE, false);
    }
}
