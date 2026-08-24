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

package com.nereusstream.storage.object.extent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

class CheckedExtentAccountingTest {
    @Test
    void localD1CapsUseCheckedOffsetsCountsAndDecodedTotals() {
        CheckedExtentAccounting accounting =
                new CheckedExtentAccounting(new ObjectWalAdmissionCaps(1024, 512, 256, 2048));
        accounting.chargeFixedBodyBytes(256);
        accounting.chargeContext(16);
        accounting.chargeAppendUnit(32);
        accounting.chargeFrame(24, 100, 200);

        assertThat(accounting.snapshot()).isEqualTo(new CheckedExtentAccounting.Snapshot(1, 1, 1, 72, 356, 200));
        assertThat(accounting.checkedDirectoryPrefixEnd(256, 100)).isEqualTo(356);
        assertThat(accounting.checkedFrameEnd(356, 100)).isEqualTo(456);
    }

    @Test
    void formatProviderAndArithmeticOverflowFailBeforeAllocation() {
        assertThatThrownBy(() -> new ObjectWalAdmissionCaps(ObjectWalFormatCaps.MAX_BODY_BYTES + 1, 1024, 1024, 1024))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("body admission cap");
        assertThatThrownBy(() -> CheckedExtentAccounting.checkedEnd(Long.MAX_VALUE, 1, Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);

        CheckedExtentAccounting accounting = new CheckedExtentAccounting(new ObjectWalAdmissionCaps(512, 256, 32, 100));
        assertThatThrownBy(() -> accounting.chargeFrame(1, 1, 101))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("decoded frame total");
    }

    @Test
    void frameAndDirectoryFormatHardCapsAreClosed() {
        CheckedExtentAccounting accounting = new CheckedExtentAccounting(new ObjectWalAdmissionCaps(
                ObjectWalFormatCaps.MAX_BODY_BYTES,
                ObjectWalFormatCaps.MAX_DIRECTORY_PREFIX_BYTES,
                ObjectWalFormatCaps.MAX_DIRECTORY_PLAINTEXT_BYTES,
                ObjectWalFormatCaps.MAX_TOTAL_DECODED_BYTES));
        assertThatThrownBy(() -> accounting.chargeFrame(1, ObjectWalFormatCaps.MAX_FRAME_STORED_BYTES + 1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stored frame");
        assertThatThrownBy(() -> accounting.chargeFrame(1, 1, ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("decoded frame");
    }

    @Test
    void admittedFormatHardCapsHaveAllocationFreeExactBoundaryEvidence() {
        ObjectWalLocalCapacityHarnessV1.Evidence evidence = ObjectWalLocalCapacityHarnessV1.verifyFormatHardCaps();

        assertThat(evidence.exactBoundaryChecks()).isEqualTo(15);
        assertThat(evidence.rejectionChecks()).isEqualTo(17);
        assertThat(evidence.minimumChecks()).isEqualTo(1);
        assertThat(evidence.capMinusOneChecks()).isEqualTo(1);
        assertThat(evidence.cartesianNonClosureChecks()).isEqualTo(1);
        assertThat(evidence.checkedToIntChecks()).isEqualTo(1);
        assertThat(evidence.streamingCounterChecks()).isEqualTo(4);
        assertThat(evidence.parserChecks()).isEqualTo(2);
        assertThat(evidence.kmsEnvelopeChecks()).isEqualTo(2);
        assertThat(evidence.zstdSemanticChecks()).isEqualTo(2);
        assertThat(evidence.maximumDirectoryPrefixBytes()).isEqualTo(4 * 1024 * 1024);
        assertThat(evidence.maximumDirectoryPlaintextBytes()).isEqualTo(4_194_032);
        assertThat(evidence.maximumContexts()).isEqualTo(256);
        assertThat(evidence.maximumAppendUnits()).isEqualTo(65_536);
        assertThat(evidence.maximumFrames()).isEqualTo(65_536);
        assertThat(evidence.maximumFrameDecodedBytes()).isEqualTo(64 * 1024 * 1024);
        assertThat(evidence.maximumFrameStoredBytes()).isEqualTo((64 * 1024 * 1024) + 16);
        assertThat(evidence.maximumTotalDecodedBytes()).isEqualTo(4L * 1024 * 1024 * 1024);
        assertThat(evidence.maximumBodyBytes()).isEqualTo(4L * 1024 * 1024 * 1024);
        assertThat(evidence.zeroDecodedFrameAdmitted()).isTrue();
        assertThat(evidence.allocationFreeAnalyticalOnly()).isTrue();
        assertThat(evidence.providerTransferClaimed()).isFalse();
        assertThat(evidence.records())
                .extracting(ObjectWalLocalCapacityHarnessV1.LocalRecord::name)
                .containsExactly(
                        "NWG1_CAP_LOCAL_FORMULA_V1",
                        "NWG1_CAP_LOCAL_PARSER_V1",
                        "NWG1_CAP_LOCAL_CHECKED_ARITHMETIC_V1",
                        "NWG1_CAP_LOCAL_KMS_ENVELOPE_V1",
                        "NWG1_CAP_LOCAL_ZSTD_V1",
                        "NWG1_CAP_LOCAL_STREAMING_COUNTER_V1");
    }

    @Test
    void localFormulaRecordExercisesExactAndCartesianBoundaries() {
        ObjectWalLocalCapacityHarnessV1.Evidence evidence = ObjectWalLocalCapacityHarnessV1.verifyFormatHardCaps();
        assertThat(record(evidence, "NWG1_CAP_LOCAL_FORMULA_V1").counter())
                .isEqualTo("exactBoundaryChecks=15;cartesianNonClosureChecks=1");
        assertThat(evidence.exactBoundaryChecks()).isEqualTo(15);
        assertThat(evidence.cartesianNonClosureChecks()).isEqualTo(1);
    }

    @Test
    void localParserRecordExercisesLengthsFirstEnvelopeParser() {
        ObjectWalLocalCapacityHarnessV1.Evidence evidence = ObjectWalLocalCapacityHarnessV1.verifyFormatHardCaps();
        assertThat(record(evidence, "NWG1_CAP_LOCAL_PARSER_V1").counter())
                .isEqualTo("truncatedInputRejects=2;validationStage=HEADER_GRAMMAR");
        assertThat(evidence.parserChecks()).isEqualTo(2);
    }

    @Test
    void localCheckedArithmeticRecordExercisesOverflowAndNarrowing() {
        ObjectWalLocalCapacityHarnessV1.Evidence evidence = ObjectWalLocalCapacityHarnessV1.verifyFormatHardCaps();
        assertThat(record(evidence, "NWG1_CAP_LOCAL_CHECKED_ARITHMETIC_V1").counter())
                .isEqualTo("rejectionChecks=17;checkedToIntChecks=1");
        assertThat(evidence.rejectionChecks()).isEqualTo(17);
        assertThat(evidence.checkedToIntChecks()).isEqualTo(1);
    }

    @Test
    void localKmsEnvelopeRecordExercisesRoundTripAndOversizeRejection() {
        ObjectWalLocalCapacityHarnessV1.Evidence evidence = ObjectWalLocalCapacityHarnessV1.verifyFormatHardCaps();
        assertThat(record(evidence, "NWG1_CAP_LOCAL_KMS_ENVELOPE_V1").counter())
                .isEqualTo("canonicalRoundTrips=1;oversizeRejections=1");
        assertThat(evidence.kmsEnvelopeChecks()).isEqualTo(2);
    }

    @Test
    void localZstdRecordExercisesSemanticRoundTripWithoutExactOutputClaim() {
        ObjectWalLocalCapacityHarnessV1.Evidence evidence = ObjectWalLocalCapacityHarnessV1.verifyFormatHardCaps();
        assertThat(record(evidence, "NWG1_CAP_LOCAL_ZSTD_V1").counter())
                .isEqualTo("semanticRoundTrips=1;productionExactOutputClaims=0");
        assertThat(evidence.zstdSemanticChecks()).isEqualTo(2);
    }

    @Test
    void localStreamingCounterRecordIsAnalyticalAndClaimsNoProviderTransfer() {
        ObjectWalLocalCapacityHarnessV1.Evidence evidence = ObjectWalLocalCapacityHarnessV1.verifyFormatHardCaps();
        assertThat(record(evidence, "NWG1_CAP_LOCAL_STREAMING_COUNTER_V1").counter())
                .isEqualTo("analytical4GiBCounters=2;realProviderTransfers=0;streamingCounterChecks=4");
        assertThat(evidence.streamingCounterChecks()).isEqualTo(4);
        assertThat(evidence.allocationFreeAnalyticalOnly()).isTrue();
        assertThat(evidence.providerTransferClaimed()).isFalse();
    }

    @Test
    void streamingCounterRejectsCapAndArithmeticOverflowWithoutPartialCharge() {
        CheckedStreamingCounter counter = new CheckedStreamingCounter(Long.MAX_VALUE);
        counter.charge(Long.MAX_VALUE - 7);
        CheckedStreamingCounter.Snapshot exact = counter.snapshot();

        assertThatThrownBy(() -> counter.charge(16)).isInstanceOf(ArithmeticException.class);
        assertThat(counter.snapshot()).isEqualTo(exact);
    }

    private static ObjectWalLocalCapacityHarnessV1.LocalRecord record(
            ObjectWalLocalCapacityHarnessV1.Evidence evidence, String name) {
        return evidence.records().stream()
                .filter(record -> record.name().equals(name))
                .findFirst()
                .orElseThrow();
    }
}
