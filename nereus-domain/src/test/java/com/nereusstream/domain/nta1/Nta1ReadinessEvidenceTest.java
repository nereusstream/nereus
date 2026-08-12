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

package com.nereusstream.domain.nta1;

import static com.nereusstream.domain.nta1.Nta1ReadinessHarness.COMPATIBILITY_CAPS;
import static com.nereusstream.domain.nta1.Nta1ReadinessHarness.PERFORMANCE_CAPS;
import static com.nereusstream.domain.nta1.Nta1ReadinessHarness.PolicyCandidate.NONE;
import static com.nereusstream.domain.nta1.Nta1ReadinessHarness.PolicyCandidate.ZSTD_FAST_IF_SAVES_12_5_PERCENT_V1;
import static com.nereusstream.domain.nta1.Nta1ReadinessHarness.PolicyCandidate.ZSTD_FAST_IF_SMALLER_V1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.protocol.KafkaTopicName;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class Nta1ReadinessEvidenceTest {
    @Test
    void measuresFixedAndDerivedCandidateBounds() {
        assertThat(Nta1ReadinessHarness.FIXED_NTA1_BYTES).isEqualTo(129);
        assertThat(Nta1ReadinessHarness.KAFKA_CELL_BYTES).isEqualTo(38);
        assertThat(Nta1ReadinessHarness.PULSAR_CELL_BYTES).isEqualTo(54);
        assertThat(Nta1ReadinessHarness.MAX_KAFKA_INCARNATION_BYTES).isEqualTo(275);
        assertThat(PERFORMANCE_CAPS.maxIncarnationBytes()).isEqualTo(8214);
        assertThat(PERFORMANCE_CAPS.maxNta1Bytes()).isEqualTo(8397);
        assertThat(COMPATIBILITY_CAPS.maxIncarnationBytes()).isEqualTo(32790);
        assertThat(COMPATIBILITY_CAPS.maxNta1Bytes()).isEqualTo(32973);
    }

    @Test
    void measuresMinimumTypicalAndMaximumRealDomainObjects() {
        assertThat(Nta1ReadinessHarness.measuredFourKibVectorSizes()).containsExactly(194, 202, 442, 239, 261, 8395);
        byte[] sixteenKibMax = Nta1ReadinessHarness.encode(
                Nta1ReadinessHarness.maxClassicPulsarAggregate(
                        COMPATIBILITY_CAPS, StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1),
                COMPATIBILITY_CAPS);
        assertThat(sixteenKibMax).hasSize(32971);
    }

    @Test
    void roundTripsAllSixProtocolProfileRowsForRecommendedPolicy() {
        for (StorageProfileV1 profile : StorageProfileV1.values()) {
            var policy = profile == StorageProfileV1.OBJECT_WAL ? ZSTD_FAST_IF_SMALLER_V1 : NONE;
            assertRoundTrip(Nta1ReadinessHarness.kafkaAggregate("orders", profile, policy), PERFORMANCE_CAPS);
            assertRoundTrip(
                    Nta1ReadinessHarness.pulsarAggregate(
                            "tenant/ns/persistent/orders", "persistent://tenant/ns/orders", profile, policy),
                    PERFORMANCE_CAPS);
        }
    }

    @Test
    void roundTripsBothNonNonePolicyCandidatesWithoutPayloadBytes() {
        TopicBindingAggregateV1 ifSmaller =
                Nta1ReadinessHarness.kafkaAggregate("orders", StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1);
        TopicBindingAggregateV1 savesTwelvePercent = Nta1ReadinessHarness.kafkaAggregate(
                "orders", StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SAVES_12_5_PERCENT_V1);

        assertRoundTrip(ifSmaller, PERFORMANCE_CAPS);
        assertRoundTrip(savesTwelvePercent, PERFORMANCE_CAPS);
        assertThat(Nta1ReadinessHarness.encode(ifSmaller, PERFORMANCE_CAPS))
                .hasSameSizeAs(Nta1ReadinessHarness.encode(savesTwelvePercent, PERFORMANCE_CAPS));
    }

    @Test
    void rejectsEveryProfilePolicyMismatch() {
        assertThatThrownBy(() -> Nta1ReadinessHarness.encode(
                        Nta1ReadinessHarness.kafkaAggregate("orders", StorageProfileV1.OBJECT_WAL, NONE),
                        PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class);
        for (StorageProfileV1 profile :
                Arrays.asList(StorageProfileV1.BOOKKEEPER_WAL_ONLY, StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT)) {
            assertThatThrownBy(() -> Nta1ReadinessHarness.encode(
                            Nta1ReadinessHarness.kafkaAggregate("orders", profile, ZSTD_FAST_IF_SMALLER_V1),
                            PERFORMANCE_CAPS))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Nta1ReadinessHarness.encode(
                            Nta1ReadinessHarness.pulsarAggregate(
                                    "tenant/ns/persistent/orders",
                                    "persistent://tenant/ns/orders",
                                    profile,
                                    ZSTD_FAST_IF_SMALLER_V1),
                            PERFORMANCE_CAPS))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsUnknownMixedAndNonemptyFramePolicies() {
        assertThatThrownBy(() -> new FrameEncodingPolicyValueV1(0, 1, CanonicalBytes.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FrameEncodingPolicyValueV1(1, 0, CanonicalBytes.empty()))
                .isInstanceOf(IllegalArgumentException.class);

        TopicBindingAggregateV1 valid =
                Nta1ReadinessHarness.kafkaAggregate("orders", StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1);
        var nonempty = new FrameEncodingPolicyValueV1(1, 1, CanonicalBytes.copyOf(new byte[] {1}));
        var epoch = new InitialStorageEpochV1(
                valid.initialEpoch().storageEpochId(),
                0,
                valid.initialEpoch().storageProfile(),
                valid.initialEpoch().profileOrigin(),
                valid.initialEpoch().policyCatalogDigest(),
                nonempty);
        TopicBindingAggregateV1 aggregate = new TopicBindingAggregateV1(1, valid.binding(), epoch);
        assertThatThrownBy(() -> Nta1ReadinessHarness.encode(aggregate, PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class);

        byte[] encoded = Nta1ReadinessHarness.encode(valid, PERFORMANCE_CAPS);
        ByteBuffer.wrap(encoded).putShort(encoded.length - 5, (short) 99);
        assertThatThrownBy(() -> Nta1ReadinessHarness.decode(encoded, PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void measuresKafka249ByteBoundaryAndRejects250() {
        TopicBindingAggregateV1 max = Nta1ReadinessHarness.kafkaAggregate(
                "k".repeat(KafkaTopicName.MAX_LENGTH), StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1);
        assertThat(Nta1ReadinessHarness.encode(max, PERFORMANCE_CAPS)).hasSize(442);
        assertThatThrownBy(() -> Nta1ReadinessHarness.kafkaAggregate(
                        "k".repeat(KafkaTopicName.MAX_LENGTH + 1),
                        StorageProfileV1.OBJECT_WAL,
                        ZSTD_FAST_IF_SMALLER_V1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void measuresBothPulsarNameCapsAndRejectsOneByteOver() {
        TopicBindingAggregateV1 fourKib = Nta1ReadinessHarness.maxClassicPulsarAggregate(
                PERFORMANCE_CAPS, StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1);
        TopicBindingAggregateV1 sixteenKib = Nta1ReadinessHarness.maxClassicPulsarAggregate(
                COMPATIBILITY_CAPS, StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1);
        assertThat(Nta1ReadinessHarness.encode(fourKib, PERFORMANCE_CAPS)).hasSize(8395);
        assertThat(Nta1ReadinessHarness.encode(sixteenKib, COMPATIBILITY_CAPS)).hasSize(32971);

        String prefix = "persistent://t/n/";
        String local = "a".repeat(PERFORMANCE_CAPS.maxTopicNameBytes() - prefix.length() + 1);
        TopicBindingAggregateV1 over = Nta1ReadinessHarness.pulsarAggregate(
                "t/n/persistent/" + local, prefix + local, StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1);
        assertThatThrownBy(() -> Nta1ReadinessHarness.encode(over, PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsPulsarNameMismatchAndNonClassicDomainCandidate() {
        assertThatThrownBy(() -> Nta1ReadinessHarness.encode(
                        Nta1ReadinessHarness.pulsarAggregate(
                                "tenant/ns/persistent/other",
                                "persistent://tenant/ns/orders",
                                StorageProfileV1.OBJECT_WAL,
                                ZSTD_FAST_IF_SMALLER_V1),
                        PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Nta1ReadinessHarness.encode(
                        Nta1ReadinessHarness.pulsarAggregate(
                                "tenant/ns/orders",
                                "topic://tenant/ns/orders",
                                StorageProfileV1.OBJECT_WAL,
                                ZSTD_FAST_IF_SMALLER_V1),
                        PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsStrictUnicodeRoundTripAndRejectsMalformedUtf8() {
        String local = "orders-α";
        TopicBindingAggregateV1 unicode = Nta1ReadinessHarness.pulsarAggregate(
                "tenant/ns/persistent/orders-%CE%B1",
                "persistent://tenant/ns/" + local, StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1);
        byte[] encoded = Nta1ReadinessHarness.encode(unicode, PERFORMANCE_CAPS);
        assertRoundTrip(unicode, PERFORMANCE_CAPS);

        int cellLength = ByteBuffer.wrap(encoded).getInt(40);
        int incarnationOffset = 48 + cellLength;
        int persistenceOffset = incarnationOffset + 10;
        encoded[persistenceOffset] = (byte) 0xc0;
        assertThatThrownBy(() -> Nta1ReadinessHarness.decode(encoded, PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validatesUnsignedLengthsBeforeLengthFramedAllocation() {
        TopicBindingAggregateV1 aggregate =
                Nta1ReadinessHarness.kafkaAggregate("orders", StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1);
        byte[] encoded = Nta1ReadinessHarness.encode(aggregate, PERFORMANCE_CAPS);
        ByteBuffer.wrap(encoded).putInt(40, -1);
        assertThatThrownBy(() -> Nta1ReadinessHarness.decode(encoded, PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allocation bound");

        byte[] signedOverflow = Nta1ReadinessHarness.encode(aggregate, PERFORMANCE_CAPS);
        ByteBuffer.wrap(signedOverflow).putInt(40, Integer.MIN_VALUE);
        assertThatThrownBy(() -> Nta1ReadinessHarness.decode(signedOverflow, PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allocation bound");
    }

    @Test
    void boundsActualParserAllocationToValidatedLengths() {
        TopicBindingAggregateV1 aggregate = Nta1ReadinessHarness.maxClassicPulsarAggregate(
                PERFORMANCE_CAPS, StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1);
        var result =
                Nta1ReadinessHarness.decode(Nta1ReadinessHarness.encode(aggregate, PERFORMANCE_CAPS), PERFORMANCE_CAPS);
        assertThat(result.largestLengthFramedAllocation()).isEqualTo(8212);
        assertThat(result.totalLengthFramedAllocation()).isEqualTo(8266);
        assertThat(result.largestLengthFramedAllocation()).isLessThanOrEqualTo(PERFORMANCE_CAPS.maxIncarnationBytes());
    }

    @Test
    void rejectsTotalCapTrailingBytesTruncationAndCheckedOverflow() {
        byte[] tooLarge = new byte[PERFORMANCE_CAPS.maxNta1Bytes() + 1];
        assertThatThrownBy(() -> Nta1ReadinessHarness.decode(tooLarge, PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parser cap");

        byte[] typical = Nta1ReadinessHarness.encode(
                Nta1ReadinessHarness.kafkaAggregate("orders", StorageProfileV1.OBJECT_WAL, ZSTD_FAST_IF_SMALLER_V1),
                PERFORMANCE_CAPS);
        byte[] trailing = Arrays.copyOf(typical, typical.length + 1);
        assertThatThrownBy(() -> Nta1ReadinessHarness.decode(trailing, PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing");
        assertThatThrownBy(
                        () -> Nta1ReadinessHarness.decode(Arrays.copyOf(typical, typical.length - 1), PERFORMANCE_CAPS))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Nta1ReadinessHarness.checkedSize(Integer.MAX_VALUE, 1))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void rendersDeterministicStructuredReadinessEvidence() throws Exception {
        String evidence = Nta1ReadinessHarness.renderEvidenceJson();
        assertThat(evidence)
                .contains("\"result\": \"READINESS_EVIDENCE_ONLY\"")
                .contains("\"promotionEligible\": false")
                .contains("\"productionCodecImplemented\": false")
                .contains("\"scenarioPromotion\": false");
        Path report = Path.of("build/reports/v2-m1-nta1-readiness/readiness.json");
        Files.createDirectories(report.getParent());
        Files.writeString(report, evidence, StandardCharsets.UTF_8);
        assertThat(Files.readString(report, StandardCharsets.UTF_8)).isEqualTo(evidence);
    }

    private static void assertRoundTrip(TopicBindingAggregateV1 aggregate, Nta1ReadinessHarness.Caps caps) {
        byte[] encoded = Nta1ReadinessHarness.encode(aggregate, caps);
        assertThat(Nta1ReadinessHarness.decode(encoded, caps).aggregate()).isEqualTo(aggregate);
    }
}
