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

package com.nereusstream.kafka.bookkeeper.object.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaObjectWalNativeResultV1Test {
    private static final List<String> SUITES = List.of(
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1CodecV1Test",
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.ObjectKafkaProtocolCheckpointStoreV1Test",
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.StorageObjectNwkcp1BackendV1Test",
            "com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPublicationBridgeV1Test",
            "com.nereusstream.kafka.bookkeeper.object.evidence.KafkaObjectWalNativeResultV1Test");
    private static final List<Integer> TEST_COUNTS = List.of(4, 3, 17, 14, 3);

    @Test
    void roundTripsClosedRawResultWithExactCountersAndM6Exclusion() {
        KafkaObjectWalNativeResultV1.Receipt receipt = receipt();
        KafkaObjectWalNativeResultV1.Receipt parsed =
                KafkaObjectWalNativeResultV1.parseCanonical(KafkaObjectWalNativeResultV1.canonicalBytes(receipt));

        assertThat(parsed).isEqualTo(receipt);
        assertThat(parsed.componentKind()).isEqualTo("U_KAFKA_OBJECT_WAL");
        assertThat(parsed.counters().completionTicketBits()).isEqualTo(Long.SIZE);
        assertThat(parsed.counters().m6ActivationClaims()).isZero();
        assertThat(parsed.junit().totals().skipped()).isZero();
        assertThat(parsed.exclusions()).containsExactly("M6_NATIVE_BROKER_CONTROLLER_ACTIVATION");
    }

    @Test
    void rejectsCallerStatusTrailingBytesAndSelfHashTampering() {
        KafkaObjectWalNativeResultV1.Receipt valid = receipt();
        byte[] canonical = KafkaObjectWalNativeResultV1.canonicalBytes(valid);
        byte[] trailing = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        trailing[trailing.length - 1] = '\n';
        assertThatThrownBy(() -> KafkaObjectWalNativeResultV1.parseCanonical(trailing))
                .isInstanceOf(IllegalArgumentException.class);

        String changedStatus =
                new String(canonical, StandardCharsets.UTF_8).replace("\"status\":\"PASS\"", "\"status\":\"FAIL\"");
        assertThatThrownBy(() ->
                        KafkaObjectWalNativeResultV1.parseCanonical(changedStatus.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class);

        String changedHash =
                new String(canonical, StandardCharsets.UTF_8).replace(valid.receiptSha256(), "f".repeat(64));
        assertThatThrownBy(
                        () -> KafkaObjectWalNativeResultV1.parseCanonical(changedHash.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");
    }

    @Test
    void rejectsFailureErrorSkipAndNonCanonicalInventory() {
        KafkaObjectWalNativeResultV1.Receipt valid = receipt();
        var skipped = new KafkaObjectWalNativeResultV1.JunitEvidence(
                valid.junit().xmlRoot(),
                valid.junit().xmlFiles(),
                new KafkaObjectWalNativeResultV1.JunitTotals(5, 41, 0, 0, 1));
        assertThatThrownBy(
                        () -> KafkaObjectWalNativeResultV1.canonicalBytes(unsigned(valid, skipped, valid.artifacts())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zero-failure/skip");

        List<KafkaObjectWalNativeResultV1.Artifact> reversed = new ArrayList<>(valid.artifacts());
        Collections.reverse(reversed);
        assertThatThrownBy(() -> KafkaObjectWalNativeResultV1.canonicalBytes(unsigned(valid, valid.junit(), reversed)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closed identity/inventory");
    }

    private static KafkaObjectWalNativeResultV1.Receipt receipt() {
        List<KafkaObjectWalNativeResultV1.JunitFile> junitFiles = new ArrayList<>();
        int total = 0;
        for (int index = 0; index < SUITES.size(); index++) {
            int tests = TEST_COUNTS.get(index);
            total += tests;
            junitFiles.add(new KafkaObjectWalNativeResultV1.JunitFile(
                    "nereus-kafka-bookkeeper/build/test-results/test/TEST-" + SUITES.get(index) + ".xml",
                    Integer.toString(index + 1).repeat(64),
                    tests,
                    0,
                    0,
                    0));
        }
        var unsigned = new KafkaObjectWalNativeResultV1.Receipt(
                KafkaObjectWalNativeResultV1.SCHEMA,
                KafkaObjectWalNativeResultV1.COMPONENT_KIND,
                KafkaObjectWalNativeResultV1.STATUS,
                new KafkaObjectWalNativeResultV1.TestedSource("nereus", "6".repeat(40), "7".repeat(64)),
                List.of(new KafkaObjectWalNativeResultV1.ExternalSource("apache/kafka", "8".repeat(40))),
                new KafkaObjectWalNativeResultV1.Execution(
                        KafkaObjectWalNativeResultV1.DEFAULT_COMMAND,
                        Instant.parse("2026-08-24T10:00:00Z").toString(),
                        Instant.parse("2026-08-24T10:01:00Z").toString(),
                        "OpenJDK 21",
                        "macOS arm64"),
                new KafkaObjectWalNativeResultV1.JunitEvidence(
                        "nereus-kafka-bookkeeper/build/test-results/test",
                        junitFiles,
                        new KafkaObjectWalNativeResultV1.JunitTotals(5, total, 0, 0, 0)),
                KafkaObjectWalNativeResultV1.requiredTestsForTest(),
                KafkaObjectWalNativeResultV1.countersForTest(),
                artifacts(),
                List.of(KafkaObjectWalNativeResultV1.M6_EXCLUSION),
                "0".repeat(64));
        return KafkaObjectWalNativeResultV1.sealForTest(unsigned);
    }

    private static KafkaObjectWalNativeResultV1.Receipt unsigned(
            KafkaObjectWalNativeResultV1.Receipt valid,
            KafkaObjectWalNativeResultV1.JunitEvidence junit,
            List<KafkaObjectWalNativeResultV1.Artifact> artifacts) {
        return new KafkaObjectWalNativeResultV1.Receipt(
                valid.schema(),
                valid.componentKind(),
                valid.status(),
                valid.testedSource(),
                valid.externalSources(),
                valid.execution(),
                junit,
                valid.requiredTests(),
                valid.counters(),
                artifacts,
                valid.exclusions(),
                "0".repeat(64));
    }

    private static List<KafkaObjectWalNativeResultV1.Artifact> artifacts() {
        List<String> paths = new ArrayList<>(List.of(
                "nereus-kafka-bookkeeper/build.gradle.kts",
                "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/"
                        + "KafkaCoherentCommitCoordinatorV1.java",
                "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                        + "KafkaPartitionObjectTailRetirementSlotV1.java",
                "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                        + "KafkaPartitionPublicationCellV1.java",
                "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                        + "KafkaPartitionPublicationKindV1.java",
                "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                        + "KafkaPartitionPublicationOutcomeV1.java",
                "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                        + "KafkaPartitionSpeculativeRollbackSlotV1.java"));
        for (int index = 0; index < 44; index++) {
            paths.add("nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/"
                    + String.format("SyntheticInventory%02d.java", index));
        }
        return paths.stream()
                .sorted()
                .map(path -> new KafkaObjectWalNativeResultV1.Artifact(path, "9".repeat(64), 1))
                .toList();
    }
}
