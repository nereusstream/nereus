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

package com.nereusstream.kafka.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanPage;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanToken;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaCompactionPlan;
import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaCompactionPlanOrphanScannerTest {
    private static final KafkaPartitionId PARTITION = new KafkaPartitionId("kraft", "AAAAAAAAAAAAAAAAAAAAAQ", 4);
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @Test
    void removesOnlyOldPlanWithoutTaskUnderRepeatedAuthorityChecks() {
        FakePlanStore plans = new FakePlanStore(plan('a', 'b', 1_000), plan('c', 'd', 2_000), plan('e', 'f', 9_500));
        AtomicInteger guardCalls = new AtomicInteger();
        KafkaCompactionPlanOrphanScanner scanner =
                scanner(plans, (streamId, taskId) -> CompletableFuture.completedFuture(taskId.equals(taskId('d'))));

        KafkaCompactionPlanOrphanScanner.ScanResult result = scanner.scan(PARTITION, Optional.empty(), () -> {
                    guardCalls.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                })
                .join();

        assertThat(result.scanned()).isEqualTo(3);
        assertThat(result.deleted()).isEqualTo(1);
        assertThat(result.taskRootPresent()).isEqualTo(1);
        assertThat(result.youngerThanGrace()).isEqualTo(1);
        assertThat(result.disappeared()).isZero();
        assertThat(result.reconciledDeleteResponses()).isZero();
        assertThat(result.exhausted()).isTrue();
        assertThat(plans.taskIds()).containsExactlyInAnyOrder(taskId('d'), taskId('f'));
        assertThat(guardCalls).hasValue(3);
    }

    @Test
    void reconcilesDeleteResponseLossByExactAbsence() {
        FakePlanStore plans = new FakePlanStore(plan('a', 'b', 1_000));
        plans.loseDeleteResponse = true;
        KafkaCompactionPlanOrphanScanner scanner =
                scanner(plans, (streamId, taskId) -> CompletableFuture.completedFuture(false));

        KafkaCompactionPlanOrphanScanner.ScanResult result = scanner.scan(
                        PARTITION, Optional.empty(), () -> CompletableFuture.completedFuture(null))
                .join();

        assertThat(result.deleted()).isEqualTo(1);
        assertThat(result.reconciledDeleteResponses()).isEqualTo(1);
        assertThat(plans.taskIds()).isEmpty();
    }

    @Test
    void taskAppearingBeforeFinalFencePreventsPlanDeletion() {
        FakePlanStore plans = new FakePlanStore(plan('a', 'b', 1_000));
        AtomicInteger taskLookups = new AtomicInteger();
        KafkaCompactionPlanOrphanScanner scanner = scanner(
                plans, (streamId, taskId) -> CompletableFuture.completedFuture(taskLookups.incrementAndGet() >= 2));

        KafkaCompactionPlanOrphanScanner.ScanResult result = scanner.scan(
                        PARTITION, Optional.empty(), () -> CompletableFuture.completedFuture(null))
                .join();

        assertThat(result.taskRootPresent()).isEqualTo(1);
        assertThat(result.deleted()).isZero();
        assertThat(plans.deleteCalls).isZero();
    }

    @Test
    void authorityLossStopsBeforeMetadataScanOrDelete() {
        FakePlanStore plans = new FakePlanStore(plan('a', 'b', 1_000));
        KafkaCompactionPlanOrphanScanner scanner =
                scanner(plans, (streamId, taskId) -> CompletableFuture.completedFuture(false));

        CompletableFuture<KafkaCompactionPlanOrphanScanner.ScanResult> failed = scanner.scan(
                PARTITION,
                Optional.empty(),
                () -> CompletableFuture.failedFuture(new IllegalStateException("leadership lost")));

        assertThatThrownBy(failed::join).isInstanceOf(CompletionException.class).hasRootCauseMessage("leadership lost");
        assertThat(plans.scanCalls).isZero();
        assertThat(plans.deleteCalls).isZero();
    }

    private static KafkaCompactionPlanOrphanScanner scanner(
            FakePlanStore plans, KafkaCompactionPlanOrphanScanner.TaskRoots tasks) {
        return new KafkaCompactionPlanOrphanScanner(plans, tasks, CLOCK, Duration.ofSeconds(1), 16, 16);
    }

    private static VersionedKafkaCompactionPlan plan(char planCharacter, char taskCharacter, long createdAtMillis) {
        byte[] bytes = ("canonical-" + planCharacter).getBytes(StandardCharsets.UTF_8);
        KafkaCompactionPlanRecord value = new KafkaCompactionPlanRecord(
                1,
                PARTITION.kafkaClusterId(),
                PARTITION.topicId(),
                PARTITION.partitionId(),
                "stream-19",
                "kcp1-" + Character.toString(planCharacter).repeat(52),
                taskId(taskCharacter),
                0,
                10,
                12,
                sha256(bytes),
                bytes,
                createdAtMillis,
                3);
        return new VersionedKafkaCompactionPlan(
                "plans/" + value.materializationTaskId(), value, 3, new Checksum(ChecksumType.SHA256, "a".repeat(64)));
    }

    private static String taskId(char character) {
        return "mat1-" + Character.toString(character).repeat(52);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static final class FakePlanStore implements KafkaCompactionPlanMetadataStore {
        private final Map<String, VersionedKafkaCompactionPlan> plans = new LinkedHashMap<>();
        private int scanCalls;
        private int deleteCalls;
        private boolean loseDeleteResponse;

        private FakePlanStore(VersionedKafkaCompactionPlan... values) {
            for (VersionedKafkaCompactionPlan value : values) {
                plans.put(value.value().materializationTaskId(), value);
            }
        }

        @Override
        public CompletableFuture<Optional<VersionedKafkaCompactionPlan>> getCompactionPlan(
                KafkaPartitionId id, String materializationTaskId) {
            if (!PARTITION.equals(id)) {
                return CompletableFuture.failedFuture(new AssertionError("wrong partition"));
            }
            return CompletableFuture.completedFuture(Optional.ofNullable(plans.get(materializationTaskId)));
        }

        @Override
        public CompletableFuture<VersionedKafkaCompactionPlan> putCompactionPlanIfAbsent(
                KafkaCompactionPlanRecord value) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<KafkaCompactionPlanScanPage> scanCompactionPlans(
                KafkaPartitionId id, Optional<KafkaCompactionPlanScanToken> continuation, int limit) {
            scanCalls++;
            if (!PARTITION.equals(id) || continuation.isPresent() || plans.size() > limit) {
                return CompletableFuture.failedFuture(new AssertionError("unexpected scan request"));
            }
            List<VersionedKafkaCompactionPlan> values = new ArrayList<>(plans.values());
            values.sort(Comparator.comparing(value -> value.value().materializationTaskId()));
            return CompletableFuture.completedFuture(new KafkaCompactionPlanScanPage(values, Optional.empty()));
        }

        @Override
        public CompletableFuture<Void> deleteCompactionPlan(VersionedKafkaCompactionPlan expected) {
            deleteCalls++;
            VersionedKafkaCompactionPlan current = plans.get(expected.value().materializationTaskId());
            if (!expected.equals(current)) {
                return CompletableFuture.failedFuture(new IllegalStateException("stale plan"));
            }
            plans.remove(expected.value().materializationTaskId());
            return loseDeleteResponse
                    ? CompletableFuture.failedFuture(new IllegalStateException("lost delete response"))
                    : CompletableFuture.completedFuture(null);
        }

        private List<String> taskIds() {
            return List.copyOf(plans.keySet());
        }
    }
}
