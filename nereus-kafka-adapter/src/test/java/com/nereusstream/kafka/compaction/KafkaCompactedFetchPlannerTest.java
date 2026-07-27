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
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;
import com.nereusstream.kafka.partition.KafkaStorageReadRequest;
import com.nereusstream.metadata.oxia.KafkaCompactionCoverageActivationMode;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class KafkaCompactedFetchPlannerTest {
  private static final KafkaPartitionIdentity IDENTITY = identity();
  private static final StreamId STREAM = new StreamId("kafka-compacted-plan-stream");
  private static final KafkaCompactedFetchPlanner PLANNER = new KafkaCompactedFetchPlanner();

  @Test
  void splitsMandatoryPrefixFromCommittedTailAndFreezesExactAuthority() {
    VersionedKafkaPartitionBinding binding = binding(5, 20, 5, 12);

    KafkaCompactedFetchPlanner.Plan plan =
        PLANNER.plan(
            IDENTITY,
            STREAM,
            KafkaStableSnapshot.nonTransactional(5, 20, 7),
            request(7, 18),
            binding);

    assertThat(plan.requestRange()).isEqualTo(new OffsetRange(7, 18));
    assertThat(plan.segments())
        .extracting(KafkaCompactedFetchPlanner.Segment::view)
        .containsExactly(ReadView.TOPIC_COMPACTED, ReadView.COMMITTED);
    assertThat(plan.segments())
        .extracting(KafkaCompactedFetchPlanner.Segment::range)
        .containsExactly(new OffsetRange(7, 12), new OffsetRange(12, 18));
    KafkaCompactedFetchPlanner.MandatoryAuthority authority =
        plan.segments().get(0).mandatoryAuthority().orElseThrow();
    assertThat(authority.activationEpoch()).isEqualTo(1);
    assertThat(authority.generationSetSha256().value()).isEqualTo("11".repeat(32));
    assertThat(authority.policySha256().value()).isEqualTo("22".repeat(32));
  }

  @Test
  void policyChangesCannotEraseAlreadyActivatedReadCoverage() {
    VersionedKafkaPartitionBinding binding = binding(0, 20, 0, 10);

    KafkaCompactedFetchPlanner.Plan plan =
        PLANNER.plan(
            IDENTITY,
            STREAM,
            KafkaStableSnapshot.nonTransactional(0, 20, 7),
            request(2, 16),
            binding);

    assertThat(plan.hasMandatoryCompactedPrefix()).isTrue();
    assertThat(plan.segments().get(0).range()).isEqualTo(new OffsetRange(2, 10));
    assertThat(plan.segments().get(1).range()).isEqualTo(new OffsetRange(10, 16));
  }

  @Test
  void rejectsTrimmedOffsetsWrongRootsAndCoverageBeyondLocalStableTruth() {
    VersionedKafkaPartitionBinding binding = binding(5, 20, 5, 12);
    KafkaStableSnapshot snapshot = KafkaStableSnapshot.nonTransactional(7, 20, 7);

    assertThatThrownBy(() -> PLANNER.plan(IDENTITY, STREAM, snapshot, request(6, 10), binding))
        .isInstanceOf(NereusException.class)
        .extracting(value -> ((NereusException) value).code())
        .isEqualTo(ErrorCode.OFFSET_TRIMMED);

    assertThatThrownBy(
            () ->
                PLANNER.plan(
                    IDENTITY, new StreamId("another-stream"), snapshot, request(7, 10), binding))
        .isInstanceOf(NereusException.class)
        .extracting(value -> ((NereusException) value).code())
        .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);

    assertThatThrownBy(
            () ->
                PLANNER.plan(
                    IDENTITY,
                    STREAM,
                    KafkaStableSnapshot.nonTransactional(5, 11, 7),
                    request(5, 11),
                    binding))
        .isInstanceOf(NereusException.class)
        .extracting(value -> ((NereusException) value).code())
        .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
  }

  static VersionedKafkaPartitionBinding binding(
      long observedLogStart, long observedStableEnd, long coverageStart, long coverageEnd) {
    KafkaPartitionPendingOperationRecord create =
        new KafkaPartitionPendingOperationRecord(
            KafkaPartitionOperationType.CREATE.wireId(),
            "create-compacted-fetch-test",
            "test-owner",
            1,
            20_000,
            1,
            10_000,
            "");
    var creating =
        KafkaPartitionMetadataTransitions.creating(
            IDENTITY.durableId(),
            IDENTITY.observedTopicName(),
            StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT.name(),
            1,
            10_000,
            create);
    var active =
        KafkaPartitionMetadataTransitions.activate(
            creating, "kafka-compacted-plan-name", STREAM.value(), 2, 10_001);
    var observed =
        KafkaPartitionMetadataTransitions.observe(
            active,
            active.observedTopicName(),
            3,
            1,
            4,
            9,
            observedLogStart,
            observedStableEnd,
            10_002);
    var activated =
        KafkaPartitionMetadataTransitions.activateCompactionCoverage(
            observed,
            KafkaCompactionCoverageActivationMode.INITIAL,
            coverageStart,
            coverageEnd,
            bytes(0x11),
            bytes(0x22),
            10_003);
    return new VersionedKafkaPartitionBinding(
        "/test/kafka-compacted-fetch",
        activated,
        0,
        new Checksum(ChecksumType.SHA256, "33".repeat(32)));
  }

  static KafkaStorageReadRequest request(long start, long end) {
    return new KafkaStorageReadRequest(
        start, end, 100, 1024 * 1024, 1024 * 1024, true, 0, 0, Duration.ofSeconds(5));
  }

  static KafkaPartitionIdentity identity() {
    ByteBuffer bytes = ByteBuffer.allocate(16).putLong(0x1020_3040_5060_7080L).putLong(4);
    return new KafkaPartitionIdentity(
        "kraft",
        Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array()),
        2,
        "compacted-orders");
  }

  private static byte[] bytes(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
