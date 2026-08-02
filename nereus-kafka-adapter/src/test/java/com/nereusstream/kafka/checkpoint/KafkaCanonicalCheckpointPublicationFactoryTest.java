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

package com.nereusstream.kafka.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class KafkaCanonicalCheckpointPublicationFactoryTest {
    private static final int LEADER_EPOCH = 7;
    private final KafkaPartitionIdentity identity = identity();
    private final VersionedKafkaPartitionBinding binding = binding(identity);
    private final KafkaCanonicalCheckpointPublicationFactory factory = new KafkaCanonicalCheckpointPublicationFactory(
            "nereus", sha256('c'), Duration.ofSeconds(5), Duration.ofMinutes(5), "test-build");

    @Test
    void buildsExactHeaderAndAllSevenCanonicalSections() {
        KafkaCheckpointSourceState source = source(false, 0);
        KafkaCheckpointPublicationRequest request =
                factory.create(identity, binding, source, genesis(), LEADER_EPOCH, validator(source));

        KafkaCheckpointHeader header = request.objectRequest().header();
        assertThat(header.kafkaClusterId()).isEqualTo(identity.kafkaClusterId());
        assertThat(header.topicId()).isEqualTo(identity.topicId());
        assertThat(header.partitionId()).isEqualTo(identity.partition());
        assertThat(header.checkpointOffset()).isZero();
        assertThat(header.logStartOffset()).isZero();
        assertThat(header.stableEndOffset()).isZero();
        assertThat(header.sourceCommitVersion()).isEqualTo(1);
        assertThat(request.objectRequest().sections())
                .extracting(section -> section.sectionType())
                .containsExactly(1, 2, 3, 4, 5, 6, 7);
    }

    @Test
    void rejectsInFlightOrStateMapAndCanonicalBoundMismatches() {
        assertThatThrownBy(() -> factory.create(
                        identity, binding, source(true, 0), genesis(), LEADER_EPOCH, validator(source(false, 0))))
                .hasMessageContaining("not exact");

        assertThatThrownBy(() -> factory.create(
                        identity, binding, source(false, 1), genesis(), LEADER_EPOCH, validator(source(false, 0))))
                .hasMessageContaining("not exact");
    }

    @Test
    void rejectsLeaderEpochThatDoesNotOwnTheCapturedSource() {
        KafkaCheckpointSourceState source = source(false, 0);
        assertThatThrownBy(
                        () -> factory.create(identity, binding, source, genesis(), LEADER_EPOCH + 1, validator(source)))
                .hasMessageContaining("not exact");
    }

    private KafkaCanonicalCheckpointState genesis() {
        return new KafkaCanonicalCheckpointState(
                0,
                0,
                0,
                new KafkaProducerTransactionState(0, List.of(), List.of(), List.of()),
                new KafkaLeaderEpochState(0, 0, List.of()),
                new KafkaVirtualSegmentState(0, 0, List.of(), List.of()),
                new KafkaDerivedIndexState(0, 0, List.of(), List.of()));
    }

    private KafkaCheckpointSourceState source(boolean appendInFlight, long stateMapEndOffset) {
        return new KafkaCheckpointSourceState(
                new AppendAuthority(
                        "kafka-partition-leader-v1",
                        identity.durableId().canonicalIdentity(),
                        LEADER_EPOCH,
                        "broker-1",
                        1),
                "writer-1",
                1,
                "token-1",
                1,
                0,
                0,
                1,
                "commit-1",
                sha256('b'),
                appendInFlight,
                stateMapEndOffset);
    }

    private static KafkaCheckpointSourceValidator validator(KafkaCheckpointSourceState source) {
        return new KafkaCheckpointSourceValidator() {
            @Override
            public CompletableFuture<KafkaCheckpointSourceState> loadCurrent() {
                return CompletableFuture.completedFuture(source);
            }

            @Override
            public CompletableFuture<Boolean> isSourceCommitReachable(
                    KafkaCheckpointHeader captured, KafkaCheckpointSourceState current) {
                return CompletableFuture.completedFuture(true);
            }
        };
    }

    private static KafkaPartitionIdentity identity() {
        ByteBuffer bytes =
                ByteBuffer.allocate(16).putLong(0x1234_5678_9abc_def0L).putLong(99);
        return new KafkaPartitionIdentity(
                "kraft", Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array()), 3, "orders");
    }

    private static VersionedKafkaPartitionBinding binding(KafkaPartitionIdentity identity) {
        KafkaPartitionPendingOperationRecord operation = new KafkaPartitionPendingOperationRecord(
                KafkaPartitionOperationType.CREATE.wireId(), "create-test", "broker-test", 1, 20_000, 1, 10_000, "");
        var creating = KafkaPartitionMetadataTransitions.creating(
                identity.durableId(),
                identity.observedTopicName(),
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT.name(),
                1,
                10_000,
                operation);
        var active = KafkaPartitionMetadataTransitions.activate(
                creating, "kafka-orders-stream", "kafka-orders-stream-id", 1, 10_001);
        return new VersionedKafkaPartitionBinding("/test/kafka-binding", active, 0, sha256('a'));
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }
}
