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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointWriteRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

/**
 * Builds an exact publication request from one partition-lock-frozen canonical state image.
 */
public final class KafkaCanonicalCheckpointPublicationFactory {
    private static final String AUTHORITY_TYPE = "kafka-partition-leader-v1";

    private final String nereusCluster;
    private final Checksum contentPolicySha256;
    private final Duration objectTimeout;
    private final Duration pendingProtectionTtl;
    private final String writerBuild;
    private final KafkaCanonicalCheckpointStateCodecV1 stateCodec;

    public KafkaCanonicalCheckpointPublicationFactory(
            String nereusCluster,
            Checksum contentPolicySha256,
            Duration objectTimeout,
            Duration pendingProtectionTtl,
            String writerBuild) {
        this(
                nereusCluster,
                contentPolicySha256,
                objectTimeout,
                pendingProtectionTtl,
                writerBuild,
                new KafkaCanonicalCheckpointStateCodecV1());
    }

    KafkaCanonicalCheckpointPublicationFactory(
            String nereusCluster,
            Checksum contentPolicySha256,
            Duration objectTimeout,
            Duration pendingProtectionTtl,
            String writerBuild,
            KafkaCanonicalCheckpointStateCodecV1 stateCodec) {
        this.nereusCluster = text(nereusCluster, "nereusCluster");
        this.contentPolicySha256 = Objects.requireNonNull(contentPolicySha256, "contentPolicySha256");
        if (contentPolicySha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("contentPolicySha256 must use SHA256");
        }
        this.objectTimeout = positive(objectTimeout, "objectTimeout");
        this.pendingProtectionTtl = positive(pendingProtectionTtl, "pendingProtectionTtl");
        this.writerBuild = text(writerBuild, "writerBuild");
        this.stateCodec = Objects.requireNonNull(stateCodec, "stateCodec");
    }

    public KafkaCheckpointPublicationRequest create(
            KafkaPartitionIdentity identity,
            VersionedKafkaPartitionBinding capturedBinding,
            KafkaCheckpointSourceState capturedSource,
            KafkaCanonicalCheckpointState capturedState,
            int leaderEpoch,
            KafkaCheckpointSourceValidator sourceValidator) {
        KafkaPartitionIdentity exactIdentity = Objects.requireNonNull(identity, "identity");
        VersionedKafkaPartitionBinding exactBinding = Objects.requireNonNull(capturedBinding, "capturedBinding");
        KafkaCheckpointSourceState exactSource = Objects.requireNonNull(capturedSource, "capturedSource");
        KafkaCanonicalCheckpointState exactState = Objects.requireNonNull(capturedState, "capturedState");
        KafkaCheckpointSourceValidator exactValidator = Objects.requireNonNull(sourceValidator, "sourceValidator");
        var binding = exactBinding.value();
        if (leaderEpoch < 0
                || !binding.identity().equals(exactIdentity.durableId())
                || binding.lifecycle() != KafkaPartitionLifecycle.ACTIVE
                || exactState.checkpointOffset() != exactSource.endOffset()
                || exactState.logStartOffset() != exactSource.trimOffset()
                || exactState.stableEndOffset() != exactSource.endOffset()
                || exactSource.appendInFlight()
                || exactSource.stateMapEndOffset() != exactSource.endOffset()
                || !exactSource.authority().authorityType().equals(AUTHORITY_TYPE)
                || exactSource.authority().authorityEpoch() != leaderEpoch
                || !exactSource
                        .authority()
                        .authorityId()
                        .equals(exactIdentity.durableId().canonicalIdentity())) {
            throw new IllegalArgumentException("canonical Kafka checkpoint capture is not exact");
        }
        KafkaCheckpointHeader header = new KafkaCheckpointHeader(
                0,
                exactIdentity.kafkaClusterId(),
                exactIdentity.topicId(),
                exactIdentity.partition(),
                binding.incarnation(),
                new StreamId(binding.streamId()),
                binding.payloadMappingId(),
                leaderEpoch,
                exactState.checkpointOffset(),
                exactState.logStartOffset(),
                exactState.stableEndOffset(),
                exactSource.commitVersion(),
                exactSource.lastCommitId(),
                exactSource.headSha256());
        KafkaCheckpointWriteRequest objectRequest = new KafkaCheckpointWriteRequest(
                nereusCluster, header, stateCodec.encodeSections(exactState), contentPolicySha256, objectTimeout);
        return new KafkaCheckpointPublicationRequest(
                exactIdentity,
                exactBinding,
                exactSource,
                objectRequest,
                exactValidator,
                pendingProtectionTtl,
                writerBuild);
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > 64 * 1024) {
            throw new IllegalArgumentException(name + " must be nonblank and bounded");
        }
        return value;
    }
}
