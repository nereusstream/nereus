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

package com.nereusstream.kafka.bookkeeper.replication;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Exact source-map answer for one descriptor, including compatible replacement proof. */
public record KafkaReplicaSourceQualificationV1(
        Sha256Digest descriptorDigest,
        KafkaReplicaSourceReferenceV1 installedSource,
        long coveredStartOffset,
        long coveredEndOffsetExclusive,
        Sha256Digest payloadContentDigest,
        KafkaReplicaProtocolProofV1 protocolProof,
        boolean accessible,
        boolean durable,
        boolean qualifiedWithoutPayload) {
    public KafkaReplicaSourceQualificationV1 {
        Objects.requireNonNull(descriptorDigest, "descriptorDigest");
        Objects.requireNonNull(installedSource, "installedSource");
        Objects.requireNonNull(payloadContentDigest, "payloadContentDigest");
        Objects.requireNonNull(protocolProof, "protocolProof");
        if (descriptorDigest.isZero()
                || coveredStartOffset < 0
                || coveredEndOffsetExclusive <= coveredStartOffset
                || payloadContentDigest.isZero()) {
            throw new IllegalArgumentException("source qualification is outside its exact coverage domain");
        }
    }

    public boolean qualifies(KafkaReplicaCommitDescriptorV1 descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        return descriptorDigest.equals(KafkaReplicaCommitDescriptorCodecV1.digest(descriptor))
                && accessible
                && durable
                && coveredStartOffset <= descriptor.startOffset()
                && coveredEndOffsetExclusive >= descriptor.endOffsetExclusive()
                && installedSource.kafkaStartOffset() <= descriptor.startOffset()
                && installedSource.kafkaEndOffsetExclusive() >= descriptor.endOffsetExclusive()
                && payloadContentDigest.equals(descriptor.aggregateAssignedPayloadSha256())
                && installedSource.payloadContentDigest().equals(descriptor.aggregateAssignedPayloadSha256())
                && protocolProof.equals(descriptor.protocolProof())
                && (installedSource.equals(descriptor.source())
                        || installedSource.sourceGeneration()
                                > descriptor.source().sourceGeneration());
    }
}
