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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedDurableCommitV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Compact exact source identity and recoverable logical/physical coverage. */
public record KafkaReplicaSourceReferenceV1(
        KafkaReplicaSourceKindV1 sourceKind,
        CellProviderScopeId providerScopeId,
        Id128 sourceId,
        long sourceGeneration,
        long physicalStartUnit,
        long physicalEndUnitExclusive,
        long kafkaStartOffset,
        long kafkaEndOffsetExclusive,
        Sha256Digest sourceIdentityDigest,
        Sha256Digest payloadContentDigest) {
    public KafkaReplicaSourceReferenceV1 {
        Objects.requireNonNull(sourceKind, "sourceKind");
        Objects.requireNonNull(providerScopeId, "providerScopeId");
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(sourceIdentityDigest, "sourceIdentityDigest");
        Objects.requireNonNull(payloadContentDigest, "payloadContentDigest");
        if (sourceId.isZero()
                || sourceGeneration < 0
                || physicalStartUnit < 0
                || physicalEndUnitExclusive <= physicalStartUnit
                || kafkaStartOffset < 0
                || kafkaEndOffsetExclusive <= kafkaStartOffset
                || sourceIdentityDigest.isZero()
                || payloadContentDigest.isZero()) {
            throw new IllegalArgumentException("replica source reference is outside its bounded identity domain");
        }
    }

    public static KafkaReplicaSourceReferenceV1 bookKeeper(KafkaOrderedDurableCommitV1 commit, long sourceGeneration) {
        Objects.requireNonNull(commit, "commit");
        byte[] provider = commit.handle().providerScopeId().digest().bytes().toByteArray();
        byte[] runId = commit.handle().runId().value().bytes().toByteArray();
        byte[] configuration = commit.handle().configurationDigest().bytes().toByteArray();
        ByteBuffer identity = ByteBuffer.allocate(108);
        identity.putInt(0x4b_42_52_31);
        identity.put(provider);
        identity.put(runId);
        identity.putLong(commit.handle().ledgerIdentity().ledgerId());
        identity.put(configuration);
        identity.putLong(commit.firstDataEntryId());
        identity.putLong(commit.lastDataEntryId());
        return new KafkaReplicaSourceReferenceV1(
                KafkaReplicaSourceKindV1.BOOKKEEPER_RUN,
                commit.handle().providerScopeId(),
                commit.handle().runId().value(),
                sourceGeneration,
                commit.firstDataEntryId(),
                Math.incrementExact(commit.lastDataEntryId()),
                commit.startOffset(),
                commit.endOffsetExclusive(),
                Sha256Digest.hash(CanonicalBytes.copyOf(identity.array())),
                commit.aggregateAssignedPayloadSha256());
    }
}
