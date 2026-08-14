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
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedDurableCommitV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedDurableDataMemberV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFrontiersV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferencesV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.util.ArrayList;
import java.util.List;

final class KafkaReplicationTestFixtures {
    private KafkaReplicationTestFixtures() {}

    static Nbke2RunBindingV1 binding() {
        return KafkaRunTestFixtures.binding(6, 11, 5);
    }

    static KafkaPartitionFenceV1 fence() {
        return new KafkaPartitionFenceV1(
                binding().bindingId(),
                binding().topicIncarnation(),
                binding().partitionId(),
                13,
                binding().storageEpochId(),
                binding().creatorOwnerEpoch(),
                binding().kafkaLeaderEpoch());
    }

    static KafkaReplicaProtocolProofV1 protocolProof() {
        return new KafkaReplicaProtocolProofV1(digest(31), digest(32), digest(33), digest(34));
    }

    static KafkaReplicaCommitDescriptorV1 descriptor(long startOffset, long endOffset, long stateVersion) {
        return descriptor(
                startOffset, endOffset, stateVersion, KafkaReplicaObservationModeV1.DESCRIPTOR_QUALIFIED, 500);
    }

    static KafkaReplicaCommitDescriptorV1 descriptor(
            long startOffset,
            long endOffset,
            long stateVersion,
            KafkaReplicaObservationModeV1 mode,
            long encodedBytes) {
        Sha256Digest payload = digest((int) (40 + startOffset % 20));
        KafkaReplicaSourceReferenceV1 source = new KafkaReplicaSourceReferenceV1(
                KafkaReplicaSourceKindV1.BOOKKEEPER_RUN,
                binding().providerScopeId(),
                new Id128(0, 70 + stateVersion),
                3,
                10 + stateVersion,
                11 + stateVersion,
                startOffset,
                endOffset,
                digest((int) (60 + stateVersion)),
                payload);
        return new KafkaReplicaCommitDescriptorV1(
                fence(), stateVersion, startOffset, endOffset, encodedBytes, payload, source, protocolProof(), mode);
    }

    static KafkaReplicaSourceQualificationV1 qualification(
            KafkaReplicaCommitDescriptorV1 descriptor, boolean accessible, boolean withoutPayload) {
        return new KafkaReplicaSourceQualificationV1(
                KafkaReplicaCommitDescriptorCodecV1.digest(descriptor),
                descriptor.source(),
                descriptor.startOffset(),
                descriptor.endOffsetExclusive(),
                descriptor.aggregateAssignedPayloadSha256(),
                descriptor.protocolProof(),
                accessible,
                true,
                withoutPayload);
    }

    static KafkaReplicaApplyProofV1 applyProof(KafkaReplicaCommitDescriptorV1 descriptor) {
        return new KafkaReplicaApplyProofV1(
                KafkaReplicaCommitDescriptorCodecV1.digest(descriptor),
                descriptor.startOffset(),
                descriptor.endOffsetExclusive(),
                descriptor.validatedStateVersion(),
                descriptor.protocolProof());
    }

    static KafkaReplicaCommitDescriptorV1 descriptorFromBookKeeperCut() {
        RunLedgerHandleV1 handle = new RunLedgerHandleV1(
                binding().providerScopeId(), binding().runId(), new BookKeeperLedgerIdentity(47), digest(20));
        KafkaOrderedDurableCommitV1 commit = new KafkaOrderedDurableCommitV1(
                100,
                101,
                binding(),
                handle,
                1,
                1,
                1,
                500,
                digest(21),
                new Id128(0, 81),
                new Id128(0, 82),
                List.of(new KafkaOrderedDurableDataMemberV1(100, 101, 1, 0, 61)));
        KafkaPartitionStateReferencesV1 references = new KafkaPartitionStateReferencesV1(
                ref(1), ref(2), ref(3), ref(4), ref(5), ref(6), ref(7), ref(8), ref(9));
        KafkaPartitionProtocolStateV1 state = new KafkaPartitionProtocolStateV1(
                fence(), 2, new KafkaPartitionFrontiersV1(100, 101, 101, 101, 100, 100), references);
        return KafkaReplicaCommitDescriptorV1.fromBookKeeper(
                commit, state, KafkaReplicaObservationModeV1.DESCRIPTOR_QUALIFIED);
    }

    static KafkaPartitionStateReferenceV1 ref(int value) {
        return new KafkaPartitionStateReferenceV1(value, digest(90 + value));
    }

    static Sha256Digest digest(int seed) {
        return KafkaRunTestFixtures.digest(seed);
    }

    static final class FakeJournalStorage implements KafkaReplicaObservationJournalStorageV1 {
        final List<CanonicalBytes> records = new ArrayList<>();
        KafkaReplicaJournalHealthV1 health = KafkaReplicaJournalHealthV1.HEALTHY;
        KafkaReplicaJournalAppendProofV1 proofOverride;
        RuntimeException appendFailure;
        int appendCalls;

        @Override
        public KafkaReplicaJournalStorageSnapshotV1 readBounded(KafkaReplicaJournalBoundsV1 bounds) {
            long bytes = records.stream().mapToLong(CanonicalBytes::length).sum();
            KafkaReplicaJournalHealthV1 resultHealth =
                    records.size() > bounds.maximumRecords() || bytes > bounds.maximumEncodedBytes()
                            ? KafkaReplicaJournalHealthV1.OVER_BOUND
                            : health;
            return new KafkaReplicaJournalStorageSnapshotV1(records, resultHealth);
        }

        @Override
        public KafkaReplicaJournalAppendProofV1 appendAndSync(long expectedOrdinal, CanonicalBytes recordBytes) {
            appendCalls++;
            records.add(recordBytes);
            if (appendFailure != null) {
                RuntimeException failure = appendFailure;
                appendFailure = null;
                throw failure;
            }
            KafkaReplicaJournalAppendProofV1 result = proofOverride;
            proofOverride = null;
            return result == null
                    ? new KafkaReplicaJournalAppendProofV1(
                            expectedOrdinal, recordBytes.length(), Sha256Digest.hash(recordBytes))
                    : result;
        }
    }

    static final class MutableResolver implements KafkaReplicaSourceResolverV1 {
        boolean accessible = true;
        boolean withoutPayload = true;
        KafkaReplicaSourceQualificationV1 override;

        @Override
        public KafkaReplicaSourceQualificationV1 qualify(KafkaReplicaCommitDescriptorV1 descriptor) {
            return override == null ? qualification(descriptor, accessible, withoutPayload) : override;
        }
    }

    static final class MutableApplyAdapter implements KafkaReplicaApplyAdapterV1 {
        KafkaReplicaApplyProofV1 override;
        int calls;

        @Override
        public KafkaReplicaApplyProofV1 apply(
                KafkaReplicaCommitDescriptorV1 descriptor, KafkaReplicaSourceQualificationV1 qualification) {
            calls++;
            return override == null ? applyProof(descriptor) : override;
        }
    }
}
