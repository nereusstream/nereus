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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaAssignedProtocolBatchV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProducerBatchResultV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProducerSessionStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind;
import com.nereusstream.storage.bookkeeper.BookKeeperV3Crc32cAddPayloadLimitV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.SimpleRecord;

/** Explicit synthetic control/source authority fixtures; real BK tests reuse only the physical provider path. */
final class KafkaBookKeeperCompactionTestSupportV2 {
    private KafkaBookKeeperCompactionTestSupportV2() {}

    static KafkaBookKeeperCompactionLayoutV2.Layout layout(boolean empty, long attempt) {
        return input(empty, attempt).layout();
    }

    record Input(
            KafkaCompactionRecordsV1.CompactionPlan plan,
            KafkaCompactionSemanticOutputV2 semantic,
            KafkaBookKeeperCompactionLayoutV2.Layout layout,
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence
                    capabilityEvidence) {}

    static Input input(boolean empty, long attempt) {
        return input(empty, attempt, Optional.empty());
    }

    static Input inputWithRecovery(KafkaCompactionRecoveryStateV1 state, long attempt) {
        return input(false, attempt, Optional.of(state));
    }

    private static Input input(boolean empty, long attempt, Optional<KafkaCompactionRecoveryStateV1> state) {
        var bodies = empty
                ? List.of(KafkaSemanticCompactorV1Test.emptyBatch(0, 1))
                : List.of(
                        KafkaSemanticCompactorV1Test.records(
                                0, 1, Compression.NONE, new SimpleRecord(1, new byte[] {1}, new byte[] {4, 5, 6})),
                        KafkaSemanticCompactorV1Test.records(
                                1, 1, Compression.NONE, new SimpleRecord(2, new byte[] {1}, new byte[1024])));
        var proofs = empty
                ? List.<KafkaCompactionRecordsV1.LatestKeyProof>of()
                : List.of(new KafkaCompactionRecordsV1.LatestKeyProof(
                        CanonicalBytes.copyOf(new byte[] {1}), 1, true, true, false, 100));
        return input(bodies, 0, empty ? 1 : 2, proofs, List.of(), attempt, state);
    }

    static Input input(
            List<CanonicalBytes> bodies,
            long start,
            long end,
            List<KafkaCompactionRecordsV1.LatestKeyProof> proofs,
            List<KafkaCompactionRecordsV1.TransactionRange> transactions,
            long attempt) {
        return input(bodies, start, end, proofs, transactions, attempt, Optional.empty());
    }

    private static Input input(
            List<CanonicalBytes> bodies,
            long start,
            long end,
            List<KafkaCompactionRecordsV1.LatestKeyProof> proofs,
            List<KafkaCompactionRecordsV1.TransactionRange> transactions,
            long attempt,
            Optional<KafkaCompactionRecoveryStateV1> state) {
        var fixture = fixture(
                bodies,
                start,
                end,
                new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                        1, digest("capability")),
                state);
        var initial = fixture.plan(proofs, transactions, List.of(), 100, CompressionType.NONE);
        var capabilityEvidence =
                new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence(
                        initial.sourceCut().identity().binding(),
                        1,
                        1,
                        com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind
                                .DURABLE_DRAIN_ONLY_V1,
                        com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState.ADMITTED,
                        digest("synthetic-adapter"),
                        digest("synthetic-backend"),
                        digest("synthetic-contract"),
                        digest("synthetic-verifier"),
                        digest("synthetic-receipt-id"),
                        digest("synthetic-receipt"),
                        digest("synthetic-time"),
                        10_000,
                        0,
                        0);
        var admitted = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                1,
                com.nereusstream.storage.object.read.control.M4ReadControlCodecV1.capabilityEvidenceSha256(
                        capabilityEvidence));
        var plan = fixture(bodies, start, end, admitted, state)
                .plan(proofs, transactions, List.of(), 100, CompressionType.NONE);
        var semantic = new KafkaSemanticCompactorV1().compileSemantic(plan);
        var namespace = new Namespace(
                ProviderKind.BOOKKEEPER,
                CanonicalUtf8.fromString("test-bk-incarnation"),
                CanonicalUtf8.fromString("test-ledgers-generation"));
        var cap = capability(plan.sourceCut().identity().providerScopeSha256());
        var layout = state.isPresent()
                ? KafkaBookKeeperCompactionLayoutV2.plan(
                        plan, semantic, state.orElseThrow(), namespace, cap, attempt, 64 * 1024, 256 * 1024)
                : KafkaBookKeeperCompactionLayoutV2.plan(plan, semantic, namespace, cap, attempt, 512, 1024);
        return new Input(plan, semantic, layout, capabilityEvidence);
    }

    private static KafkaSemanticCompactorV1Test.Fixture fixture(
            List<CanonicalBytes> bodies,
            long start,
            long end,
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding capability,
            Optional<KafkaCompactionRecoveryStateV1> state) {
        if (state.isEmpty()) {
            return new KafkaSemanticCompactorV1Test.Fixture(bodies, start, end, capability);
        }
        var fence = state.orElseThrow().sourceFence();
        var binding = new BindingIdentity(
                fence.bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(fence.topicIncarnation())),
                fence.storageEpochId().digest());
        return new KafkaSemanticCompactorV1Test.Fixture(
                bodies, start, end, capability, binding, state.orElseThrow().sourceCheckpointSha256());
    }

    /** Synthetic full-state fixture; no native source/owner qualification is implied by this data. */
    static KafkaCompactionRecoveryStateV1 recoveryState(boolean large) {
        var binding = KafkaRunTestFixtures.binding(6, 1, 1);
        var fence = new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                1,
                binding.storageEpochId(),
                1,
                1);
        var data = new KafkaProtocolBatchDeltaV1(
                1,
                Optional.of(new KafkaBatchDuplicateIdentityV1(71, (short) 0, 0, 0)),
                KafkaTransactionBatchKindV1.TRANSACTIONAL_DATA,
                71,
                -1);
        var marker = KafkaProtocolBatchDeltaV1.marker(KafkaTransactionBatchKindV1.ABORT_MARKER, 71, (short) 0, 3);
        var commit = new KafkaSpeculativeCommitV1(
                0,
                2,
                fence,
                List.of(new KafkaAssignedProtocolBatchV1(0, 1, data), new KafkaAssignedProtocolBatchV1(1, 2, marker)));
        var producers = new java.util.TreeMap<>(
                KafkaCommittedProducerStateV1.empty().apply(commit).producers());
        if (large) {
            for (long producerId = 1_000; producerId < 21_000; producerId++) {
                var result = new KafkaProducerBatchResultV1(
                        new KafkaBatchDuplicateIdentityV1(producerId, (short) 0, 0, 0), 0, 1, 12);
                producers.put(
                        producerId, KafkaProducerSessionStateV1.first(result).marker((short) 0, 3, 1, 12));
            }
        }
        var checkpoint = new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(binding, 2, 2, 2, 2),
                new KafkaCommittedProducerStateV1(producers),
                KafkaTransactionStateV1.empty().apply(commit),
                KafkaLeaderEpochIndexV1.empty().observe(1, 0));
        return new KafkaCompactionRecoveryStateV1(
                fence,
                2,
                KafkaCoherentCommitCoordinatorV1.recoveryCheckpointContentDigest(checkpoint),
                checkpoint.components());
    }

    static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }

    static List<com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity>
            installReadAuthority(Store store, Input input) {
        store.allowSelectorCas = true;
        return installReadAuthority((CanonicalControlMetadataStore) store, input);
    }

    static List<com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity>
            installReadAuthority(CanonicalControlMetadataStore store, Input input) {
        var cut = input.plan().sourceCut();
        var m4 = new com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1(
                store, 7, cut.identity().binding());
        m4.createCapability(input.capabilityEvidence());
        var keys = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(
                7, cut.identity().binding());
        var sources = cut.sources().stream()
                .map(source -> {
                    if (cut.predecessorSelector().fallbackSetSha256().isPresent()) {
                        return com.nereusstream.storage.object.read.control.M4ReadControlCodecV1.decodeProtection(
                                        store.get(keys.protection(source.sourceIdentitySha256(), 1))
                                                .orElseThrow())
                                .identity();
                    }
                    return new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1
                            .SourceProtectionIdentity(
                            source.sourceIdentitySha256(),
                            1,
                            Math.addExact(cut.predecessorSelector().readAdmissionEpoch(), 1),
                            cut.predecessorSelector().sourceGeneration(),
                            cut.identity().capability());
                })
                .sorted(java.util.Comparator.comparing(
                        source -> source.sourceIdentitySha256().toHex()))
                .toList();
        for (var source : sources) {
            m4.createProtection(
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection(
                            cut.identity().binding(),
                            source,
                            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState
                                    .PROTECTED,
                            Optional.empty(),
                            Optional.empty()));
        }
        m4.createSelector(cut.predecessorSelector());
        return sources;
    }

    private static BookKeeperCapabilitySnapshotV1 capability(Sha256Digest scope) {
        int limit = 5_242_880;
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(scope),
                "cd06340851d6d657b7c7546df01df365c18980de",
                sha("8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f"),
                "cd06340851d6d657b7c7546df01df365c18980de",
                sha("c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"),
                BookKeeperProtocolModeV1.V3,
                limit,
                limit,
                BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(limit, limit),
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(10_000, 5_000, 5_000, 30_000),
                "bk-k0-no-auth:v1",
                sha("eaf41c4b42b767b8ea6e86023a784425b8073f174dbade92b4249c8f3d301dbd"));
    }

    private static Sha256Digest sha(String value) {
        return Sha256Digest.copyOf(HexFormat.of().parseHex(value));
    }

    static final class Store implements CanonicalControlMetadataStore {
        final Map<String, CanonicalBytes> values = new LinkedHashMap<>();
        final List<String> operations = new ArrayList<>();
        boolean loseNextPutResponse;
        boolean dropNextPut;
        boolean allowSelectorCas;
        boolean loseNextSelectorCasResponse;
        boolean dropNextSelectorCas;
        int selectorCasCount;
        Runnable beforeNextSelectorCas;

        @Override
        public synchronized Optional<CanonicalBytes> get(String key) {
            operations.add("read:" + key);
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public synchronized ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
            operations.add("put:" + key);
            if (dropNextPut) {
                dropNextPut = false;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            boolean inserted = values.putIfAbsent(key, value) == null;
            if (loseNextPutResponse) {
                loseNextPutResponse = false;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            return inserted ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        @Override
        public synchronized ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
            if (!allowSelectorCas || !key.endsWith("/selector")) {
                throw new AssertionError("immutable BK inventory must never overwrite a prior allocation");
            }
            selectorCasCount++;
            Runnable before = beforeNextSelectorCas;
            beforeNextSelectorCas = null;
            if (before != null) {
                before.run();
            }
            if (dropNextSelectorCas) {
                dropNextSelectorCas = false;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            boolean matches = Optional.ofNullable(values.get(key)).equals(expected);
            if (matches) {
                values.put(key, value);
            }
            if (loseNextSelectorCasResponse) {
                loseNextSelectorCasResponse = false;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            return matches ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }
    }
}
