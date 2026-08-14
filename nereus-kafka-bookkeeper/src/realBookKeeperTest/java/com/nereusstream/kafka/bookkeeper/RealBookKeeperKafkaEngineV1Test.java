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

package com.nereusstream.kafka.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaAssignedRecordBatchGroupAdapterV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNbke2AssignedAppendGroupV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionTicketV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolAppendPlanV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendAdmissionRequestV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityBudgetV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaBookKeeperOrderedPipelineV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOffsetAssignedAppendV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadSnapshotV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperTargetedReaderV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRecoveryOutcomeV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRecoveryRequestV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperTakeoverRecoveryV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaElectionKindV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaElectionRecoveryBoundaryV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaRecoveryBatchProtocolAdapterV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunSnapshotV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunStateV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaRunRootAuthority;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.bookkeeper.BookKeeperV3Crc32cAddPayloadLimitV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperClientConfigurationV1;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RealBookKeeperKafkaEngineV1Test {
    private static final int LOG_OVERHEAD = 12;
    private static final int LEADER_EPOCH_OFFSET = 12;
    private static final int MAGIC_OFFSET = 16;
    private static final int CRC_OFFSET = 17;
    private static final int CRC_DOMAIN_OFFSET = 21;
    private static final int LAST_OFFSET_DELTA_OFFSET = 23;
    private static final int BATCH_OVERHEAD = 61;
    private static final AtomicLong RUN_IDS = new AtomicLong(1_000);
    private static BookKeeper client;
    private static BookKeeperCapabilitySnapshotV1 capability;

    @BeforeAll
    static void connectExactClient() throws Exception {
        capability = capability();
        client = BookKeeper.newBuilder(RealBookKeeperClientConfigurationV1.from(
                        System.getProperty("nereus.bookkeeper.metadataServiceUri"), capability))
                .build();
        assertThat(client.isDriverMetadataServiceAvailable().get(10, TimeUnit.SECONDS))
                .isTrue();
    }

    @AfterAll
    static void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void orderedRealDataIsCoherentlyPublishedAndTargetReadByExactEntry() throws Exception {
        Context context = context();
        Plan plan = plan(context.lifecycle(), context.binding(), 2, 100);
        var result = context.pipeline()
                .submit(
                        plan.request(),
                        plan::assignment,
                        context.coordinator().protocolHooks(protocolPlan(context.fence(), 2)))
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        assertThat(context.coordinator().capture().root().frontiers().readableEndOffset())
                .isEqualTo(102);
        assertThat(context.roots().mutations()).isEqualTo(1);

        KafkaBookKeeperReadSnapshotV1 snapshot =
                KafkaBookKeeperReadSnapshotV1.fromActive(context.coordinator().capture());
        KafkaBookKeeperTargetedReaderV1 reader = new KafkaBookKeeperTargetedReaderV1(context.session(), 2);
        var first = reader.readOne(snapshot, 100, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        var second = reader.readOne(snapshot, 101, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertThat(first.outcome()).isEqualTo(KafkaBookKeeperReadOutcomeV1.FOUND);
        assertThat(second.outcome()).isEqualTo(KafkaBookKeeperReadOutcomeV1.FOUND);
        assertThat(first.batches().get(0).entryId()).isEqualTo(1);
        assertThat(second.batches().get(0).entryId()).isEqualTo(2);
        assertThat(context.roots().mutations()).isEqualTo(1);
        context.session().closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void realCheckpointFooterAndCloseSealOneExactNbke2Run() throws Exception {
        Context context = context();
        Plan plan = plan(context.lifecycle(), context.binding(), 1, 100);
        assertThat(context.pipeline()
                        .submit(
                                plan.request(),
                                plan::assignment,
                                context.coordinator().protocolHooks(protocolPlan(context.fence(), 1)))
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);

        Nbke2ProtocolCheckpointV1 checkpoint = new Nbke2ProtocolCheckpointV1(
                context.binding(),
                101,
                101,
                101,
                101,
                CanonicalBytes.copyOf(new byte[] {1}),
                CanonicalBytes.copyOf(new byte[] {2}),
                CanonicalBytes.copyOf(new byte[] {3}));
        long checkpointEntryId = context.lifecycle()
                .appendProtocolCheckpoint(checkpoint)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        context.lifecycle().drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
        KafkaBookKeeperRunSnapshotV1 draining = context.lifecycle().snapshot();
        long footerEntryId = draining.nextEntryId();
        Nbke2RunFooterV1 footer = new Nbke2RunFooterV1(
                context.binding(),
                101,
                footerEntryId + 1,
                -1,
                checkpointEntryId,
                context.binding().creatorOwnerEpoch(),
                List.of());
        KafkaBookKeeperRunSnapshotV1 sealed =
                context.lifecycle().seal(footer).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(sealed.state()).isEqualTo(KafkaBookKeeperRunStateV1.SEALED);
        assertThat(sealed.root().kafkaEndOffsetExclusive()).hasValue(101);
        assertThat(context.roots().mutations()).isEqualTo(2);

        RealBookKeeperCellSessionV1 reader = session();
        assertThat(reader.openRunLedger(sealed.handle())
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .exactHandle())
                .contains(sealed.handle());
        assertThat(Nbke2CodecV1.decode(
                        reader.readExactEntry(sealed.handle(), 0)
                                .toCompletableFuture()
                                .get(10, TimeUnit.SECONDS)
                                .exactEntry()
                                .orElseThrow()
                                .payload()
                                .toByteArray(),
                        sealed.handle().ledgerIdentity().ledgerId(),
                        0))
                .isInstanceOf(Nbke2RunHeaderV1.class);
        assertThat(Nbke2CodecV1.decode(
                        reader.readExactEntry(sealed.handle(), footerEntryId)
                                .toCompletableFuture()
                                .get(10, TimeUnit.SECONDS)
                                .exactEntry()
                                .orElseThrow()
                                .payload()
                                .toByteArray(),
                        sealed.handle().ledgerIdentity().ledgerId(),
                        footerEntryId))
                .isEqualTo(footer);
        reader.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        context.session().closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void realTakeoverFencesAndRecoversTheGreatestCompleteKafkaPrefix() throws Exception {
        Context context = context();
        for (long offset = 100; offset < 103; offset++) {
            Plan plan = plan(context.lifecycle(), context.binding(), 1, offset);
            assertThat(context.pipeline()
                            .submit(
                                    plan.request(),
                                    plan::assignment,
                                    context.coordinator().protocolHooks(protocolPlan(context.fence(), 1)))
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .outcome())
                    .isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        }

        RealBookKeeperCellSessionV1 newOwner = session();
        AtomicLong clock = new AtomicLong();
        KafkaRecoveryBatchProtocolAdapterV1 adapter =
                batch -> KafkaProtocolBatchDeltaV1.nonIdempotent((long) batch.lastOffsetDelta() + 1L);
        KafkaBookKeeperTakeoverRecoveryV1 recovery =
                new KafkaBookKeeperTakeoverRecoveryV1(newOwner, adapter, () -> clock.getAndAdd(100));
        KafkaPartitionFenceV1 recoveredFence = new KafkaPartitionFenceV1(
                context.binding().bindingId(),
                context.binding().topicIncarnation(),
                context.binding().partitionId(),
                13,
                context.binding().storageEpochId(),
                context.binding().creatorOwnerEpoch() + 1,
                context.binding().kafkaLeaderEpoch() + 1);
        KafkaBookKeeperRecoveryRequestV1 request = new KafkaBookKeeperRecoveryRequestV1(
                context.binding(),
                context.lifecycle().snapshot().handle(),
                100,
                OptionalLong.empty(),
                new KafkaBookKeeperRecoveryEnvelopeV1(32, 1_000_000, 1_000_000),
                new KafkaElectionRecoveryBoundaryV1(KafkaElectionKindV1.ISR_ELECTION, 102, 102, 102),
                recoveredFence);
        var recovered = recovery.recover(request).toCompletableFuture().get(30, TimeUnit.SECONDS);

        assertThat(recovered.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_WITH_INERT_RESIDUE);
        assertThat(recovered.physicalRecoveredEndOffset()).isEqualTo(103);
        assertThat(recovered.newLeaderLeo()).hasValue(102);
        assertThat(recovered.progress().entries()).isEqualTo(4);
        context.session().closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        newOwner.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static Context context() throws Exception {
        Nbke2RunBindingV1 binding = binding(RUN_IDS.incrementAndGet());
        RealBookKeeperCellSessionV1 session = session();
        RootAuthority roots = new RootAuthority();
        KafkaBookKeeperRunLifecycleV1 lifecycle = KafkaBookKeeperRunLifecycleV1.createActive(
                        session, roots, binding, 100)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        KafkaPartitionFenceV1 fence = fence(binding);
        KafkaCoherentCommitCoordinatorV1 coordinator = KafkaCoherentCommitCoordinatorV1.bootstrap(
                fence, 100, lifecycle.snapshot().handle(), ignored -> {});
        KafkaAppendCapacityControllerV1 partition =
                new KafkaAppendCapacityControllerV1(new KafkaAppendCapacityBudgetV1(8, 32, 1_000_000));
        KafkaAppendCapacityControllerV1 global =
                new KafkaAppendCapacityControllerV1(new KafkaAppendCapacityBudgetV1(32, 128, 4_000_000));
        KafkaBookKeeperOrderedPipelineV1 pipeline =
                new KafkaBookKeeperOrderedPipelineV1(session, lifecycle, partition, global, coordinator);
        return new Context(binding, session, roots, lifecycle, fence, coordinator, pipeline);
    }

    private static Plan plan(
            KafkaBookKeeperRunLifecycleV1 lifecycle, Nbke2RunBindingV1 binding, int members, long startOffset) {
        List<KafkaNativeAssignedRecordBatchV1> batches = new ArrayList<>();
        for (int index = 0; index < members; index++) {
            batches.add(assigned(startOffset + index));
        }
        KafkaBookKeeperDataAdmissionV1 admission =
                KafkaBookKeeperDataAdmissionV1.admitProfile(binding, capability, 1_000_000);
        List<KafkaBookKeeperDataAdmissionTicketV1> tickets = new ArrayList<>();
        for (int index = 0; index < members; index++) {
            tickets.add(admission.admitBeforeOffsetAllocation(
                    batches.get(index).rawAssignedRecordBatch().length(), index, members));
        }
        KafkaPartitionFenceV1 fence = fence(binding);
        LongFunction<KafkaNbke2AssignedAppendGroupV1> factory =
                firstEntryId -> KafkaAssignedRecordBatchGroupAdapterV1.adapt(
                        fence,
                        binding,
                        firstEntryId,
                        new Id128(0, startOffset + 8),
                        new Id128(0, startOffset + 9),
                        batches,
                        tickets);
        KafkaNbke2AssignedAppendGroupV1 sample = factory.apply(1);
        long encodedBytes = sample
                .encode(lifecycle.snapshot().handle().ledgerIdentity().ledgerId())
                .stream()
                .mapToLong(bytes -> bytes.length())
                .sum();
        return new Plan(
                new KafkaAppendAdmissionRequestV1(members, encodedBytes),
                new KafkaOffsetAssignedAppendV1(startOffset, startOffset + members, factory));
    }

    private static KafkaProtocolAppendPlanV1 protocolPlan(KafkaPartitionFenceV1 fence, int members) {
        List<KafkaProtocolBatchDeltaV1> batches = new ArrayList<>();
        for (int index = 0; index < members; index++) {
            batches.add(KafkaProtocolBatchDeltaV1.nonIdempotent(1));
        }
        return new KafkaProtocolAppendPlanV1(fence, batches);
    }

    private static KafkaNativeAssignedRecordBatchV1 assigned(long baseOffset) {
        byte[] raw = new byte[BATCH_OVERHEAD];
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(0, baseOffset);
        buffer.putInt(Long.BYTES, raw.length - LOG_OVERHEAD);
        buffer.putInt(LEADER_EPOCH_OFFSET, 5);
        buffer.put(MAGIC_OFFSET, (byte) 2);
        buffer.putInt(LAST_OFFSET_DELTA_OFFSET, 0);
        buffer.putLong(43, -1L);
        buffer.putShort(51, (short) -1);
        buffer.putInt(53, -1);
        long crc = crc(raw);
        buffer.putInt(CRC_OFFSET, (int) crc);
        return KafkaNativeAssignedRecordBatchV1.validate(new Facts(raw, baseOffset, crc));
    }

    private static long crc(byte[] raw) {
        CRC32C crc = new CRC32C();
        crc.update(raw, CRC_DOMAIN_OFFSET, raw.length - CRC_DOMAIN_OFFSET);
        return crc.getValue();
    }

    private static KafkaPartitionFenceV1 fence(Nbke2RunBindingV1 binding) {
        return new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                13,
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch());
    }

    private static Nbke2RunBindingV1 binding(long runId) {
        return new Nbke2RunBindingV1(
                new TopicBindingId(digest(10)),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(0, 20)), new KafkaTopicName("k9-real")),
                7,
                new StorageEpochId(digest(30)),
                11,
                5,
                capability.providerScopeId(),
                new StorageRunId(new Id128(0, runId)));
    }

    private static RealBookKeeperCellSessionV1 session() {
        return new RealBookKeeperCellSessionV1(client, capability, new byte[0]);
    }

    private static BookKeeperCapabilitySnapshotV1 capability() {
        int frameLimit = 5_242_880;
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(digest(1)),
                "cd06340851d6d657b7c7546df01df365c18980de",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f")),
                "cd06340851d6d657b7c7546df01df365c18980de",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d")),
                BookKeeperProtocolModeV1.V3,
                frameLimit,
                frameLimit,
                BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(frameLimit, frameLimit),
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(10_000, 5_000, 5_000, 30_000),
                "bk-k0-no-auth:v1",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("eaf41c4b42b767b8ea6e86023a784425b8073f174dbade92b4249c8f3d301dbd")));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }

    private record Plan(KafkaAppendAdmissionRequestV1 request, KafkaOffsetAssignedAppendV1 assignment) {}

    private record Context(
            Nbke2RunBindingV1 binding,
            RealBookKeeperCellSessionV1 session,
            RootAuthority roots,
            KafkaBookKeeperRunLifecycleV1 lifecycle,
            KafkaPartitionFenceV1 fence,
            KafkaCoherentCommitCoordinatorV1 coordinator,
            KafkaBookKeeperOrderedPipelineV1 pipeline) {}

    private record Facts(byte[] rawAssignedRecordBatch, long baseOffset, long crc)
            implements KafkaNativeRecordBatchFactsV1 {
        @Override
        public int batchCount() {
            return 1;
        }

        @Override
        public int completeBytes() {
            return rawAssignedRecordBatch.length;
        }

        @Override
        public long lastOffset() {
            return baseOffset;
        }

        @Override
        public int partitionLeaderEpoch() {
            return 5;
        }

        @Override
        public byte magic() {
            return 2;
        }

        @Override
        public long storedCrc32c() {
            return crc;
        }

        @Override
        public long computedCrc32c() {
            return crc;
        }
    }

    private static final class RootAuthority implements KafkaRunRootAuthority {
        private final Map<StorageRunId, KafkaRunRootSnapshotV1> roots = new LinkedHashMap<>();
        private int mutations;

        @Override
        public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> createRoot(
                KafkaRunRootSnapshotV1 activeCandidate) {
            return mutate(activeCandidate);
        }

        @Override
        public CompletionStage<Optional<KafkaRunRootSnapshotV1>> openRoot(StorageRunId runId) {
            return CompletableFuture.completedFuture(Optional.ofNullable(roots.get(runId)));
        }

        @Override
        public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> sealRoot(
                KafkaRunRootSnapshotV1 expectedActive, KafkaRunRootSnapshotV1 sealedCandidate) {
            if (!expectedActive.equals(roots.get(expectedActive.runId()))) {
                return CompletableFuture.completedFuture(ProviderMutationResultV1.fencedOrConflict());
            }
            return mutate(sealedCandidate);
        }

        @Override
        public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> createSuccessor(
                KafkaRunRootSnapshotV1 expectedSealed, KafkaRunRootSnapshotV1 activeSuccessor) {
            if (!expectedSealed.equals(roots.get(expectedSealed.runId()))) {
                return CompletableFuture.completedFuture(ProviderMutationResultV1.fencedOrConflict());
            }
            return mutate(activeSuccessor);
        }

        int mutations() {
            return mutations;
        }

        private CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> mutate(
                KafkaRunRootSnapshotV1 candidate) {
            roots.put(candidate.runId(), candidate);
            mutations++;
            return CompletableFuture.completedFuture(ProviderMutationResultV1.appliedExact(candidate));
        }
    }
}
