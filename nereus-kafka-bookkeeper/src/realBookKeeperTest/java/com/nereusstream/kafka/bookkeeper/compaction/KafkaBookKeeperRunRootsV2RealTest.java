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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2AppendGroupDescriptorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperNativeRootVerifierV2;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunStateV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunRetirementPermitV1;
import com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaRunRootAuthorityV2;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2.Scope;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Real BK/Oxia admission and replay; Controller identity and GC eligibility are fixtures. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class KafkaBookKeeperRunRootsV2RealTest {
    private static final List<String> TOPICS = List.of("__consumer_offsets", "__transaction_state");

    @Test
    void brokerPartitionCompositionContinuesSharedCommitAndFetchAcrossTwoColdOwners() throws Exception {
        try (var f = new Fixture(1594, "broker-composition", null)) {
            var envelope = new com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1(
                    100, 1_000_000, TimeUnit.SECONDS.toNanos(30));
            var global = new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1(
                    new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityBudgetV1(8, 16, 1_000_000));
            var current = new java.util.concurrent.atomic.AtomicInteger(1);
            var first = await(openBrokerPartition(f, 1, Optional.empty(), envelope, global, current));
            var data = nativeTransactional(0, 71, 0, "transaction before cold takeover");
            try (var admission = first.admit(List.of(data.length()))) {
                assertThat(await(first.appendAssigned(admission, List.of(assigned(data))))
                                .outcome())
                        .isEqualTo(
                                com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1
                                        .COMMITTED_ORDERED);
            }
            assertThat(first.capture().root().frontiers().highWatermark()).isEqualTo(1);
            assertThat(first.capture().root().frontiers().lastStableOffset()).isZero();
            await(first.resign());
            var closed = await(f.roots.readClosedOwner(1)).orElseThrow();
            current.set(2);
            var second = await(openBrokerPartition(f, 2, Optional.of(closed), envelope, global, current));
            assertThat(second.capture().root().frontiers().highWatermark()).isEqualTo(1);
            assertThat(second.capture().root().frontiers().lastStableOffset()).isZero();
            assertThat(second.capture()
                            .committedProducerState()
                            .findDuplicate(new com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1(
                                    71, (short) 0, 0, 0)))
                    .get()
                    .extracting(com.nereusstream.kafka.bookkeeper.commit.KafkaProducerBatchResultV1::startOffset)
                    .isEqualTo(0L);
            var marker = canonical(org.apache.kafka.common.record.MemoryRecords.withEndTransactionMarker(
                    1,
                    100,
                    2,
                    71,
                    (short) 0,
                    new org.apache.kafka.common.record.EndTransactionMarker(
                            org.apache.kafka.common.record.ControlRecordType.COMMIT, 3)));
            try (var admission = second.admit(List.of(marker.length()))) {
                assertThat(await(second.appendAssigned(admission, List.of(assigned(marker))))
                                .outcome())
                        .isEqualTo(
                                com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1
                                        .COMMITTED_ORDERED);
            }
            assertThat(second.capture().root().frontiers().highWatermark()).isEqualTo(2);
            assertThat(second.capture().root().frontiers().lastStableOffset()).isEqualTo(2);
            var request = new com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1(
                    0,
                    com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1.READ_COMMITTED,
                    10_000,
                    Optional.empty());
            assertThat(await(second.read(request)).batches())
                    .extracting(
                            com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadBatchV1::rawAssignedRecordBatch)
                    .containsExactly(data, marker);
            await(second.resign());
            var closed2 = await(f.roots.readClosedOwner(2)).orElseThrow();
            current.set(3);
            var third = await(openBrokerPartition(f, 3, Optional.of(closed2), envelope, global, current));
            assertThat(third.capture().root().frontiers().highWatermark()).isEqualTo(2);
            assertThat(third.capture().root().frontiers().lastStableOffset()).isEqualTo(2);
            assertThat(await(third.read(request)).batches())
                    .extracting(
                            com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadBatchV1::rawAssignedRecordBatch)
                    .containsExactly(data, marker);
            await(third.resign());
        }
    }

    @Test
    void smallRunBudgetRollsRepeatedlyAndRestoresCheckpointTailWithCrossRunTransactions() throws Exception {
        try (var f = new Fixture(System.currentTimeMillis(), "checkpoint-rollover", null)) {
            var envelope = new com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1(
                    8, 8_192, TimeUnit.SECONDS.toNanos(30));
            var global = new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1(
                    new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityBudgetV1(8, 16, 1_000_000));
            var current = new java.util.concurrent.atomic.AtomicInteger(1);
            var first = await(openBrokerPartition(f, 1, Optional.empty(), envelope, global, current));
            for (int offset = 0; offset < 40; offset++) {
                var data = nativeTransactional(offset, 71, offset, "transaction across checkpoint " + offset);
                try (var admission = first.admit(List.of(data.length()))) {
                    assertThat(await(first.appendAssigned(admission, List.of(assigned(data))))
                                    .outcome())
                            .isEqualTo(
                                    com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1
                                            .COMMITTED_ORDERED);
                }
            }
            assertThat(first.capture().root().frontiers().highWatermark()).isEqualTo(40);
            assertThat(first.capture().root().frontiers().lastStableOffset()).isZero();
            var admitted = await(f.roots.readOwnerAdmission()).orElseThrow();
            assertThat(admitted.runs().size()).isGreaterThan(10);
            assertThat(f.source.spec().configurations()).hasSize(1);
            var request = new com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1(
                    0,
                    com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1.READ_UNCOMMITTED,
                    50_000,
                    Optional.empty());
            var firstRead = await(first.read(request));
            assertThat(firstRead.outcome())
                    .as(firstRead.detail())
                    .isEqualTo(com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1.FOUND);
            assertThat(firstRead.batches()).hasSize(40);
            await(first.resign());
            var closed = await(f.roots.readClosedOwner(1)).orElseThrow();
            var fence = new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(
                    f.scope.bindingId(), f.scope.topic(), 0, 1, f.scope.storageEpoch(), 2, 2);
            var recovered = await(new com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1(
                            f.admitting,
                            f.roots,
                            new com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1(),
                            System::nanoTime)
                    .recover(closed, fence, 0, envelope));
            assertThat(recovered.selectedCheckpointEndOffset()).hasValue(39);
            assertThat(recovered.progress().entries()).isEqualTo(3);
            assertThat(recovered.runs()).hasSize(1);
            current.set(2);
            var second = await(openBrokerPartition(f, 2, Optional.of(closed), envelope, global, current));
            assertThat(second.capture()
                            .committedProducerState()
                            .findDuplicate(new com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1(
                                    71, (short) 0, 39, 39)))
                    .get()
                    .extracting(com.nereusstream.kafka.bookkeeper.commit.KafkaProducerBatchResultV1::startOffset)
                    .isEqualTo(39L);
            var marker = canonical(org.apache.kafka.common.record.MemoryRecords.withEndTransactionMarker(
                    40,
                    100,
                    2,
                    71,
                    (short) 0,
                    new org.apache.kafka.common.record.EndTransactionMarker(
                            org.apache.kafka.common.record.ControlRecordType.COMMIT, 3)));
            try (var admission = second.admit(List.of(marker.length()))) {
                await(second.appendAssigned(admission, List.of(assigned(marker))));
            }
            assertThat(second.capture().root().frontiers().lastStableOffset()).isEqualTo(41);
            for (int offset = 41; offset < 60; offset++) {
                var data = nativeTransactional(offset, 72, offset - 41, "later transaction " + offset, 2);
                try (var admission = second.admit(List.of(data.length()))) {
                    await(second.appendAssigned(admission, List.of(assigned(data))));
                }
            }
            assertThat(second.capture().root().frontiers().lastStableOffset()).isEqualTo(41);
            await(second.resign());
            current.set(3);
            var third = await(openBrokerPartition(f, 3, await(f.roots.readClosedOwner(2)), envelope, global, current));
            assertThat(third.capture().root().frontiers().highWatermark()).isEqualTo(60);
            assertThat(third.capture().root().frontiers().lastStableOffset()).isEqualTo(41);
            var abort = canonical(org.apache.kafka.common.record.MemoryRecords.withEndTransactionMarker(
                    60,
                    100,
                    3,
                    72,
                    (short) 0,
                    new org.apache.kafka.common.record.EndTransactionMarker(
                            org.apache.kafka.common.record.ControlRecordType.ABORT, 4)));
            try (var admission = third.admit(List.of(abort.length()))) {
                await(third.appendAssigned(admission, List.of(assigned(abort))));
            }
            assertThat(third.capture().root().frontiers().lastStableOffset()).isEqualTo(61);
            assertThat(await(third.read(request)).batches()).hasSize(61);
            await(third.resign());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unknownCheckpointAndCloseStopAllocationAndPreserveAckedPrefix(boolean closeFault) throws Exception {
        try (var f = new Fixture(System.currentTimeMillis(), "checkpoint-unknown-" + closeFault, null)) {
            var expected = new java.util.ArrayList<CanonicalBytes>();
            var envelope = new com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1(
                    8, 8_192, TimeUnit.SECONDS.toNanos(30));
            var global = new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1(
                    new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityBudgetV1(8, 16, 1_000_000));
            var current = new java.util.concurrent.atomic.AtomicInteger(1);
            var first = await(openBrokerPartition(f, 1, Optional.empty(), envelope, global, current));
            for (int offset = 0; offset < 3; offset++) {
                var data = nativeTransactional(offset, 81, offset, "acked-" + offset);
                expected.add(data);
                try (var admission = first.admit(List.of(data.length()))) {
                    assertThat(await(first.appendAssigned(admission, List.of(assigned(data))))
                                    .outcome())
                            .isEqualTo(
                                    com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1
                                            .COMMITTED_ORDERED);
                }
            }
            f.loseCheckpoint = !closeFault;
            f.loseClose = closeFault;
            var next = nativeTransactional(3, 81, 3, "not allocated");
            assertThatThrownBy(() -> first.admit(List.of(next.length()))).hasMessageContaining("rollover");
            assertThat(first.capture().root().frontiers().allocatedEndOffset()).isEqualTo(3);
            assertThat(await(f.roots.readOwnerAdmission()).orElseThrow().runs()).hasSize(1);
            assertThatThrownBy(() -> first.admit(List.of(next.length()))).hasMessageContaining("fenced");
            assertThat(await(f.roots.closeOwner(first.owner())).exactProof()).isPresent();
            current.set(2);
            var second = await(openBrokerPartition(f, 2, await(f.roots.readClosedOwner(1)), envelope, global, current));
            assertThat(second.capture().root().frontiers().highWatermark()).isEqualTo(3);
            assertThat(second.capture().root().frontiers().lastStableOffset()).isZero();
            var marker = canonical(org.apache.kafka.common.record.MemoryRecords.withEndTransactionMarker(
                    3,
                    100,
                    2,
                    81,
                    (short) 0,
                    new org.apache.kafka.common.record.EndTransactionMarker(
                            org.apache.kafka.common.record.ControlRecordType.COMMIT, 3)));
            expected.add(marker);
            try (var admission = second.admit(List.of(marker.length()))) {
                assertThat(await(second.appendAssigned(admission, List.of(assigned(marker))))
                                .outcome())
                        .isEqualTo(
                                com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1
                                        .COMMITTED_ORDERED);
            }
            assertThat(second.capture().root().frontiers().lastStableOffset()).isEqualTo(4);
            var request = new com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1(
                    0,
                    com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1.READ_COMMITTED,
                    50_000,
                    Optional.empty());
            assertThat(await(second.read(request)).batches()).hasSize(4);
            await(second.resign());
            var closed = await(f.roots.readClosedOwner(2)).orElseThrow();
            var firstRoot = await(f.roots.readAdmittedRun(await(f.roots.readClosedOwner(1))
                            .orElseThrow()
                            .runs()
                            .get(0)))
                    .orElseThrow();
            assertThat(firstRoot.recoveryCut()).isPresent();
            current.set(3);
            var third = await(openBrokerPartition(f, 3, Optional.of(closed), envelope, global, current));
            assertThat(third.capture().root().frontiers().highWatermark()).isEqualTo(4);
            assertThat(third.capture().root().frontiers().lastStableOffset()).isEqualTo(4);
            var recovered = await(new com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1(
                            f.admitting,
                            f.roots,
                            new com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1(),
                            System::nanoTime)
                    .recover(
                            closed,
                            new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(
                                    f.scope.bindingId(), f.scope.topic(), 0, 1, f.scope.storageEpoch(), 3, 3),
                            0,
                            envelope));
            assertThat(recovered.selectedCheckpointEndOffset()).hasValue(3);
            assertThat(recovered.progress().entries()).isEqualTo(3);
            assertThat(recovered.endOffset()).isEqualTo(4);
            var retry = nativeTransactional(2, 81, 2, "acked-2", 3);
            try (var admission = third.admit(List.of(retry.length()))) {
                var duplicate = await(third.appendAssigned(admission, List.of(assigned(retry))));
                assertThat(duplicate.outcome())
                        .isEqualTo(
                                com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1
                                        .COMMITTED_ORDERED);
                assertThat(duplicate.startOffset()).hasValue(2);
                assertThat(duplicate.endOffsetExclusive()).hasValue(3);
            }
            assertThat(third.capture().root().frontiers().allocatedEndOffset()).isEqualTo(4);
            assertThat(await(third.read(request)).batches())
                    .extracting(
                            com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadBatchV1::rawAssignedRecordBatch)
                    .containsExactlyElementsOf(expected);
            var later = nativeTransactional(4, 81, 3, "after repeated takeover", 3);
            try (var admission = third.admit(List.of(later.length()))) {
                var appended = await(third.appendAssigned(admission, List.of(assigned(later))));
                assertThat(appended.outcome())
                        .isEqualTo(
                                com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1
                                        .COMMITTED_ORDERED);
                assertThat(appended.startOffset()).hasValue(4);
                assertThat(appended.endOffsetExclusive()).hasValue(5);
            }
            assertThat(third.capture().root().frontiers().highWatermark()).isEqualTo(5);
            assertThat(third.capture().root().frontiers().lastStableOffset()).isEqualTo(4);
            assertThat(await(third.read(request)).batches()).hasSize(4);
            expected.add(later);
            assertThat(await(third.read(
                                    new com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1(
                                            0,
                                            com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1
                                                    .READ_UNCOMMITTED,
                                            50_000,
                                            Optional.empty())))
                            .batches())
                    .extracting(
                            com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadBatchV1::rawAssignedRecordBatch)
                    .containsExactlyElementsOf(expected);
            await(third.resign());
        }
    }

    @Test
    void unknownSuccessorAttachmentRemainsInClosedMembershipAndCannotAllocate() throws Exception {
        try (var f = new Fixture(System.currentTimeMillis(), "rollover-attach-unknown", null)) {
            var envelope = new com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1(
                    8, 8_192, TimeUnit.SECONDS.toNanos(30));
            var global = new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1(
                    new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityBudgetV1(8, 16, 1_000_000));
            var current = new java.util.concurrent.atomic.AtomicInteger(1);
            var faults = new Faults(f.nativeClient);
            var roots = f.faulted(faults);
            var first = await(openBrokerPartition(f, roots, 1, Optional.empty(), envelope, global, current));
            for (int offset = 0; offset < 3; offset++) {
                var data = nativeTransactional(offset, 91, offset, "before lost attach " + offset);
                try (var admission = first.admit(List.of(data.length()))) {
                    assertThat(await(first.appendAssigned(admission, List.of(assigned(data))))
                                    .outcome())
                            .isEqualTo(
                                    com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1
                                            .COMMITTED_ORDERED);
                }
            }
            faults.loseAdmission = true;
            assertThatThrownBy(() -> first.admit(List.of(
                            nativeTransactional(3, 91, 3, "not allocated").length())))
                    .hasMessageContaining("rollover");
            assertThat(first.capture().root().frontiers().allocatedEndOffset()).isEqualTo(3);
            faults.blocked = false;
            var closure = await(f.roots.closeOwner(first.owner())).exactProof().orElseThrow();
            assertThat(closure.runs()).hasSize(2);
            current.set(2);
            var second = await(openBrokerPartition(f, 2, Optional.of(closure), envelope, global, current));
            assertThat(second.capture().root().frontiers().highWatermark()).isEqualTo(3);
            var request = new com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1(
                    0,
                    com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1.READ_UNCOMMITTED,
                    50_000,
                    Optional.empty());
            assertThat(await(second.read(request)).batches()).hasSize(3);
            await(second.resign());
        }
    }

    private static CompletionStage<com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1>
            openBrokerPartition(
                    Fixture f,
                    int epoch,
                    Optional<com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1> closed,
                    com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1 envelope,
                    com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1 global,
                    java.util.concurrent.atomic.AtomicInteger current) {
        return openBrokerPartition(f, f.roots, epoch, closed, envelope, global, current);
    }

    private static CompletionStage<com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1>
            openBrokerPartition(
                    Fixture f,
                    OxiaKafkaRunRootAuthorityV2 roots,
                    int epoch,
                    Optional<com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1> closed,
                    com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1 envelope,
                    com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1 global,
                    java.util.concurrent.atomic.AtomicInteger current) {
        var original = f.runBinding(epoch - 1);
        var binding = new Nbke2RunBindingV1(
                original.bindingId(),
                original.topicIncarnation(),
                original.partitionId(),
                original.storageEpochId(),
                epoch,
                epoch,
                original.providerScopeId(),
                original.runId());
        var fence = new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(
                f.scope.bindingId(), f.scope.topic(), 0, 1, f.scope.storageEpoch(), epoch, epoch);
        return com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.open(
                f.admitting,
                roots,
                roots,
                new com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1(epoch, epoch, epoch, epoch, epoch),
                binding,
                fence,
                closed,
                new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1(
                        new com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityBudgetV1(8, 16, 1_000_000)),
                global,
                envelope,
                10_000,
                ignored -> {},
                () -> current.get() == epoch);
    }

    private static com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1 assigned(
            CanonicalBytes bytes) {
        return com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1.validate(
                com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1.parse(bytes));
    }

    @Test
    void crashRunCutPermitsANewNativeWriterAndRecoveryOfATransactionAcrossOwners() throws Exception {
        try (var f = new Fixture(1593, "crash-successor", null)) {
            var old = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, f.roots, f.runBinding(0), 0));
            writeNative(f, old, 0, nativeTransactional(0, 71, 0, "transaction before failure"));
            // The next multi-member append reached BK only for its first entry. Its offset is not adoptable.
            var partial = old.reserveDataGroup(2);
            f.append(
                    old.snapshot().handle(),
                    partial.firstEntryId(),
                    Nbke2CodecV1.encode(
                            old.snapshot().handle().ledgerIdentity().ledgerId(),
                            partial.firstEntryId(),
                            new Nbke2DataV1(
                                    old.snapshot().runBinding(),
                                    1,
                                    0,
                                    0,
                                    2,
                                    new Id128(1593, 991),
                                    new Id128(1593, 992),
                                    Optional.empty(),
                                    nativeTransactional(1, 71, 1, "incomplete append"))));
            var owner = new com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1(1, 1, 1, 1, 1);
            var closed = await(f.roots.closeOwner(owner)).exactProof().orElseThrow();
            var fence = new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(
                    f.scope.bindingId(), f.scope.topic(), 0, 1, f.scope.storageEpoch(), 2, 2);
            var reads = f.source.newSession();
            try {
                var recovery = new com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1(
                        reads,
                        f.roots,
                        new com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1(),
                        System::nanoTime);
                var envelope = new com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1(
                        100, 1_000_000, TimeUnit.SECONDS.toNanos(30));
                var recovered = await(recovery.recover(closed, fence, 0, envelope));
                var exact = recovered.runs().get(0);
                assertThat(exact.recovery().outcome())
                        .isEqualTo(
                                com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRecoveryOutcomeV1
                                        .RECOVERED_WITH_INERT_RESIDUE);
                assertThat(exact.recovery().conflictEntryId()).hasValue(partial.firstEntryId());
                var oldPayload = ImmutableRetainedStoragePayload.copyOf(new byte[] {1});
                try {
                    assertThat(await(f.session.appendExplicitEntry(new RunLedgerAppendRequestV1(
                                            old.snapshot().handle(), partial.firstEntryId() + 1, oldPayload)))
                                    .outcome())
                            .isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
                } finally {
                    assertThat(oldPayload.release()).isTrue();
                }
                assertThat(await(f.roots.sealRecoveredRun(
                                        exact.root().root(), closed, exact.fenceProof(), 2, OptionalLong.empty()))
                                .outcome())
                        .isEqualTo(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN);
                var sealed = await(f.roots.sealRecoveredRun(
                                exact.root().root(),
                                closed,
                                exact.fenceProof(),
                                1,
                                OptionalLong.of(partial.firstEntryId())))
                        .exactProof()
                        .orElseThrow();
                var stored = f.stored(sealed);
                assertThat(stored.recoveryCut()).isPresent();
                assertThat(KafkaRunRootRecordV2.decode(stored.encode())).isEqualTo(stored);
                assertThat(await(f.roots.sealRecoveredRun(
                                        exact.root().root(),
                                        closed,
                                        exact.fenceProof(),
                                        1,
                                        OptionalLong.of(partial.firstEntryId())))
                                .exactProof())
                        .contains(sealed);
                var owner2 = new com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1(2, 2, 2, 2, 2);
                assertThat(await(f.roots.openOwner(owner2, Optional.of(closed))).exactProof())
                        .isPresent();
                var b = f.runBinding(1);
                var nextBinding = new Nbke2RunBindingV1(
                        b.bindingId(),
                        b.topicIncarnation(),
                        b.partitionId(),
                        b.storageEpochId(),
                        2,
                        2,
                        b.providerScopeId(),
                        b.runId());
                var next = await(
                        KafkaBookKeeperRunLifecycleV1.createAfterRecovery(f.admitting, f.roots, sealed, nextBinding));
                var marker = canonical(org.apache.kafka.common.record.MemoryRecords.withEndTransactionMarker(
                        1,
                        100,
                        2,
                        71,
                        (short) 0,
                        new org.apache.kafka.common.record.EndTransactionMarker(
                                org.apache.kafka.common.record.ControlRecordType.COMMIT, 3)));
                writeNative(f, next, 1, marker);
                var closed2 = await(f.roots.closeOwner(owner2)).exactProof().orElseThrow();
                var fence3 = new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(
                        f.scope.bindingId(), f.scope.topic(), 0, 1, f.scope.storageEpoch(), 3, 3);
                var repeated = await(recovery.recover(closed2, fence3, 0, envelope));
                assertThat(repeated.endOffset()).isEqualTo(2);
                assertThat(repeated.protocolState()
                                .orElseThrow()
                                .transactionState()
                                .firstUnstableOffset(2))
                        .isEmpty();
                assertThat(repeated.protocolState()
                                .orElseThrow()
                                .transactionState()
                                .completedTransactions())
                        .singleElement()
                        .satisfies(transaction -> {
                            assertThat(transaction.firstOffset()).isZero();
                            assertThat(transaction.markerEndOffsetExclusive()).isEqualTo(2);
                            assertThat(transaction.aborted()).isFalse();
                        });
                assertThat(repeated.runs().get(0).root())
                        .isEqualTo(stored.select(f.stored(next.snapshot().root())));
                assertThat(f.stored(sealed).initialLink())
                        .isEqualTo(exact.root().initialLink());
            } finally {
                await(reads.closeAsync());
            }
        }
    }

    @Test
    void closedMultiRunHistoryRecoversNativeTransactionsAndTheSameOffsetsTwice() throws Exception {
        try (var f = new Fixture(1592, "cold-history", null)) {
            var first = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, f.roots, f.runBinding(0), 0));
            var firstData = nativeTransactional(0, 71, 0, "first transaction");
            writeNative(f, first, 0, firstData);
            await(first.drain());
            await(first.seal(
                    f.footer(first.snapshot().runBinding(), 1, first.snapshot().nextEntryId())));
            var second = await(first.createSuccessor(f.runBinding(1)));
            var marker = canonical(org.apache.kafka.common.record.MemoryRecords.withEndTransactionMarker(
                    1,
                    100,
                    1,
                    71,
                    (short) 0,
                    new org.apache.kafka.common.record.EndTransactionMarker(
                            org.apache.kafka.common.record.ControlRecordType.ABORT, 3)));
            writeNative(f, second, 1, marker);
            var openData = nativeTransactional(2, 72, 0, "ongoing transaction");
            writeNative(f, second, 2, openData);

            var owner = new com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1(1, 1, 1, 1, 1);
            var closed = await(f.roots.closeOwner(owner)).exactProof().orElseThrow();
            assertThat(closed.runs()).hasSize(2);
            var fence = new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(
                    f.scope.bindingId(), f.scope.topic(), f.scope.partition(), 1, f.scope.storageEpoch(), 2, 2);
            var nativeAdapter = new com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1();
            var reads = f.source.newSession();
            try {
                var recovery = new com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1(
                        reads,
                        f.roots,
                        batch -> {
                            // The first replayed batch is reached only after every legal ledger is closed natively.
                            for (var handle : f.handles.values()) {
                                assertThat(f.source
                                                .captureExactTarget(handle)
                                                .toCompletableFuture()
                                                .join()
                                                .exactTarget())
                                        .isPresent();
                            }
                            return nativeAdapter.protocolDelta(batch);
                        },
                        System::nanoTime);
                var envelope = new com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1(
                        100, 1_000_000, TimeUnit.SECONDS.toNanos(30));
                var recovered = await(recovery.recover(closed, fence, 0, envelope));
                assertThat(recovered.endOffset()).isEqualTo(3);
                var state = recovered.protocolState().orElseThrow();
                assertThat(state.producerState()
                                .findDuplicate(
                                        new com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1(
                                                71, (short) 0, 0, 0)))
                        .get()
                        .satisfies(result -> {
                            assertThat(result.startOffset()).isZero();
                            assertThat(result.endOffsetExclusive()).isEqualTo(1);
                        });
                assertThat(state.producerState().producers().get(71L).lastSequence())
                        .isZero();
                assertThat(state.producerState().producers().get(71L).coordinatorEpoch())
                        .isEqualTo(3);
                assertThat(state.transactionState().abortedTransactions())
                        .singleElement()
                        .satisfies(aborted -> {
                            assertThat(aborted.firstOffset()).isZero();
                            assertThat(aborted.markerEndOffsetExclusive()).isEqualTo(2);
                        });
                assertThat(state.transactionState().firstUnstableOffset(3)).hasValue(2);
                assertThat(recovered.readTable(1).runs()).hasSize(2);
                assertThat(recovered
                                .readTable(1)
                                .floorOrSuccessor(2)
                                .orElseThrow()
                                .activeIndex()
                                .orElseThrow()
                                .floorOrSuccessor(2)
                                .orElseThrow()
                                .entryId())
                        .isEqualTo(2);

                // A failed takeover with no admitted new run still links the identical immutable prior history.
                var owner2 = new com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1(2, 2, 2, 2, 2);
                assertThat(await(f.roots.openOwner(owner2, Optional.of(closed))).exactProof())
                        .isPresent();
                var closed2 = await(f.roots.closeOwner(owner2)).exactProof().orElseThrow();
                var fence3 = new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(
                        f.scope.bindingId(), f.scope.topic(), f.scope.partition(), 1, f.scope.storageEpoch(), 3, 3);
                var repeated = await(recovery.recover(closed2, fence3, 0, envelope));
                assertThat(repeated.endOffset()).isEqualTo(recovered.endOffset());
                assertThat(repeated.protocolState()).isEqualTo(recovered.protocolState());
                assertThat(repeated.runs().stream()
                                .map(run -> run.root().initialLink())
                                .toList())
                        .containsExactlyElementsOf(closed.runs());
            } finally {
                await(reads.closeAsync());
            }
        }
    }

    private static CanonicalBytes nativeTransactional(long offset, long producerId, int sequence, String value) {
        return nativeTransactional(offset, producerId, sequence, value, 1);
    }

    private static CanonicalBytes nativeTransactional(
            long offset, long producerId, int sequence, String value, int epoch) {
        return canonical(org.apache.kafka.common.record.MemoryRecords.withTransactionalRecords(
                org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2,
                offset,
                org.apache.kafka.common.compress.Compression.NONE,
                producerId,
                (short) 0,
                sequence,
                epoch,
                new org.apache.kafka.common.record.SimpleRecord(
                        value.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    }

    private static byte[] canonicalBuffer(java.nio.ByteBuffer source) {
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        return bytes;
    }

    private static CanonicalBytes canonical(org.apache.kafka.common.record.MemoryRecords records) {
        byte[] bytes = new byte[records.sizeInBytes()];
        records.buffer().duplicate().get(bytes);
        return CanonicalBytes.copyOf(bytes);
    }

    private static void writeNative(
            Fixture fixture, KafkaBookKeeperRunLifecycleV1 run, long offset, CanonicalBytes body) throws Exception {
        var reservation = run.reserveDataGroup(1);
        fixture.data(run.snapshot().handle(), run.snapshot().runBinding(), reservation.firstEntryId(), offset, body);
        run.completeDataGroup(reservation);
    }

    @Test
    void nativeLifecyclePublishesSealedRootsAndDataForBothInternalTopics() throws Exception {
        for (int i = 0; i < TOPICS.size(); i++) {
            try (var f = new Fixture(1501 + i, TOPICS.get(i), null)) {
                var run = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, f.roots, f.runBinding(0), 0));
                assertThat(await(f.source.captureExactTarget(run.snapshot().handle()))
                                .exactTarget())
                        .isEmpty();
                f.writeData(run);
                await(run.drain());
                var sealed = await(run.seal(f.footer(
                                run.snapshot().runBinding(), 2, run.snapshot().nextEntryId())))
                        .root();
                assertThat(await(f.roots.openRoot(sealed.runId()))).contains(sealed);
                var next = await(run.createSuccessor(f.runBinding(1)));
                assertThat(next.snapshot().root().kafkaStartOffset()).isEqualTo(2);
                assertThat(await(f.roots.openRoot(next.snapshot().root().runId())))
                        .contains(next.snapshot().root());
                assertThat(f.record(sealed).initialLink())
                        .isEqualTo(f.stored(sealed).initialLink());
                f.verifyData(run.snapshot().handle());
                assertThat(f.tickets(sealed.ledgerIdentity())).isZero();
                assertThat(f.tickets(next.snapshot().handle().ledgerIdentity())).isZero();
                await(next.drain());
                await(next.seal(f.footer(
                        next.snapshot().runBinding(), 2, next.snapshot().nextEntryId())));
            }
        }
    }

    @Test
    void wrongNativeHeaderAndFooterCannotPublishAndExactRootsReconcileRejectedAttempts() throws Exception {
        try (var f = new Fixture(1503, "orders", null)) {
            var root = f.prepareSealedSource();
            var wrongHeader = new KafkaRunRootSnapshotV1(
                    root.bindingId(),
                    root.topicIncarnation(),
                    root.partitionId(),
                    root.storageEpochId(),
                    root.creatorOwnerEpoch(),
                    root.kafkaLeaderEpoch(),
                    root.providerScopeId(),
                    root.runId(),
                    root.ledgerIdentity(),
                    1,
                    OptionalLong.empty(),
                    KafkaRunRootStateV1.ACTIVE,
                    Optional.empty());
            assertThat(await(f.roots.createRoot(wrongHeader)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN);
            assertThat(await(f.nativeClient.read(f.roots.nativeRootKey(root.runId()))))
                    .isEmpty();
            assertThat(f.tickets(root.ledgerIdentity())).isEqualTo(1);
            assertThat(await(f.roots.createRoot(root)).exactProof()).contains(root);
            assertThat(await(f.roots.createRoot(wrongHeader)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
            assertThat(f.tickets(root.ledgerIdentity())).isZero();
            var wrongFooter = sealed(root, 3);
            assertThat(await(f.roots.sealRoot(root, wrongFooter)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN);
            assertThat(await(f.roots.openRoot(root.runId()))).contains(root);
            assertThat(await(f.roots.sealRoot(root, sealed(root, 2))).exactProof())
                    .contains(sealed(root, 2));
            assertThat(await(f.roots.sealRoot(root, wrongFooter)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
            assertThat(f.tickets(root.ledgerIdentity())).isZero();
        }
    }

    @Test
    void nativeOwnerClosureFencesLateRolloverAndRepeatsTheSameLegalRunSet() throws Exception {
        try (var f = new Fixture(1591, "orders", null)) {
            var faults = new Faults(f.nativeClient);
            var roots = f.faulted(faults);
            var run = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, roots, f.runBinding(0), 0));
            f.writeData(run);
            await(run.drain());
            var sealed = await(run.seal(f.footer(
                            run.snapshot().runBinding(), 2, run.snapshot().nextEntryId())))
                    .root();
            faults.holdKey = roots.nativeOwnerKey();
            var late = run.createSuccessor(f.runBinding(1)).toCompletableFuture();
            try {
                faults.held.get(30, TimeUnit.SECONDS);
                var owner = new com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1(1, 1, 1, 1, 1);
                var closed = await(f.roots.closeOwner(owner)).exactProof().orElseThrow();
                assertThat(closed.runs()).containsExactly(f.record(sealed).initialLink());
                var proof = await(f.session.fenceAndRecoverRunLedger(
                                run.snapshot().handle()))
                        .exactProof()
                        .orElseThrow();
                assertThat(proof.fenced()).isTrue();
                faults.release.run();
                assertThatThrownBy(() -> late.get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("run-root mutation was not established exactly");
                assertThat(await(f.roots.openRoot(f.runBinding(1).runId()))).isEmpty();
                assertThat(await(f.roots.closeOwner(owner)).exactProof()).contains(closed);
                assertThat(await(f.roots.readClosedOwner(1))).contains(closed);
                assertThat(await(f.roots.openOwner(
                                        new com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1(2, 2, 2, 2, 2),
                                        Optional.of(closed)))
                                .exactProof())
                        .isPresent();
                assertThat(await(f.roots.closeOwner(owner)).exactProof()).contains(closed);
            } finally {
                if (faults.release != null) {
                    faults.release.run();
                }
            }
        }
    }

    @Test
    void nativeSuccessorRaceKeepsOneChildAndTheWinningMetadataVersion() throws Exception {
        try (var f = new Fixture(1504, "orders", null)) {
            var faults = new Faults(f.nativeClient);
            var roots = f.faulted(faults);
            var run = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, roots, f.runBinding(0), 0));
            f.writeData(run);
            await(run.drain());
            var sealed = await(run.seal(f.footer(
                            run.snapshot().runBinding(), 2, run.snapshot().nextEntryId())))
                    .root();
            faults.holdKey = roots.nativeOwnerKey();
            var older = run.createSuccessor(f.runBinding(1)).toCompletableFuture();
            try {
                faults.held.get(30, TimeUnit.SECONDS);
                assertThat(await(roots.openRoot(f.runBinding(1).runId()))).isEmpty();
                var winner = await(run.createSuccessor(f.runBinding(2)));
                var nativeWinner = await(f.nativeClient.read(roots.nativeRootKey(sealed.runId())))
                        .orElseThrow();
                assertThat(older).isNotDone();
                faults.release.run();
                assertThatThrownBy(() -> older.get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("run-root mutation was not established exactly");
                assertThat(await(f.nativeClient.read(roots.nativeRootKey(sealed.runId()))))
                        .contains(nativeWinner);
                assertThat(await(roots.openRoot(f.runBinding(1).runId()))).isEmpty();
                assertThat(await(roots.openRoot(winner.snapshot().root().runId())))
                        .contains(winner.snapshot().root());
                for (var handle : f.handles.values()) {
                    assertThat(f.tickets(handle.ledgerIdentity())).isZero();
                }
                await(winner.drain());
                await(winner.seal(f.footer(
                        winner.snapshot().runBinding(), 2, winner.snapshot().nextEntryId())));
            } finally {
                if (faults.release != null) {
                    faults.release.run();
                }
            }
        }
    }

    @Test
    void nativePhysicalFencePreventsRootRecordPublication() throws Exception {
        try (var f = new Fixture(1505, "orders", null)) {
            var root = f.prepareSealedSource();
            var resource = f.record(root).resource();
            var before = await(f.route.read(resource.authorityKey())).orElseThrow();
            assertThat(await(f.route.compareAndSet(
                            Optional.of(before),
                            resource.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                                    SyntheticDeleteAuthorityFixturesV2.phases(resource)
                                            .get(1)))))
                    .isEqualTo(
                            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                    .APPLIED_EXACT);
            assertThat(await(f.roots.createRoot(root)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
            assertThat(await(f.nativeClient.read(f.roots.nativeRootKey(root.runId()))))
                    .isEmpty();
            assertThat(await(f.roots.readOwnerAdmission()).orElseThrow().runs()).isEmpty();
        }
    }

    @Test
    void boundNativeDeleteFinishesPermanentDoneBeforeRetiringTheSealedRunRoot() throws Exception {
        try (var f = new Fixture(1506, "deleted-root", null)) {
            var run = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, f.roots, f.runBinding(0), 0));
            f.writeData(run);
            await(run.drain());
            var sealed = await(run.seal(f.footer(
                            run.snapshot().runBinding(), 2, run.snapshot().nextEntryId())))
                    .root();
            var next = await(run.createSuccessor(f.runBinding(1)));
            await(next.drain());
            var child = await(next.seal(f.footer(
                            next.snapshot().runBinding(), 2, next.snapshot().nextEntryId())))
                    .root();
            await(f.source.fenceCreates());

            var handle = run.snapshot().handle();
            var resource = f.record(sealed).resource();
            var gc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
            var namespace = await(OxiaPhysicalMetadataNamespaceV2.connect(f.oxia, f.binding.metadataNamespace()));
            var route = await(namespace.openAuthorityRoute(
                    f.backend, gc.readOnlyFacts(new Oxia09ExactMetadataTransactionStoreV1(f.oxia))));
            var deletes = new M5TargetDeleteAuthorityCoordinatorV1(
                    route, gc, new KafkaBookKeeperDeleteIdentityReaderV2(f.source, handle));
            var open = await(route.read(resource.authorityKey())).orElseThrow();
            var facts = nativeDeleteFacts(f, resource);
            var qualified = await(deletes.qualifyEligibility(
                            open,
                            SyntheticDeleteAuthorityFixturesV2.replacement(
                                    resource,
                                    M5TargetDeleteAuthorityCodecV1.decodeAuthority(open.canonicalStoredBytes())
                                                    .authorityRevision()
                                            + 1,
                                    facts)))
                    .observed()
                    .orElseThrow();
            await(gc.claim(route, Optional.empty()));
            var observation = await(gc.observe(1, Optional.empty()));
            var fenced = await(deletes.prepareIdentityRead(
                            qualified, SyntheticDeleteAuthorityFixturesV2.digest("run-root-delete-read"), observation))
                    .observed()
                    .orElseThrow();
            var identity = await(new KafkaBookKeeperDeleteIdentityReaderV2(f.source, handle)
                    .capture(M5TargetDeleteAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes())));
            var intent = await(deletes.bindDeleteIntent(
                            fenced, identity, SyntheticDeleteAuthorityFixturesV2.digest("run-root-delete-dispatch")))
                    .observed()
                    .orElseThrow();
            var bound = await(gc.bindIntent(route, intent));
            var finalizer = new KafkaBookKeeperRunDeleteFinalizerV2(deletes, route.quota(), f.roots);
            var permit = new KafkaRunRetirementPermitV1(true, true, 0, true);
            assertThatThrownBy(() -> await(finalizer.finishAbsent(intent, sealed)))
                    .hasRootCauseMessage("native target absence was not established");
            assertThat(await(f.roots.openRoot(sealed.runId()))).contains(sealed);
            assertThatThrownBy(() -> await(run.retire(permit)))
                    .hasRootCauseMessage("run root lacks its exact durable retirement marker");

            var deleted = await(gc.dispatchBoundDelete(route, f.backend.nativeDeleteCellBudget(), intent, bound));
            assertThat(deleted.deleteResult().outcome())
                    .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
            assertThat(await(f.source.captureExactTarget(handle)).exactTarget()).isEmpty();
            assertThat(await(finalizer.finishAbsent(intent, sealed))).isEqualTo(sealed);
            assertThat(await(finalizer.finishAbsent(intent, sealed))).isEqualTo(sealed);
            assertThat(await(f.roots.openRoot(sealed.runId()))).isEmpty();
            assertThat(await(f.roots.openRoot(child.runId()))).contains(child);
            assertThat(await(f.roots.isDurablyRetired(sealed))).isTrue();
            assertThat(await(run.retire(permit)).state()).isEqualTo(KafkaBookKeeperRunStateV1.RETIRED);
            assertThat(await(route.quota().settle(resource))).isEqualTo(M5GcQuotaCoordinatorV2.Result.SETTLED);
        }
    }

    @Test
    void writeBeforeServerRestart() throws Exception {
        Files.createDirectories(checkpoint());
        for (int i = 0; i < TOPICS.size(); i++) {
            try (var f = new Fixture(1510 + i, TOPICS.get(i), null)) {
                var root = f.prepareSealedSource();
                var faults = new Faults(f.nativeClient);
                faults.loseAdmission = true;
                assertThat(await(f.faulted(faults).createRoot(root)).outcome())
                        .isEqualTo(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN);
                assertThat(f.tickets(root.ledgerIdentity())).isEqualTo(1);
                var rootValue = await(f.nativeClient.read(f.roots.nativeRootKey(root.runId())))
                        .orElseThrow();
                var genesis =
                        await(f.nativeClient.read(f.roots.nativeOwnerKey())).orElseThrow();
                var authority = await(f.route.read(f.record(root).resource().authorityKey()))
                        .orElseThrow();
                Files.write(
                        checkpoint().resolve(TOPICS.get(i)),
                        List.of(
                                f.source.spec().encode().toHex(),
                                root.runId().value().toHex(),
                                Long.toString(root.ledgerIdentity().ledgerId()),
                                identity(rootValue),
                                identity(genesis),
                                authority.canonicalStoredSha256().toHex() + ":"
                                        + authority.metadataVersion().value().toHex()));
            }
        }
        // Test-only native CAS: production still lacks a complete reference-free retirement producer.
        try (var f = new Fixture(1512, "retired-restart", null)) {
            var run = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, f.roots, f.runBinding(0), 0));
            f.writeData(run);
            await(run.drain());
            var sealed = await(run.seal(f.footer(
                            run.snapshot().runBinding(), 2, run.snapshot().nextEntryId())))
                    .root();
            var next = await(run.createSuccessor(f.runBinding(1)));
            await(next.drain());
            var child = await(next.seal(f.footer(
                            next.snapshot().runBinding(), 2, next.snapshot().nextEntryId())))
                    .root();
            var key = f.roots.nativeRootKey(sealed.runId());
            var before = await(f.nativeClient.read(key)).orElseThrow();
            var retired = KafkaRunRootRecordV2.decode(before.storedBytes()).retire();
            assertThat(retired.successor()).contains(f.stored(child).initialLink());
            var permit = new KafkaRunRetirementPermitV1(true, true, 0, true);
            assertThatThrownBy(() -> await(run.retire(permit)))
                    .hasRootCauseMessage("run root lacks its exact durable retirement marker");
            assertThat(run.snapshot().state()).isEqualTo(KafkaBookKeeperRunStateV1.SEALED);
            await(f.nativeClient.compareAndSet(key, retired.encode(), before.versionId()));
            var stored = await(f.nativeClient.read(key)).orElseThrow();
            assertThat(KafkaRunRootRecordV2.decode(stored.storedBytes())).isEqualTo(retired);
            assertThat(await(run.retire(permit)).state()).isEqualTo(KafkaBookKeeperRunStateV1.RETIRED);
            Files.write(
                    checkpoint().resolve("test-only-retired-root"),
                    List.of(
                            f.source.spec().encode().toHex(),
                            sealed.runId().value().toHex(),
                            child.runId().value().toHex(),
                            identity(stored),
                            identity(await(f.nativeClient.read(f.roots.nativeRootKey(child.runId())))
                                    .orElseThrow()),
                            Long.toString(sealed.ledgerIdentity().ledgerId())));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        for (int i = 0; i < TOPICS.size(); i++) {
            var lines = Files.readAllLines(checkpoint().resolve(TOPICS.get(i)));
            try (var f = new Fixture(1510 + i, TOPICS.get(i), lines)) {
                var runId = new StorageRunId(
                        Id128.fromBytes(java.util.HexFormat.of().parseHex(lines.get(1))));
                var rootValue =
                        await(f.nativeClient.read(f.roots.nativeRootKey(runId))).orElseThrow();
                var genesis =
                        await(f.nativeClient.read(f.roots.nativeOwnerKey())).orElseThrow();
                assertThat(identity(rootValue)).isEqualTo(lines.get(3));
                assertThat(identity(genesis)).isEqualTo(lines.get(4));
                var actual = await(f.roots.openRoot(runId)).orElseThrow();
                assertThat(actual.ledgerIdentity().ledgerId()).isEqualTo(Long.parseLong(lines.get(2)));
                var authority = await(f.route.read(f.record(actual).resource().authorityKey()))
                        .orElseThrow();
                assertThat(authority.canonicalStoredSha256().toHex() + ":"
                                + authority.metadataVersion().value().toHex())
                        .isEqualTo(lines.get(5));
                assertThat(f.tickets(actual.ledgerIdentity())).isEqualTo(1);
                assertThat(await(f.roots.createRoot(actual)).exactProof()).contains(actual);
                assertThat(f.tickets(actual.ledgerIdentity())).isZero();
                assertThat(await(f.nativeClient.read(f.roots.nativeRootKey(runId))))
                        .contains(rootValue);
                assertThat(await(f.nativeClient.read(f.roots.nativeOwnerKey()))).contains(genesis);
                var sealed = sealed(actual, 2);
                assertThat(await(f.roots.sealRoot(actual, sealed)).exactProof()).contains(sealed);
                assertThat(await(f.roots.openRoot(runId))).contains(sealed);
                var handle = new RunLedgerHandleV1(
                        actual.providerScopeId(),
                        actual.runId(),
                        actual.ledgerIdentity(),
                        f.verifier.capabilitySha256());
                f.verifyData(handle);
            }
        }
        var lines = Files.readAllLines(checkpoint().resolve("test-only-retired-root"));
        try (var f = new Fixture(1512, "retired-restart", lines)) {
            var parent =
                    new StorageRunId(Id128.fromBytes(java.util.HexFormat.of().parseHex(lines.get(1))));
            var child =
                    new StorageRunId(Id128.fromBytes(java.util.HexFormat.of().parseHex(lines.get(2))));
            var storedParent =
                    await(f.nativeClient.read(f.roots.nativeRootKey(parent))).orElseThrow();
            var storedChild =
                    await(f.nativeClient.read(f.roots.nativeRootKey(child))).orElseThrow();
            assertThat(identity(storedParent)).isEqualTo(lines.get(3));
            assertThat(identity(storedChild)).isEqualTo(lines.get(4));
            var retired = KafkaRunRootRecordV2.decode(storedParent.storedBytes());
            var selectedChild = KafkaRunRootRecordV2.decode(storedChild.storedBytes());
            assertThat(retired.retired()).isTrue();
            assertThat(retired.successor()).contains(selectedChild.initialLink());
            assertThat(await(f.roots.openRoot(parent))).isEmpty();
            assertThat(await(f.roots.readSelectedRoot(f.roots.nativeRootKey(parent))))
                    .isEmpty();
            assertThat(await(f.roots.openRoot(child))).contains(selectedChild.root());
            assertThat(retired.resource().ledgerId()).isEqualTo(Long.parseLong(lines.get(5)));
            assertThat(f.tickets(retired.root().ledgerIdentity())).isZero();
            f.verifyData(new RunLedgerHandleV1(
                    retired.root().providerScopeId(),
                    parent,
                    retired.root().ledgerIdentity(),
                    f.verifier.capabilitySha256()));
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.runroot.restartCheckpoint"));
    }

    private static String identity(AuthorityRecord record) {
        return Sha256Digest.hash(record.storedBytes()).toHex() + ":" + record.versionId();
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private static KafkaRunRootSnapshotV1 sealed(KafkaRunRootSnapshotV1 root, long end) {
        return new KafkaRunRootSnapshotV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId(),
                root.ledgerIdentity(),
                root.kafkaStartOffset(),
                OptionalLong.of(end),
                KafkaRunRootStateV1.SEALED,
                root.predecessorRunId());
    }

    private static BiFunction<String, CanonicalBytes, AuthorityFactV1> nativeDeleteFacts(
            Fixture fixture, PhysicalResourceIdV2 resource) {
        var metadata = new Oxia09ExactMetadataTransactionStoreV1(fixture.oxia);
        return (suffix, bytes) -> {
            String key = "/native-run-delete-facts/" + resource.sha256().toHex() + suffix;
            try {
                if (await(metadata.read(key)).isEmpty()) {
                    await(metadata.compareAndSet(Optional.empty(), key, bytes));
                }
                var actual = await(metadata.read(key)).orElseThrow();
                assertThat(actual.canonicalStoredBytes()).isEqualTo(bytes);
                return new AuthorityFactV1(key, actual.metadataVersion(), actual.canonicalStoredSha256());
            } catch (Exception failure) {
                throw new IllegalStateException("native delete fact was not stored exactly", failure);
            }
        };
    }

    static final class Fixture implements AutoCloseable {
        final long attempt;
        final Scope scope;
        final List<CanonicalBytes> bodies;
        final AsyncOxiaClient oxia;
        final OxiaConditionalClient nativeClient;
        final M5BookKeeperNamespaceAuthorityV2 backend;
        final PhysicalNamespaceAuthorityBindingV2 binding;
        final M5BookKeeperNativeCreateClientV2 source;
        final RealBookKeeperCellSessionV1 session;
        final List<RunLedgerConfigurationV1> configurations;
        final KafkaBookKeeperNativeRootVerifierV2 verifier;
        final OxiaQuotaTargetDeleteStoreV2 route;
        final BookKeeperCellSession admitting;
        final OxiaKafkaRunRootAuthorityV2 roots;
        final Map<StorageRunId, RunLedgerHandleV1> handles = new ConcurrentHashMap<>();
        volatile boolean loseCheckpoint;
        volatile boolean loseClose;
        volatile RunLedgerAppendRequestV1 lostCheckpoint;

        Fixture(long attempt, String topic, List<String> restore) throws Exception {
            this.attempt = attempt;
            var input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, attempt);
            var capability = input.layout().task().capability();
            bodies = input.plan().inputBatches().stream()
                    .map(KafkaCompactionRecordsV1.InputBatch::canonicalBody)
                    .toList();
            scope = new Scope(
                    new TopicBindingId(digest("native-root-binding/" + topic + attempt)),
                    new KafkaTopicIncarnationIdentity(
                            new KafkaTopicId(new Id128(attempt, 1)), new KafkaTopicName(topic)),
                    0,
                    new StorageEpochId(digest("native-root-storage/" + topic + attempt)),
                    capability.providerScopeId());
            var builder = OxiaClientBuilder.create(System.getProperty("nereus.m5.oxia.serviceAddress"));
            assertArtifact(builder.getClass(), "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5");
            assertArtifact(
                    org.apache.bookkeeper.client.BookKeeper.class,
                    capability.clientArtifactSha256().toHex());
            oxia = builder.namespace("default")
                    .requestTimeout(Duration.ofSeconds(10))
                    .asyncClient()
                    .get(30, TimeUnit.SECONDS);
            nativeClient = new AsyncOxiaConditionalClient(oxia);
            String uri = System.getProperty("nereus.bookkeeper.metadataServiceUri");
            backend = M5BookKeeperNamespaceAuthorityV2.connect(uri, capability);
            var namespace = restore == null
                    ? await(OxiaPhysicalMetadataNamespaceV2.provision(oxia))
                    : await(OxiaPhysicalMetadataNamespaceV2.connect(
                            oxia, await(backend.readBinding()).orElseThrow().metadataNamespace()));
            binding = await(namespace.bind(backend));
            if (restore == null) {
                await(backend.nativeDeleteQuota()
                        .initialize(backend.nativeDeleteQuota().capacityForResources(2)));
                await(backend.nativeDeleteCellBudget()
                        .initialize(
                                new com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteCellBudgetV2.Limits(2, 2)));
            } else {
                await(backend.nativeDeleteQuota().snapshot());
                await(backend.nativeDeleteCellBudget().snapshot());
            }
            var spec = restore == null
                    ? M5BookKeeperNativeCreateSpecV2.of(
                            M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri, capability),
                            digest("native-root-task/" + topic + attempt),
                            java.util.stream.IntStream.range(0, 3)
                                    .mapToObj(ordinal -> RunLedgerConfigurationV1.from(
                                            capability, new StorageRunId(new Id128(attempt, ordinal + 1))))
                                    .toList())
                    : M5BookKeeperNativeCreateSpecV2.decode(
                            CanonicalBytes.copyOf(java.util.HexFormat.of().parseHex(restore.get(0))));
            configurations = spec.configurations();
            source = M5BookKeeperNativeCreateClientV2.connect(uri, capability, spec, binding);
            session = source.newSession();
            verifier = new KafkaBookKeeperNativeRootVerifierV2(source);
            route = await(namespace.openAuthorityRoute(backend, new Oxia09ExactMetadataTransactionStoreV1(oxia)));
            var quota = await(route.initialize(500_000_000));
            if (quota.head().capacityBytes() < 500_000_000) {
                assertThat(await(route.quota().expand(500_000_000))).isTrue();
            }
            roots = await(namespace.openKafkaRunRoots(
                    backend, new Oxia09ExactMetadataTransactionStoreV1(oxia), scope, verifier));
            admitting = new AdmittingSession(this);
            if (restore == null) {
                assertThat(await(roots.openOwner(
                                        new com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1(1, 1, 1, 1, 1),
                                        Optional.empty()))
                                .exactProof())
                        .isPresent();
            }
        }

        OxiaKafkaRunRootAuthorityV2 faulted(OxiaConditionalClient client) {
            return new OxiaKafkaRunRootAuthorityV2(
                    client,
                    binding,
                    scope,
                    verifier,
                    new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(route)));
        }

        Nbke2RunBindingV1 runBinding(int ordinal) {
            return new Nbke2RunBindingV1(
                    scope.bindingId(),
                    scope.topic(),
                    scope.partition(),
                    scope.storageEpoch(),
                    1,
                    1,
                    scope.providerScope(),
                    configurations.get(ordinal).runId());
        }

        KafkaRunRootRecordV2 record(KafkaRunRootSnapshotV1 root) {
            return new KafkaRunRootRecordV2(
                    new PhysicalResourceIdV2.BookKeeperLedger(
                            binding.physicalNamespace(), root.ledgerIdentity().ledgerId()),
                    root,
                    root.state() == KafkaRunRootStateV1.SEALED,
                    Optional.empty());
        }

        KafkaRunRootRecordV2 stored(KafkaRunRootSnapshotV1 root) throws Exception {
            return KafkaRunRootRecordV2.decode(await(nativeClient.read(roots.nativeRootKey(root.runId())))
                    .orElseThrow()
                    .storedBytes());
        }

        CompletionStage<Void> admit(RunLedgerHandleV1 handle) {
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                    binding.physicalNamespace(), handle.ledgerIdentity().ledgerId());
            return reserve(resource, 32)
                    .thenCompose(reservation -> {
                        assertThat(reservation).isEqualTo(M5GcQuotaCoordinatorV2.Result.GRANTED);
                        return route.compareAndSet(
                                Optional.empty(),
                                resource.authorityKey(),
                                M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                                        SyntheticDeleteAuthorityFixturesV2.phases(resource)
                                                .get(0)));
                    })
                    .thenAccept(outcome -> {
                        assertThat(outcome)
                                .isEqualTo(
                                        com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1
                                                .MutationOutcome.APPLIED_EXACT);
                        handles.put(handle.runId(), handle);
                    });
        }

        CompletionStage<M5GcQuotaCoordinatorV2.Result> reserve(PhysicalResourceIdV2 resource, int attempts) {
            return route.quota()
                    .reserve(resource)
                    .thenCompose(result -> result == M5GcQuotaCoordinatorV2.Result.RETRY && attempts > 1
                            ? reserve(resource, attempts - 1)
                            : CompletableFuture.completedFuture(result));
        }

        int tickets(BookKeeperLedgerIdentity ledger) throws Exception {
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(binding.physicalNamespace(), ledger.ledgerId());
            return M5TargetDeleteAuthorityCodecV1.decodeAuthority(await(route.read(resource.authorityKey()))
                            .orElseThrow()
                            .canonicalStoredBytes())
                    .activeWriterTickets()
                    .size();
        }

        void writeData(KafkaBookKeeperRunLifecycleV1 run) throws Exception {
            for (int i = 0; i < bodies.size(); i++) {
                var reservation = run.reserveDataGroup(1);
                data(
                        run.snapshot().handle(),
                        run.snapshot().runBinding(),
                        reservation.firstEntryId(),
                        i,
                        bodies.get(i));
                run.completeDataGroup(reservation);
            }
        }

        void data(RunLedgerHandleV1 handle, Nbke2RunBindingV1 binding, long entry, long offset, CanonicalBytes body)
                throws Exception {
            var descriptor =
                    new Nbke2AppendGroupDescriptorV1(offset, offset + 1, entry, entry, Sha256Digest.hash(body));
            append(
                    handle,
                    entry,
                    Nbke2CodecV1.encode(
                            handle.ledgerIdentity().ledgerId(),
                            entry,
                            new Nbke2DataV1(
                                    binding,
                                    offset,
                                    0,
                                    0,
                                    1,
                                    new Id128(attempt, entry + 1),
                                    new Id128(attempt + 1, entry + 1),
                                    Optional.of(descriptor),
                                    body)));
        }

        Nbke2RunFooterV1 footer(Nbke2RunBindingV1 binding, long end, long entry) {
            return new Nbke2RunFooterV1(binding, end, entry + 1, -1, -1, 1, List.of());
        }

        KafkaRunRootSnapshotV1 prepareSealedSource() throws Exception {
            var binding = runBinding(0);
            var handle = await(admitting.createRunLedger(
                            source.spec().configurations().get(0)))
                    .exactProof()
                    .orElseThrow();
            append(
                    handle,
                    0,
                    Nbke2CodecV1.encode(
                            handle.ledgerIdentity().ledgerId(),
                            0,
                            new Nbke2RunHeaderV1(binding, 0, 1, verifier.capabilitySha256())));
            for (int i = 0; i < bodies.size(); i++) {
                data(handle, binding, i + 1, i, bodies.get(i));
            }
            long entry = bodies.size() + 1L;
            append(
                    handle,
                    entry,
                    Nbke2CodecV1.encode(handle.ledgerIdentity().ledgerId(), entry, footer(binding, 2, entry)));
            assertThat(await(session.closeRunLedger(handle)).exactProof()).isPresent();
            return new KafkaRunRootSnapshotV1(
                    scope.bindingId(),
                    scope.topic(),
                    scope.partition(),
                    scope.storageEpoch(),
                    1,
                    1,
                    scope.providerScope(),
                    handle.runId(),
                    handle.ledgerIdentity(),
                    0,
                    OptionalLong.empty(),
                    KafkaRunRootStateV1.ACTIVE,
                    Optional.empty());
        }

        void append(RunLedgerHandleV1 handle, long entry, byte[] bytes) throws Exception {
            var payload = ImmutableRetainedStoragePayload.copyOf(bytes);
            try {
                assertThat(await(session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, entry, payload)))
                                .exactProof())
                        .isPresent();
            } finally {
                assertThat(payload.release()).isTrue();
            }
        }

        void verifyData(RunLedgerHandleV1 handle) throws Exception {
            assertThat(await(session.openRunLedger(handle)).exactHandle()).contains(handle);
            for (int i = 0; i < bodies.size(); i++) {
                var entry = await(session.readExactEntry(handle, i + 1))
                        .exactEntry()
                        .orElseThrow();
                var frame = (Nbke2DataV1) Nbke2CodecV1.decode(
                        entry.payload().toByteArray(), handle.ledgerIdentity().ledgerId(), i + 1);
                assertThat(frame.rawAssignedRecordBatch()).isEqualTo(bodies.get(i));
            }
        }

        public void close() throws Exception {
            try {
                verifier.close();
            } finally {
                try {
                    await(session.closeAsync());
                } finally {
                    try {
                        source.close();
                    } finally {
                        try {
                            backend.close();
                        } finally {
                            oxia.close();
                        }
                    }
                }
            }
        }
    }

    private static final class AdmittingSession implements BookKeeperCellSession {
        private final Fixture f;

        AdmittingSession(Fixture fixture) {
            f = fixture;
        }

        public com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId() {
            return f.session.providerScopeId();
        }

        public com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
            return f.session.capabilitySnapshot();
        }

        public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createRunLedger(
                RunLedgerConfigurationV1 configuration) {
            if (!f.source.spec().configurations().contains(configuration)) {
                try {
                    f.source.admitCreateConfiguration(configuration);
                } catch (Exception failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            }
            return f.session
                    .createRunLedger(configuration)
                    .thenCompose(result -> result.exactProof().isPresent()
                            ? f.admit(result.exactProof().orElseThrow()).thenApply(ignored -> result)
                            : CompletableFuture.completedFuture(result));
        }

        public CompletionStage<com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1> openRunLedger(
                RunLedgerHandleV1 handle) {
            return f.session.openRunLedger(handle);
        }

        public CompletionStage<ProviderMutationResultV1<com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1>>
                appendExplicitEntry(RunLedgerAppendRequestV1 request) {
            if (f.loseCheckpoint
                    && Nbke2CodecV1.decode(
                                    canonicalBuffer(request.payload().readOnlyBuffer()),
                                    request.handle().ledgerIdentity().ledgerId(),
                                    request.expectedEntryId())
                            instanceof com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1) {
                f.loseCheckpoint = false;
                f.lostCheckpoint = request;
                return f.session.appendExplicitEntry(request).thenApply(result -> {
                    assertThat(result.exactProof()).isPresent();
                    return ProviderMutationResultV1.outcomeUnknown();
                });
            }
            return f.session.appendExplicitEntry(request);
        }

        public CompletionStage<com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1> readExactEntry(
                RunLedgerHandleV1 handle, long entry) {
            if (f.lostCheckpoint != null
                    && f.lostCheckpoint.handle().equals(handle)
                    && f.lostCheckpoint.expectedEntryId() == entry) {
                f.lostCheckpoint = null;
                return CompletableFuture.completedFuture(
                        com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1.withoutEntry(
                                com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1.PROVIDER_FAILURE));
            }
            return f.session.readExactEntry(handle, entry);
        }

        public CompletionStage<
                        ProviderMutationResultV1<com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1>>
                fenceAndRecoverRunLedger(RunLedgerHandleV1 handle) {
            return f.session.fenceAndRecoverRunLedger(handle);
        }

        public CompletionStage<ProviderMutationResultV1<com.nereusstream.storage.api.bookkeeper.RunLedgerCloseProofV1>>
                closeRunLedger(RunLedgerHandleV1 handle) {
            if (f.loseClose) {
                f.loseClose = false;
                return f.session.closeRunLedger(handle).thenApply(result -> {
                    assertThat(result.exactProof()).isPresent();
                    return ProviderMutationResultV1.outcomeUnknown();
                });
            }
            return f.session.closeRunLedger(handle);
        }

        public CompletionStage<Void> drain() {
            return f.session.drain();
        }

        public CompletionStage<Void> closeAsync() {
            return f.session.closeAsync();
        }
    }

    private static final class Faults implements OxiaConditionalClient {
        final OxiaConditionalClient nativeClient;
        final CompletableFuture<Void> held = new CompletableFuture<>();
        volatile String holdKey;
        volatile boolean loseAdmission;
        volatile boolean blocked;
        Runnable release;

        Faults(OxiaConditionalClient client) {
            nativeClient = client;
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            return blocked
                    ? CompletableFuture.failedFuture(new IllegalStateException("native root read delivery loss"))
                    : nativeClient.read(key);
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes bytes) {
            return nativeClient.createIfAbsent(key, bytes);
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes bytes, long version) {
            if (key.equals(holdKey)) {
                holdKey = null;
                var result = new CompletableFuture<Void>();
                var released = new java.util.concurrent.atomic.AtomicBoolean();
                release = () -> {
                    if (released.compareAndSet(false, true)) {
                        nativeClient.compareAndSet(key, bytes, version).whenComplete((ignored, failure) -> {
                            if (failure == null) {
                                result.complete(null);
                            } else {
                                result.completeExceptionally(failure);
                            }
                        });
                    }
                };
                held.complete(null);
                return result;
            }
            return nativeClient.compareAndSet(key, bytes, version).thenCompose(ignored -> {
                if (loseAdmission && key.endsWith("/owner-admission-v1")) {
                    loseAdmission = false;
                    blocked = true;
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("native admission applied response loss"));
                }
                return CompletableFuture.completedFuture(null);
            });
        }
    }

    private static Sha256Digest digest(String text) {
        return SyntheticDeleteAuthorityFixturesV2.digest(text);
    }

    private static void assertArtifact(Class<?> type, String expected) throws Exception {
        var jar =
                Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(jar)))
                        .toHex())
                .isEqualTo(expected);
    }
}
