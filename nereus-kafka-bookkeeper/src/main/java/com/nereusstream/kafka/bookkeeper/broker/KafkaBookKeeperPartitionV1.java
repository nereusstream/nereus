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

package com.nereusstream.kafka.bookkeeper.broker;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaAssignedRecordBatchGroupAdapterV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionTicketV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolAppendPlanV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperObjectReadScopeV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadViewV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2BatchLocatorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2IndexDirectoryEntryV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RangeIndexBlockV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendAdmissionRequestV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaBookKeeperOrderedPipelineV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOffsetAssignedAppendV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendResultV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationObserver;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadResultV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadRunV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadSnapshotV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperRunTableV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperTargetedReaderV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaPackedIndexDirectoryV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRecoveryOutcomeV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRunBirthRecoveryV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunStateV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootAuthority;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadProtocolV1;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.BindingReadRouteTableV1;
import com.nereusstream.storage.object.read.BindingReadRouteV1.FailureClass;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** One native Broker's recovered BK owner, composing the existing run, commit, pipeline and read paths. */
public final class KafkaBookKeeperPartitionV1 {
    /** The physical-close stage confirms only success; uncertain close signals separately and retains ownership. */
    public record HistoricalEntryRead(
            CompletionStage<RunLedgerReadResultV1> result,
            CompletionStage<Void> closed,
            CompletionStage<Void> unknown) {
        public HistoricalEntryRead {
            Objects.requireNonNull(result, "result");
            Objects.requireNonNull(closed, "closed");
            Objects.requireNonNull(unknown, "unknown");
        }
    }

    @FunctionalInterface
    public interface HistoricalReads {
        CompletionStage<HistoricalEntryRead> read(KafkaRunRootSnapshotV1 root, RunLedgerHandleV1 handle, long entryId);
    }

    /** Low-frequency admission of the exact successor sources around their durable M4 publication. */
    @FunctionalInterface
    public interface NativeSourcePublication {
        HistoricalReads publish(NativeReadSources sources, Runnable publication);
    }

    /** Roots come from admitted native recovery/seal or this owner's exact run lifecycle. */
    public record NativeReadSources(
            List<KafkaRunRootSnapshotV1> sealedRoots,
            KafkaRunRootSnapshotV1 activeRoot,
            List<KafkaBookKeeperReadRunV1> sealedReadRuns,
            Sha256Digest configurationSha256) {
        public NativeReadSources {
            sealedRoots = List.copyOf(sealedRoots);
            sealedReadRuns = List.copyOf(sealedReadRuns);
            if (activeRoot.state() != KafkaRunRootStateV1.ACTIVE
                    || sealedRoots.stream().anyMatch(root -> root.state() != KafkaRunRootStateV1.SEALED)
                    || configurationSha256.isZero()) {
                throw new IllegalArgumentException("native read sources lack exact sealed/active roots");
            }
        }

        /** Stable across appends to the same active ledger; changes when the native source membership changes. */
        public Sha256Digest selectedViewSha256() {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                try (var output =
                        new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
                    output.writeUTF("NEREUS-BK-NATIVE-READ-SOURCES-V2");
                    var scope = KafkaRunRootRecordV2.Scope.of(activeRoot);
                    output.write(scope.encode().toByteArray());
                    output.write(configurationSha256.bytes().toByteArray());
                    output.writeInt(sealedRoots.size());
                    KafkaRunRootSnapshotV1 previous = null;
                    int readRunIndex = 0;
                    for (KafkaRunRootSnapshotV1 root : sealedRoots) {
                        if (!KafkaRunRootRecordV2.Scope.of(root).equals(scope)) {
                            throw new IllegalStateException("sealed BK source belongs to another Binding or namespace");
                        }
                        if (previous != null
                                && (!root.predecessorRunId().equals(Optional.of(previous.runId()))
                                        || root.kafkaStartOffset()
                                                != previous.kafkaEndOffsetExclusive()
                                                        .orElseThrow())) {
                            throw new IllegalStateException("sealed BK source lineage is incomplete or out of order");
                        }
                        if (root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset()) {
                            if (readRunIndex >= sealedReadRuns.size()
                                    || !sealedReadRuns
                                            .get(readRunIndex)
                                            .runBinding()
                                            .runId()
                                            .equals(root.runId())
                                    || sealedReadRuns.get(readRunIndex).startOffset() != root.kafkaStartOffset()
                                    || sealedReadRuns.get(readRunIndex).endOffsetExclusive()
                                            != root.kafkaEndOffsetExclusive().orElseThrow()) {
                                throw new IllegalStateException("sealed BK source lacks its complete read index");
                            }
                            readRunIndex++;
                        }
                        writeRoot(output, root);
                        previous = root;
                    }
                    if (readRunIndex != sealedReadRuns.size()
                            || previous != null
                                    && (!activeRoot.predecessorRunId().equals(Optional.of(previous.runId()))
                                            || activeRoot.kafkaStartOffset()
                                                    != previous.kafkaEndOffsetExclusive()
                                                            .orElseThrow())) {
                        throw new IllegalStateException("native BK read source lineage or index membership differs");
                    }
                    writeRoot(output, activeRoot);
                    output.writeInt(sealedReadRuns.size());
                    for (KafkaBookKeeperReadRunV1 source : sealedReadRuns) {
                        KafkaRunRootSnapshotV1 root = sealedRoots.stream()
                                .filter(candidate -> candidate
                                        .runId()
                                        .equals(source.runBinding().runId()))
                                .findFirst()
                                .orElseThrow(() ->
                                        new IllegalStateException("BK read index has no admitted sealed native root"));
                        if (!source.runBinding().bindingId().equals(root.bindingId())
                                || !source.runBinding().topicIncarnation().equals(root.topicIncarnation())
                                || source.runBinding().partitionId() != root.partitionId()
                                || !source.runBinding().storageEpochId().equals(root.storageEpochId())
                                || source.runBinding().creatorOwnerEpoch() != root.creatorOwnerEpoch()
                                || source.runBinding().kafkaLeaderEpoch() != root.kafkaLeaderEpoch()
                                || !source.runBinding().providerScopeId().equals(root.providerScopeId())
                                || !source.handle().ledgerIdentity().equals(root.ledgerIdentity())
                                || !source.handle().configurationDigest().equals(configurationSha256)
                                || source.startOffset() < root.kafkaStartOffset()
                                || source.endOffsetExclusive()
                                        > root.kafkaEndOffsetExclusive().orElseThrow()) {
                            throw new IllegalStateException(
                                    "BK read index differs from its admitted sealed native root");
                        }
                        output.write(source.runBinding().runId().value().bytes().toByteArray());
                        output.writeLong(source.startOffset());
                        output.writeLong(source.endOffsetExclusive());
                        if (source.activeIndex().isPresent()) {
                            var index = source.activeIndex().orElseThrow();
                            output.writeByte(1);
                            output.writeInt(index.size());
                            for (int row = 0; row < index.size(); row++) {
                                var locator = index.at(row);
                                output.writeLong(locator.startOffset());
                                output.writeLong(locator.endOffsetExclusive());
                                output.writeLong(locator.entryId());
                                output.writeLong(locator.appendGroupOrdinal());
                                output.writeLong(locator.rawPayloadBytes());
                                output.writeLong(locator.physicalChecksumGeneration());
                            }
                        } else {
                            var directory = source.sealedDirectory().orElseThrow();
                            output.writeByte(2);
                            output.writeInt(directory.size());
                            var pointer = directory.floorOrSuccessor(source.startOffset());
                            for (int row = 0; row < directory.size(); row++) {
                                var entry = pointer.orElseThrow(
                                        () -> new IllegalStateException("sealed BK directory has a missing row"));
                                output.writeLong(entry.indexBlockEntryId());
                                output.writeLong(entry.startOffset());
                                output.writeLong(entry.coveredThroughOffset());
                                pointer = directory.successor(entry.ordinal());
                            }
                        }
                    }
                }
                return Sha256Digest.copyOf(digest.digest());
            } catch (IOException | NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("cannot hash verified native BK read sources", impossible);
            }
        }

        private static void writeRoot(DataOutputStream output, KafkaRunRootSnapshotV1 root) throws IOException {
            output.write(root.runId().value().bytes().toByteArray());
            output.writeLong(root.ledgerIdentity().ledgerId());
            output.writeLong(root.creatorOwnerEpoch());
            output.writeInt(root.kafkaLeaderEpoch());
            output.writeLong(root.kafkaStartOffset());
            output.writeLong(root.kafkaEndOffsetExclusive().orElse(-1));
            output.writeByte(root.state() == KafkaRunRootStateV1.SEALED ? 2 : 1);
            output.writeBoolean(root.predecessorRunId().isPresent());
            if (root.predecessorRunId().isPresent()) {
                output.write(
                        root.predecessorRunId().orElseThrow().value().bytes().toByteArray());
            }
        }
    }

    private final KafkaBookKeeperOwnerAuthorityV1 ownerAuthority;
    private final KafkaOwnerIdentityV1 owner;
    private KafkaBookKeeperRunLifecycleV1 run;
    private final KafkaCoherentCommitCoordinatorV1 commit;
    private KafkaBookKeeperOrderedPipelineV1 pipeline;
    private KafkaBookKeeperDataAdmissionV1 dataAdmission;
    private final BookKeeperCellSession session;
    private final KafkaAppendCapacityControllerV1 partitionCapacity;
    private final KafkaAppendCapacityControllerV1 globalCapacity;
    private final int maximumBatchBytes;
    private int admissions;
    private boolean rolling;
    private Sha256Digest nativePublicationTask;
    private NativeObjectPublication nativeObjectPublication;
    private KafkaBookKeeperNativeFallbackRetirementV2 nativeObjectRetirement;
    private ObjectBookKeeperSourceTransfer pendingObjectSourceTransfer;
    private KafkaBookKeeperNativeObjectReadSourcesV1 recoveredObjectReadSources;
    private KafkaBookKeeperReadViewV2 nativePublicationPredecessor;
    private KafkaBookKeeperReadViewV2 nativePreferredCache;
    private final KafkaOwnerAdmissionV1 selectedRecoveryOwner;
    private final Sha256Digest selectedRecoveryDescriptor;
    private KafkaBookKeeperNativeFallbackRetirementV2 nativeFallbackRetirement;
    private long runEntries;
    private long runBytes;
    private long checkpointBaseBytes;
    private final KafkaBookKeeperTargetedReaderV1 reader;
    private HistoricalReads historicalReads;
    private NativeSourcePublication nativeSourcePublication;
    private final List<KafkaBookKeeperReadRunV1> historyRuns;
    private final List<KafkaRunRootSnapshotV1> sealedHistoryRoots;
    // M4 payload transfer removes read-table members, never this actual Owner's run directory.
    private final List<StorageRunId> nativeOwnerRunIds;
    private Sha256Digest nativeSourceViewSha256;
    private final KafkaBookKeeperRecoveryEnvelopeV1 envelope;
    private final Executor controlExecutor;
    private final KafkaNativeProtocolBatchAdapterV1 protocolAdapter;
    private long admittedEntries;
    private long admittedBytes;
    private boolean writable = true;
    private int activeReads;
    private final CompletableFuture<Void> readsDrained = new CompletableFuture<>();
    private CompletableFuture<Void> resignation;
    private KafkaBookKeeperM4BrokerReadOwnerV2 m4ReadOwner;

    private KafkaBookKeeperPartitionV1(
            BookKeeperCellSession session,
            KafkaBookKeeperOwnerAuthorityV1 ownerAuthority,
            KafkaOwnerIdentityV1 owner,
            KafkaBookKeeperRunLifecycleV1 run,
            KafkaCoherentCommitCoordinatorV1 commit,
            Optional<KafkaBookKeeperClosedHistoryRecoveryV1.Result> history,
            List<KafkaRunRootSnapshotV1> sealedHistoryRoots,
            KafkaAppendCapacityControllerV1 partitionCapacity,
            KafkaAppendCapacityControllerV1 globalCapacity,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            int maximumBatchBytes,
            Executor controlExecutor,
            Optional<KafkaBookKeeperReadViewV2> selectedPrefix,
            Optional<KafkaOwnerAdmissionV1> closedOwner,
            KafkaNativeProtocolBatchAdapterV1 protocolAdapter) {
        this.ownerAuthority = ownerAuthority;
        this.owner = owner;
        this.run = run;
        this.commit = commit;
        this.envelope = envelope;
        this.controlExecutor = Objects.requireNonNull(controlExecutor, "controlExecutor");
        this.protocolAdapter = protocolAdapter;
        this.session = session;
        this.partitionCapacity = partitionCapacity;
        this.globalCapacity = globalCapacity;
        this.maximumBatchBytes = maximumBatchBytes;
        nativePreferredCache = selectedPrefix.orElse(null);
        selectedRecoveryOwner = selectedPrefix.isPresent() ? closedOwner.orElseThrow() : null;
        selectedRecoveryDescriptor =
                selectedPrefix.map(view -> view.descriptor().descriptorSha256()).orElse(null);
        pipeline = new KafkaBookKeeperOrderedPipelineV1(session, run, partitionCapacity, globalCapacity, commit);
        dataAdmission = KafkaBookKeeperDataAdmissionV1.admitProfile(
                run.snapshot().runBinding(), session.capabilitySnapshot(), maximumBatchBytes);
        reader = new KafkaBookKeeperTargetedReaderV1(session, 32);
        historyRuns =
                new ArrayList<>(history.map(value -> value.readTable(0).runs()).orElse(List.of()));
        this.sealedHistoryRoots = new ArrayList<>(sealedHistoryRoots);
        nativeOwnerRunIds = new ArrayList<>(sealedHistoryRoots.stream()
                .filter(root -> root.creatorOwnerEpoch() == owner.ownerEpoch())
                .map(KafkaRunRootSnapshotV1::runId)
                .toList());
        nativeOwnerRunIds.add(run.snapshot().runBinding().runId());
        admittedEntries =
                Math.addExact(history.map(value -> value.progress().entries()).orElse(0L), 1);
        admittedBytes = Math.addExact(
                history.map(value -> value.progress().encodedBytes()).orElse(0L),
                Nbke2CodecV1.encode(
                                run.snapshot().handle().ledgerIdentity().ledgerId(),
                                0,
                                new Nbke2RunHeaderV1(
                                        run.snapshot().runBinding(),
                                        run.snapshot().root().kafkaStartOffset(),
                                        1,
                                        session.capabilitySnapshot().configurationDigest()))
                        .length);
        runEntries = 1;
        runBytes = admittedBytes
                - history.map(value -> value.progress().encodedBytes()).orElse(0L);
        var initial = commit.capture();
        long end = initial.root().frontiers().durableEndOffset();
        checkpointBaseBytes = Nbke2CodecV1.encode(
                        run.snapshot().handle().ledgerIdentity().ledgerId(),
                        1,
                        new KafkaProtocolCheckpointStateV1(
                                        new KafkaRecoveryCheckpointVectorV1(
                                                run.snapshot().runBinding(), end, end, end, end),
                                        initial.committedProducerState(),
                                        initial.transactionState(),
                                        initial.leaderEpochIndex(),
                                        historyRuns.stream()
                                                .filter(KafkaBookKeeperReadRunV1::active)
                                                .map(
                                                        com.nereusstream.kafka.bookkeeper.checkpoint
                                                                        .KafkaCheckpointReadIndexV1::from)
                                                .toList())
                                .toNbke2())
                .length;
        if (admittedEntries > envelope.maximumEntries() || admittedBytes >= envelope.maximumEncodedBytes()) {
            throw new IllegalStateException("closed history leaves no recovery capacity for this owner");
        }
        nativeSourceViewSha256 = nativeReadSourcesUnchecked().selectedViewSha256();
    }

    /** The caller projects an exact native assignment and rechecks it at every activation boundary. */
    public static CompletionStage<KafkaBookKeeperPartitionV1> open(
            BookKeeperCellSession session,
            KafkaRunRootAuthority roots,
            KafkaBookKeeperOwnerAuthorityV1 owners,
            KafkaOwnerIdentityV1 owner,
            Nbke2RunBindingV1 binding,
            KafkaPartitionFenceV1 fence,
            Optional<KafkaOwnerAdmissionV1> closed,
            KafkaAppendCapacityControllerV1 partitionCapacity,
            KafkaAppendCapacityControllerV1 globalCapacity,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            int maximumBatchBytes,
            KafkaPartitionPublicationObserver observer,
            BooleanSupplier isCurrent,
            Executor controlExecutor,
            KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence) {
        return open(
                session,
                roots,
                owners,
                owner,
                binding,
                fence,
                closed,
                partitionCapacity,
                globalCapacity,
                envelope,
                maximumBatchBytes,
                observer,
                isCurrent,
                controlExecutor,
                unselectedFence,
                KafkaBookKeeperClosedHistoryRecoveryV1.borrowedReads(session));
    }

    public static CompletionStage<KafkaBookKeeperPartitionV1> open(
            BookKeeperCellSession session,
            KafkaRunRootAuthority roots,
            KafkaBookKeeperOwnerAuthorityV1 owners,
            KafkaOwnerIdentityV1 owner,
            Nbke2RunBindingV1 binding,
            KafkaPartitionFenceV1 fence,
            Optional<KafkaOwnerAdmissionV1> closed,
            KafkaAppendCapacityControllerV1 partitionCapacity,
            KafkaAppendCapacityControllerV1 globalCapacity,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            int maximumBatchBytes,
            KafkaPartitionPublicationObserver observer,
            BooleanSupplier isCurrent,
            Executor controlExecutor,
            KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence,
            KafkaBookKeeperClosedHistoryRecoveryV1.ReadOwner historyReads) {
        return open(
                session,
                roots,
                owners,
                owner,
                binding,
                fence,
                closed,
                partitionCapacity,
                globalCapacity,
                envelope,
                maximumBatchBytes,
                observer,
                isCurrent,
                controlExecutor,
                unselectedFence,
                historyReads,
                Optional.empty());
    }

    /** The enclosing native owner retains independently admitted output/cache capacity and exact selection. */
    public static CompletionStage<KafkaBookKeeperPartitionV1> open(
            BookKeeperCellSession session,
            KafkaRunRootAuthority roots,
            KafkaBookKeeperOwnerAuthorityV1 owners,
            KafkaOwnerIdentityV1 owner,
            Nbke2RunBindingV1 binding,
            KafkaPartitionFenceV1 fence,
            Optional<KafkaOwnerAdmissionV1> closed,
            KafkaAppendCapacityControllerV1 partitionCapacity,
            KafkaAppendCapacityControllerV1 globalCapacity,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            int maximumBatchBytes,
            KafkaPartitionPublicationObserver observer,
            BooleanSupplier isCurrent,
            Executor controlExecutor,
            KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence,
            KafkaBookKeeperClosedHistoryRecoveryV1.ReadOwner historyReads,
            Optional<KafkaBookKeeperReadViewV2> selectedPrefix) {
        return openSelected(
                session,
                roots,
                owners,
                owner,
                binding,
                fence,
                closed,
                partitionCapacity,
                globalCapacity,
                envelope,
                maximumBatchBytes,
                observer,
                isCurrent,
                controlExecutor,
                unselectedFence,
                historyReads,
                selectedPrefix.map(KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix::raw));
    }

    /** The native caller qualifies immutable generation provenance before recovering the closed raw lineage. */
    public static CompletionStage<KafkaBookKeeperPartitionV1> openSelected(
            BookKeeperCellSession session,
            KafkaRunRootAuthority roots,
            KafkaBookKeeperOwnerAuthorityV1 owners,
            KafkaOwnerIdentityV1 owner,
            Nbke2RunBindingV1 binding,
            KafkaPartitionFenceV1 fence,
            Optional<KafkaOwnerAdmissionV1> closed,
            KafkaAppendCapacityControllerV1 partitionCapacity,
            KafkaAppendCapacityControllerV1 globalCapacity,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            int maximumBatchBytes,
            KafkaPartitionPublicationObserver observer,
            BooleanSupplier isCurrent,
            Executor controlExecutor,
            KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence,
            KafkaBookKeeperClosedHistoryRecoveryV1.ReadOwner historyReads,
            Optional<KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix> selectedPrefix) {
        return openSelected(
                session,
                roots,
                owners,
                owner,
                binding,
                fence,
                closed,
                partitionCapacity,
                globalCapacity,
                envelope,
                maximumBatchBytes,
                observer,
                isCurrent,
                controlExecutor,
                unselectedFence,
                historyReads,
                selectedPrefix,
                false);
    }

    /** Native consumer-offset partitions recover coordinator transactions without client sequences. */
    public static CompletionStage<KafkaBookKeeperPartitionV1> openSelected(
            BookKeeperCellSession session,
            KafkaRunRootAuthority roots,
            KafkaBookKeeperOwnerAuthorityV1 owners,
            KafkaOwnerIdentityV1 owner,
            Nbke2RunBindingV1 binding,
            KafkaPartitionFenceV1 fence,
            Optional<KafkaOwnerAdmissionV1> closed,
            KafkaAppendCapacityControllerV1 partitionCapacity,
            KafkaAppendCapacityControllerV1 globalCapacity,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            int maximumBatchBytes,
            KafkaPartitionPublicationObserver observer,
            BooleanSupplier isCurrent,
            Executor controlExecutor,
            KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence,
            KafkaBookKeeperClosedHistoryRecoveryV1.ReadOwner historyReads,
            Optional<KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix> selectedPrefix,
            boolean consumerOffsets) {
        return openSelected(
                session,
                roots,
                owners,
                owner,
                binding,
                fence,
                closed,
                partitionCapacity,
                globalCapacity,
                envelope,
                maximumBatchBytes,
                observer,
                isCurrent,
                controlExecutor,
                unselectedFence,
                historyReads,
                selectedPrefix,
                consumerOffsets,
                Optional.empty());
    }

    /** Native callers can recover a released Object prefix using its independently retained BK checkpoint. */
    public static CompletionStage<KafkaBookKeeperPartitionV1> openSelected(
            BookKeeperCellSession session,
            KafkaRunRootAuthority roots,
            KafkaBookKeeperOwnerAuthorityV1 owners,
            KafkaOwnerIdentityV1 owner,
            Nbke2RunBindingV1 binding,
            KafkaPartitionFenceV1 fence,
            Optional<KafkaOwnerAdmissionV1> closed,
            KafkaAppendCapacityControllerV1 partitionCapacity,
            KafkaAppendCapacityControllerV1 globalCapacity,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            int maximumBatchBytes,
            KafkaPartitionPublicationObserver observer,
            BooleanSupplier isCurrent,
            Executor controlExecutor,
            KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence,
            KafkaBookKeeperClosedHistoryRecoveryV1.ReadOwner historyReads,
            Optional<KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix> selectedPrefix,
            boolean consumerOffsets,
            Optional<KafkaBookKeeperClosedHistoryRecoveryV1.ObjectPrefix> objectPrefix) {
        Objects.requireNonNull(isCurrent, "isCurrent");
        Objects.requireNonNull(controlExecutor, "controlExecutor");
        Objects.requireNonNull(unselectedFence, "unselectedFence");
        Objects.requireNonNull(historyReads, "historyReads");
        Objects.requireNonNull(selectedPrefix, "selectedPrefix");
        Objects.requireNonNull(objectPrefix, "objectPrefix");
        if (selectedPrefix.isPresent() && objectPrefix.isPresent()) {
            throw new IllegalArgumentException("native recovery cannot select two prefix sources");
        }
        if ((selectedPrefix.isPresent() || objectPrefix.isPresent()) && closed.isEmpty()) {
            throw new IllegalArgumentException("selected native recovery lacks a closed predecessor owner");
        }
        if (!fence.bindingId().equals(binding.bindingId())
                || !fence.topicIncarnation().equals(binding.topicIncarnation())
                || fence.partitionId() != binding.partitionId()
                || !fence.storageEpochId().equals(binding.storageEpochId())
                || !session.providerScopeId().equals(binding.providerScopeId())
                || owner.ownerEpoch() != binding.creatorOwnerEpoch()
                || owner.kafkaLeaderEpoch() != binding.kafkaLeaderEpoch()
                || fence.ownerEpoch() != owner.ownerEpoch()
                || fence.kafkaLeaderEpoch() != owner.kafkaLeaderEpoch()) {
            throw new IllegalArgumentException("native owner, run and publication identities differ");
        }
        requireCurrent(isCurrent);
        var protocolAdapter = consumerOffsets
                ? KafkaNativeProtocolBatchAdapterV1.consumerOffsets()
                : new KafkaNativeProtocolBatchAdapterV1();
        var recoveryReader = new KafkaBookKeeperClosedHistoryRecoveryV1(
                session, owners, protocolAdapter, System::nanoTime, unselectedFence, historyReads);
        CompletionStage<Optional<KafkaBookKeeperClosedHistoryRecoveryV1.Result>> recovery = closed.<CompletionStage<
                                Optional<KafkaBookKeeperClosedHistoryRecoveryV1.Result>>>
                        map(previous -> {
                    if (objectPrefix.isPresent()) {
                        var object = objectPrefix.orElseThrow();
                        return recoveryReader
                                .recoverObjectPrefix(
                                        previous,
                                        fence,
                                        0,
                                        envelope,
                                        object.selector(),
                                        object.prefix(),
                                        object.scope())
                                .thenApply(Optional::of);
                    }
                    return (selectedPrefix.isPresent()
                                    ? recoveryReader.recoverSelected(
                                            previous, fence, 0, envelope, selectedPrefix.orElseThrow())
                                    : recoveryReader.recover(previous, fence, 0, envelope))
                            .thenApply(Optional::of);
                })
                .orElseGet(() -> CompletableFuture.completedFuture(Optional.empty()));
        return recovery.thenCompose(
                history -> sealRecoveredHistory(owners, history, isCurrent).thenCompose(sealedRoots -> {
                    requireCurrent(isCurrent);
                    Optional<KafkaRunRootSnapshotV1> predecessor = sealedRoots.isEmpty()
                            ? history.flatMap(KafkaBookKeeperClosedHistoryRecoveryV1.Result::selectedPrefixAnchor)
                            : Optional.of(sealedRoots.get(sealedRoots.size() - 1));
                    long oldEntries =
                            history.map(value -> value.progress().entries()).orElse(0L);
                    long oldBytes = history.map(value -> value.progress().encodedBytes())
                            .orElse(0L);
                    long start = history.map(KafkaBookKeeperClosedHistoryRecoveryV1.Result::endOffset)
                            .orElse(0L);
                    int headerBytes = Nbke2CodecV1.encode(
                                    0,
                                    0,
                                    new Nbke2RunHeaderV1(
                                            binding,
                                            start,
                                            1,
                                            session.capabilitySnapshot().configurationDigest()))
                            .length;
                    if (oldEntries > envelope.maximumEntries() - 1
                            || headerBytes >= envelope.maximumEncodedBytes() - oldBytes) {
                        throw new IllegalStateException("closed history leaves no capacity for a new native run");
                    }
                    return owners.openOwner(owner, closed).thenCompose(admitted -> {
                        exact(admitted);
                        requireCurrent(isCurrent);
                        CompletionStage<KafkaBookKeeperRunLifecycleV1> opening = predecessor.isPresent()
                                ? KafkaBookKeeperRunLifecycleV1.createAfterRecovery(
                                        session, roots, predecessor.orElseThrow(), binding)
                                : KafkaBookKeeperRunLifecycleV1.createActive(session, roots, binding, 0);
                        return opening.thenCompose(
                                run -> owners.readOwnerAdmission().thenApply(current -> {
                                    requireCurrent(isCurrent);
                                    if (current.isEmpty()
                                            || current.orElseThrow().closed()
                                            || !current.orElseThrow().owner().equals(owner)) {
                                        throw new IllegalStateException(
                                                "native BK owner was superseded before activation");
                                    }
                                    var recoveredState = history.flatMap(
                                            KafkaBookKeeperClosedHistoryRecoveryV1.Result::protocolState);
                                    var commit = recoveredState.isPresent()
                                            ? KafkaCoherentCommitCoordinatorV1.bootstrapRecovered(
                                                    fence,
                                                    0,
                                                    history.orElseThrow().endOffset(),
                                                    recoveredState.orElseThrow(),
                                                    run.snapshot().handle(),
                                                    observer)
                                            : KafkaCoherentCommitCoordinatorV1.bootstrap(
                                                    fence, 0, run.snapshot().handle(), observer);
                                    return new KafkaBookKeeperPartitionV1(
                                            session,
                                            owners,
                                            owner,
                                            run,
                                            commit,
                                            history,
                                            sealedRoots,
                                            partitionCapacity,
                                            globalCapacity,
                                            envelope,
                                            maximumBatchBytes,
                                            controlExecutor,
                                            selectedPrefix.map(
                                                    KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix::view),
                                            closed,
                                            protocolAdapter);
                                }));
                    });
                }));
    }

    private static CompletionStage<List<KafkaRunRootSnapshotV1>> sealRecoveredHistory(
            KafkaBookKeeperOwnerAuthorityV1 owners,
            Optional<KafkaBookKeeperClosedHistoryRecoveryV1.Result> history,
            BooleanSupplier isCurrent) {
        CompletionStage<List<KafkaRunRootSnapshotV1>> sealedRoots = CompletableFuture.completedFuture(
                history.map(KafkaBookKeeperClosedHistoryRecoveryV1.Result::admittedRoots)
                        .orElse(List.of()));
        if (history.isPresent()) {
            for (var recovered : history.orElseThrow().runs()) {
                sealedRoots = sealedRoots.thenCompose(prefix -> {
                    requireCurrent(isCurrent);
                    CompletionStage<KafkaRunRootSnapshotV1> sealed;
                    if (recovered.root().root().state() == KafkaRunRootStateV1.SEALED) {
                        sealed = CompletableFuture.completedFuture(
                                recovered.root().root());
                    } else {
                        var residue = recovered.recovery().outcome()
                                        == KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_WITH_INERT_RESIDUE
                                ? recovered.recovery().conflictEntryId()
                                : OptionalLong.empty();
                        sealed = owners.sealRecoveredRun(
                                        recovered.root().root(),
                                        recovered.closedOwner(),
                                        recovered.fenceProof(),
                                        recovered.recovery().newLeaderLeo().orElseThrow(),
                                        residue)
                                .thenApply(KafkaBookKeeperPartitionV1::exact);
                    }
                    return sealed.thenApply(root -> {
                        var all = new ArrayList<>(prefix);
                        int index = -1;
                        for (int candidate = 0; candidate < all.size(); candidate++) {
                            if (all.get(candidate).runId().equals(root.runId())) {
                                index = candidate;
                                break;
                            }
                        }
                        if (index < 0) {
                            throw new IllegalStateException("recovered BK root was not in admitted history");
                        }
                        all.set(index, root);
                        return List.copyOf(all);
                    });
                });
            }
        }
        return sealedRoots;
    }

    /** Both the single run and the cumulatively uncovered tail remain bounded before offset assignment. */
    public AdmittedAppend admit(List<Integer> rawBatchBytes) {
        long entries = rawBatchBytes.size();
        long bytes = 0;
        synchronized (this) {
            requireWritable();
            if (rolling) {
                throw new IllegalStateException("BK run rollover is in progress");
            }
            if (nativePublicationTask != null) {
                throw new IllegalStateException("native M5 source publication is in progress");
            }
            for (int index = 0; index < rawBatchBytes.size(); index++) {
                bytes = Math.addExact(
                        bytes,
                        dataAdmission
                                .admitBeforeOffsetAllocation(rawBatchBytes.get(index), index, rawBatchBytes.size())
                                .encodedDataFrameBytes());
            }
            if (entries > dataEntryLimit() - 1 || bytes > envelope.maximumEncodedBytes() / 2) {
                throw new IllegalArgumentException("append group cannot fit the bounded BK run DATA budget");
            }
            if (entries > dataEntryLimit() - runEntries
                    || bytes > envelope.maximumEncodedBytes() / 2 - runBytes
                    || entries > envelope.maximumEntries() - 3 - admittedEntries
                    || !fitsControlReserve(entries, bytes)) {
                if (nativeObjectPublication != null
                        || nativeObjectRetirement != null && m4ReadOwner == nativeObjectRetirement.predecessorOwner()) {
                    throw new IllegalStateException(
                            "native Object selector transition has not settled before BK rollover");
                }
                if (admissions != 0
                        || !commit.capture().speculativeQueue().commits().isEmpty()) {
                    throw new IllegalStateException("BK run must finish its admitted groups before rollover");
                }
                rolling = true;
            }
        }
        if (rolling) {
            try {
                rollover().toCompletableFuture().get(envelope.maximumElapsedNanos(), TimeUnit.NANOSECONDS);
            } catch (Exception failure) {
                fence();
                throw new IllegalStateException("BK checkpoint/run rollover was not established exactly", failure);
            } finally {
                synchronized (this) {
                    rolling = false;
                }
            }
        }
        synchronized (this) {
            requireWritable();
            var tickets = new ArrayList<KafkaBookKeeperDataAdmissionTicketV1>();
            if (nativePublicationTask != null) {
                throw new IllegalStateException("native M5 source publication is in progress");
            }
            bytes = 0;
            for (int index = 0; index < rawBatchBytes.size(); index++) {
                var ticket = dataAdmission.admitBeforeOffsetAllocation(
                        rawBatchBytes.get(index), index, rawBatchBytes.size());
                tickets.add(ticket);
                bytes = Math.addExact(bytes, ticket.encodedDataFrameBytes());
            }
            var request = new KafkaAppendAdmissionRequestV1(tickets.size(), bytes);
            if (request.memberCount() > envelope.maximumEntries() - 3 - admittedEntries
                    || bytes > envelope.maximumEncodedBytes() - admittedBytes
                    || request.memberCount() > dataEntryLimit() - runEntries
                    || bytes > envelope.maximumEncodedBytes() / 2 - runBytes
                    || !fitsControlReserve(request.memberCount(), bytes)) {
                throw new IllegalStateException("BK uncovered recovery debt is full");
            }
            var lease = pipeline.reserveAdmission(request)
                    .orElseThrow(() -> new IllegalStateException("BK append capacity is full"));
            admittedEntries = Math.addExact(admittedEntries, request.memberCount());
            admittedBytes = Math.addExact(admittedBytes, bytes);
            runEntries = Math.addExact(runEntries, request.memberCount());
            runBytes = Math.addExact(runBytes, bytes);
            admissions++;
            return new AdmittedAppend(request, tickets, lease);
        }
    }

    private boolean fitsControlReserve(long entries, long bytes) {
        // Protocol deltas and packed locator rows are smaller than their admitted encoded DATA. This conservative
        // bound grows with outstanding DATA, avoiding a full-state encode on ordinary append admission.
        long protocolBound = Math.addExact(checkpointBaseBytes, Math.addExact(runBytes, bytes));
        long controls = Math.addExact(protocolBound, Math.addExact(1024, Math.multiplyExact(runEntries + entries, 32)));
        return protocolBound <= session.capabilitySnapshot().maximumAddPayloadBytes()
                && controls <= envelope.maximumEncodedBytes() - admittedBytes - bytes;
    }

    private long dataEntryLimit() {
        return Math.min(envelope.maximumEntries() / 2, Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT + 1L);
    }

    private CompletionStage<Void> rollover() {
        final KafkaCoherentProtocolSnapshotV1 snapshot;
        final KafkaBookKeeperRunLifecycleV1 old;
        synchronized (this) {
            requireWritable();
            old = run;
            snapshot = commit.capture();
        }
        var binding = old.snapshot().runBinding();
        long end = snapshot.root().frontiers().durableEndOffset();
        var sourceIndexes = new ArrayList<com.nereusstream.kafka.bookkeeper.checkpoint.KafkaCheckpointReadIndexV1>();
        historyRuns.stream()
                .filter(KafkaBookKeeperReadRunV1::active)
                .map(com.nereusstream.kafka.bookkeeper.checkpoint.KafkaCheckpointReadIndexV1::from)
                .forEach(sourceIndexes::add);
        if (!snapshot.activeTail().locators().isEmpty()) {
            sourceIndexes.add(com.nereusstream.kafka.bookkeeper.checkpoint.KafkaCheckpointReadIndexV1.from(
                    new KafkaBookKeeperReadRunV1(
                            binding,
                            old.snapshot().handle(),
                            snapshot.activeTail().startOffset(),
                            end,
                            0,
                            Optional.of(
                                    com.nereusstream.kafka.bookkeeper.read.KafkaPackedBatchLocatorIndexV1
                                            .fromActiveTail(snapshot.activeTail())),
                            Optional.empty())));
        }
        var checkpoint = new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(binding, end, end, end, end),
                snapshot.committedProducerState(),
                snapshot.transactionState(),
                snapshot.leaderEpochIndex(),
                sourceIndexes);
        var directory = new ArrayList<Nbke2IndexDirectoryEntryV1>();
        var rows = new ArrayList<Nbke2BatchLocatorV1>();
        long anchor = snapshot.activeTail().startOffset();
        long firstEntry = 1;
        for (int group = 0; group < snapshot.activeTail().locators().size(); group++) {
            for (var member : snapshot.activeTail().locators().get(group).members()) {
                rows.add(new Nbke2BatchLocatorV1(
                        member.startOffset() - anchor,
                        member.endOffsetExclusive() - member.startOffset(),
                        member.entryId() - firstEntry,
                        group,
                        0,
                        member.rawAssignedRecordBatchBytes(),
                        0));
            }
        }
        long checkpointEntry = old.snapshot().nextEntryId();
        long footerEntry = checkpointEntry + 1 + (rows.isEmpty() ? 0 : 1);
        var block = rows.isEmpty()
                ? null
                : new Nbke2RangeIndexBlockV1(
                        binding, anchor, firstEntry, end, firstEntry, checkpointEntry - 1, -1, footerEntry + 1, rows);
        if (block != null) {
            directory.add(new Nbke2IndexDirectoryEntryV1(checkpointEntry + 1, anchor, end));
        }
        var footer = new Nbke2RunFooterV1(
                binding,
                end,
                footerEntry + 1,
                block == null ? -1 : checkpointEntry + 1,
                checkpointEntry,
                owner.ownerEpoch(),
                directory);
        byte[] encodedCheckpoint = Nbke2CodecV1.encode(
                old.snapshot().handle().ledgerIdentity().ledgerId(), checkpointEntry, checkpoint.toNbke2());
        long controlBytes = encodedCheckpoint.length
                + Nbke2CodecV1.encode(old.snapshot().handle().ledgerIdentity().ledgerId(), footerEntry, footer).length;
        if (block != null) {
            controlBytes += Nbke2CodecV1.encode(
                            old.snapshot().handle().ledgerIdentity().ledgerId(), checkpointEntry + 1, block)
                    .length;
        }
        if (footerEntry + 1 > envelope.maximumEntries()
                || controlBytes > envelope.maximumEncodedBytes() - admittedBytes) {
            throw new IllegalStateException("compound checkpoint and index do not fit the reserved run control budget");
        }
        return old.appendProtocolCheckpoint(checkpoint.toNbke2())
                .thenCompose(ignored ->
                        block == null ? CompletableFuture.completedFuture(-1L) : old.appendRangeIndexBlock(block))
                .thenCompose(ignored -> old.drain())
                .thenCompose(ignored -> old.seal(footer))
                .thenCompose(ignored -> session.openRunLedger(old.snapshot().handle()))
                .thenCompose(opened -> {
                    if (!opened.exactHandle().equals(Optional.of(old.snapshot().handle()))) {
                        throw new IllegalStateException("sealed run was not reopened for protected reads");
                    }
                    synchronized (this) {
                        requireWritable();
                    }
                    return old.createSuccessor(new Nbke2RunBindingV1(
                            binding.bindingId(),
                            binding.topicIncarnation(),
                            binding.partitionId(),
                            binding.storageEpochId(),
                            binding.creatorOwnerEpoch(),
                            binding.kafkaLeaderEpoch(),
                            binding.providerScopeId(),
                            new StorageRunId(randomId())));
                })
                // The M4 control facade uses synchronous Oxia reads/CAS. Never block the Oxia completion thread.
                .thenAcceptAsync(
                        successor -> {
                            final List<KafkaBookKeeperReadRunV1> nextHistoryRuns;
                            final List<KafkaRunRootSnapshotV1> nextSealedRoots;
                            final NativeReadSources nextSources;
                            final Sha256Digest nextView;
                            final Sha256Digest expectedView;
                            final KafkaBookKeeperM4BrokerReadOwnerV2 selectedOwner;
                            final NativeSourcePublication sourcePublication;
                            BindingReadAuthorityV1 stoppedAuthority = null;
                            synchronized (this) {
                                requireWritable();
                                if (run != old || admissions != 0) {
                                    throw new IllegalStateException("run switch became stale");
                                }
                                nextHistoryRuns = new ArrayList<>(historyRuns);
                                if (block != null) {
                                    nextHistoryRuns.add(new KafkaBookKeeperReadRunV1(
                                            binding,
                                            old.snapshot().handle(),
                                            anchor,
                                            end,
                                            0,
                                            Optional.empty(),
                                            Optional.of(new KafkaPackedIndexDirectoryV1(directory))));
                                }
                                nextSealedRoots = new ArrayList<>(sealedHistoryRoots);
                                nextSealedRoots.add(old.snapshot().root());
                                nextSources = new NativeReadSources(
                                        nextSealedRoots,
                                        successor.snapshot().root(),
                                        nextHistoryRuns,
                                        session.capabilitySnapshot().configurationDigest());
                                nextView = nextSources.selectedViewSha256();
                                expectedView = nativeSourceViewSha256;
                                selectedOwner = m4ReadOwner;
                                sourcePublication = nativeSourcePublication;
                                if (selectedOwner != null) {
                                    var current = selectedOwner
                                            .selectorRuntime()
                                            .currentAuthority()
                                            .get();
                                    if (!capturedRawView(current).equals(expectedView)) {
                                        throw new IllegalStateException(
                                                "native BK source map differs from the M4 read owner");
                                    }
                                    stoppedAuthority = selectedOwner.stopNativeRolloverAdmission();
                                }
                            }
                            HistoricalReads nextReads = null;
                            if (selectedOwner != null) {
                                var predecessor = stoppedAuthority;
                                Runnable publication =
                                        () -> selectedOwner.publishNativeRollover(predecessor, nextSources, end);
                                if (sourcePublication == null) {
                                    publication.run();
                                } else {
                                    nextReads = Objects.requireNonNull(
                                            sourcePublication.publish(nextSources, publication),
                                            "admitted native readers");
                                }
                            }
                            synchronized (this) {
                                requireWritable();
                                if (run != old
                                        || admissions != 0
                                        || m4ReadOwner != selectedOwner
                                        || !nativeSourceViewSha256.equals(expectedView)) {
                                    throw new IllegalStateException(
                                            "native BK rollover source publication became stale");
                                }
                                var digest = Sha256Digest.hash(
                                        com.nereusstream.domain.bytes.CanonicalBytes.copyOf(Nbke2CodecV1.encode(
                                                old.snapshot()
                                                        .handle()
                                                        .ledgerIdentity()
                                                        .ledgerId(),
                                                footerEntry,
                                                footer)));
                                commit.switchBookKeeperRun(
                                        old.snapshot().handle(),
                                        successor.snapshot().handle(),
                                        checkpoint,
                                        digest);
                                historyRuns.clear();
                                historyRuns.addAll(nextHistoryRuns);
                                sealedHistoryRoots.clear();
                                sealedHistoryRoots.addAll(nextSealedRoots);
                                run = successor;
                                nativeOwnerRunIds.add(
                                        successor.snapshot().runBinding().runId());
                                nativeSourceViewSha256 = nextView;
                                if (sourcePublication != null) {
                                    historicalReads = nextReads;
                                }
                                pipeline = new KafkaBookKeeperOrderedPipelineV1(
                                        session, run, partitionCapacity, globalCapacity, commit);
                                dataAdmission = KafkaBookKeeperDataAdmissionV1.admitProfile(
                                        run.snapshot().runBinding(), session.capabilitySnapshot(), maximumBatchBytes);
                                checkpointBaseBytes = encodedCheckpoint.length;
                                if (pendingObjectSourceTransfer != null
                                        && pendingObjectSourceTransfer.recordCheckpoint(
                                                old.snapshot().root(),
                                                checkpoint,
                                                checkpointEntry,
                                                encodedCheckpoint,
                                                digest)) {
                                    pendingObjectSourceTransfer = null;
                                }
                                admittedEntries = runEntries = 1;
                                admittedBytes = runBytes = Nbke2CodecV1.encode(
                                                run.snapshot()
                                                        .handle()
                                                        .ledgerIdentity()
                                                        .ledgerId(),
                                                0,
                                                new Nbke2RunHeaderV1(
                                                        run.snapshot().runBinding(),
                                                        end,
                                                        1,
                                                        session.capabilitySnapshot()
                                                                .configurationDigest()))
                                        .length;
                            }
                        },
                        controlExecutor);
    }

    public CompletionStage<KafkaOrderedAppendResultV1> appendAssigned(
            AdmittedAppend admitted, List<KafkaNativeAssignedRecordBatchV1> batches) {
        if (admitted.owner != this) {
            throw new IllegalArgumentException("native append admission belongs to another partition");
        }
        try {
            synchronized (this) {
                requireWritable();
                admitted.handoff(this, batches);
            }
            var snapshot = capture();
            var plan = new KafkaProtocolAppendPlanV1(
                    snapshot.root().fence(),
                    batches.stream().map(protocolAdapter::protocolDelta).toList());
            var groupId = randomId();
            var attemptId = randomId();
            var operation = pipeline.submit(
                            admitted.request,
                            () -> new KafkaOffsetAssignedAppendV1(
                                    batches.get(0).baseOffset(),
                                    batches.get(batches.size() - 1).endOffsetExclusive(),
                                    entry -> KafkaAssignedRecordBatchGroupAdapterV1.adapt(
                                            snapshot.root().fence(),
                                            run.snapshot().runBinding(),
                                            entry,
                                            groupId,
                                            attemptId,
                                            batches,
                                            admitted.tickets)),
                            commit.protocolHooks(plan),
                            admitted.lease)
                    .thenApply(result -> {
                        if (result.outcome() != KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED) {
                            fence();
                        }
                        return result;
                    })
                    .whenComplete((result, failure) -> {
                        synchronized (this) {
                            admissions--;
                        }
                        if (failure != null) {
                            fence();
                        }
                    });
            return operation.thenApply(result -> result);
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                if (admitted.handedOff) {
                    admissions--;
                }
            }
            admitted.lease.close();
            fence();
            throw failure;
        }
    }

    public KafkaCoherentProtocolSnapshotV1 capture() {
        return commit.capture();
    }

    /** Called before exposing a recovered partition to Fetch; all later reads share this native M4 owner. */
    public synchronized void installHistoricalReads(HistoricalReads reads) {
        requireWritable();
        if (historicalReads != null || m4ReadOwner != null || activeReads != 0) {
            throw new IllegalStateException("historical reads must be installed before read admission");
        }
        historicalReads = Objects.requireNonNull(reads, "historical reads");
    }

    public synchronized void installM4ReadOwner(
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector selector,
            Executor ownerEventLoop,
            int maximumInFlightReads) {
        installM4ReadOwner(
                coordinator, selector, ownerEventLoop, maximumInFlightReads, new Semaphore(maximumInFlightReads));
    }

    public synchronized void installM4ReadOwner(
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector selector,
            Executor ownerEventLoop,
            int maximumInFlightReads,
            Semaphore cellReadSlots) {
        installM4ReadOwner(
                coordinator,
                selector,
                ownerEventLoop,
                maximumInFlightReads,
                cellReadSlots,
                new Semaphore(maximumInFlightReads));
    }

    public synchronized void installM4ReadOwner(
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector selector,
            Executor ownerEventLoop,
            int maximumInFlightReads,
            Semaphore cellReadSlots,
            Semaphore bindingReadSlots) {
        installM4ReadOwner(
                coordinator, selector, ownerEventLoop, maximumInFlightReads, cellReadSlots, bindingReadSlots, null);
    }

    public synchronized void installM4ReadOwner(
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector selector,
            Executor ownerEventLoop,
            int maximumInFlightReads,
            Semaphore cellReadSlots,
            Semaphore bindingReadSlots,
            HistoricalReads historicalReads) {
        installM4ReadOwner(
                coordinator,
                selector,
                ownerEventLoop,
                maximumInFlightReads,
                cellReadSlots,
                bindingReadSlots,
                historicalReads,
                null);
    }

    public synchronized void installM4ReadOwner(
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector selector,
            Executor ownerEventLoop,
            int maximumInFlightReads,
            Semaphore cellReadSlots,
            Semaphore bindingReadSlots,
            HistoricalReads historicalReads,
            NativeSourcePublication nativeSourcePublication) {
        requireWritable();
        if (m4ReadOwner != null) {
            throw new IllegalStateException("native M4 read owner is already installed");
        }
        var sources = nativeReadSources();
        var scope = sources.activeRoot();
        var expectedBinding = new BindingIdentity(
                scope.bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(scope.topicIncarnation())),
                scope.storageEpochId().digest());
        requireNativeReadPublication(sources, selector);
        if (recoveredObjectReadSources != null && historicalReads == null) {
            throw new IllegalStateException("cold Object reader lacks its actual owned BK read lifetime");
        }
        Object payload = recoveredObjectReadSources != null
                ? recoveredObjectReadSources
                : nativePreferredCache == null
                        ? sources
                        : NativePreferredReadSources.recovered(sources, nativePreferredCache.descriptor());
        var cell = new BindingReadPublicationCellV1(
                selector.sourceGeneration(),
                capture().root().frontiers().durableEndOffset(),
                0,
                new BindingReadRouteTableV1(List.of()),
                payload);
        var authority = new BindingReadAuthorityV1(
                scope.bindingId(),
                expectedBinding.incarnationSha256(),
                new StorageEpochId(expectedBinding.storageEpochSha256()),
                BindingReadProtocolV1.KAFKA_OFFSET,
                selector.selectedViewSha256(),
                selector.ownerEpoch(),
                selector.readAdmissionEpoch(),
                true,
                selector.capability().generation(),
                selector.capability().evidenceSha256(),
                cell);
        m4ReadOwner = new KafkaBookKeeperM4BrokerReadOwnerV2(
                coordinator,
                selector,
                authority,
                ownerEventLoop,
                maximumInFlightReads,
                cellReadSlots,
                bindingReadSlots);
        this.historicalReads = historicalReads;
        this.nativeSourcePublication = nativeSourcePublication;
        recoveredObjectReadSources = null;
    }

    /** Keep the validated cold view available while the exact raw-member writer tickets cover its Owner grant. */
    public synchronized void prepareRecoveredObjectReadSources(
            KafkaBookKeeperNativeObjectReadSourcesV1 selected, BindingReadSelector predecessor) {
        requireWritable();
        if (m4ReadOwner != null
                || nativePreferredCache != null
                || recoveredObjectReadSources != null
                || !selected.rawSources().equals(nativeReadSourcesUnchecked())
                || predecessor.ownerEpoch() >= owner.ownerEpoch()) {
            throw new IllegalStateException("cold Object read sources differ from the recovered native Owner");
        }
        selected.requireSelector(predecessor);
        recoveredObjectReadSources = selected;
    }

    /** Native membership, coherent protocol state and its resolved checkpoint under one rollover boundary. */
    public record NativeInputSources(
            NativeReadSources sources,
            KafkaCoherentProtocolSnapshotV1 protocol,
            Optional<KafkaProtocolCheckpointStateV1> recoveryCheckpoint) {
        public NativeInputSources {
            Objects.requireNonNull(sources, "sources");
            Objects.requireNonNull(protocol, "protocol");
            Objects.requireNonNull(recoveryCheckpoint, "recoveryCheckpoint");
        }
    }

    /** Metadata-only source projection. The owning Task separately retains the recovered cache capacity. */
    public record NativePreferredReadSources(
            NativeReadSources rawSources,
            KafkaSealedBookKeeperDescriptorV2 descriptor,
            Optional<NativeReadSources> publicationSources,
            Optional<KafkaSealedBookKeeperDescriptorV2> publicationPreferred) {
        public NativePreferredReadSources(
                NativeReadSources rawSources,
                KafkaSealedBookKeeperDescriptorV2 descriptor,
                Optional<NativeReadSources> publicationSources) {
            this(rawSources, descriptor, publicationSources, Optional.empty());
        }

        public NativePreferredReadSources(NativeReadSources rawSources, KafkaSealedBookKeeperDescriptorV2 descriptor) {
            this(rawSources, descriptor, Optional.of(rawSources));
        }

        public static NativePreferredReadSources recovered(
                NativeReadSources tail, KafkaSealedBookKeeperDescriptorV2 descriptor) {
            return new NativePreferredReadSources(tail, descriptor, Optional.empty());
        }

        public NativePreferredReadSources {
            Objects.requireNonNull(rawSources, "rawSources");
            Objects.requireNonNull(descriptor, "descriptor");
            Objects.requireNonNull(publicationSources, "publicationSources");
            Objects.requireNonNull(publicationPreferred, "publicationPreferred");
            var cut = descriptor.sourceCut();
            var root = rawSources.activeRoot();
            var binding = cut.identity().binding();
            if (publicationSources
                            .filter(value -> !cut.predecessorViewSha256()
                                    .equals(publicationPreferred
                                            .map(KafkaSealedBookKeeperDescriptorV2::descriptorSha256)
                                            .orElseGet(value::selectedViewSha256)))
                            .isPresent()
                    || !root.bindingId().equals(binding.bindingId())
                    || !Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(root.topicIncarnation()))
                            .equals(binding.incarnationSha256())
                    || !root.storageEpochId().digest().equals(binding.storageEpochSha256())
                    || !root.providerScopeId()
                            .equals(descriptor.task().capability().providerScopeId())
                    || !rawSources
                            .configurationSha256()
                            .equals(descriptor.task().capability().configurationDigest())
                    || descriptor.recoveryStateLocator().isEmpty()) {
                throw new IllegalArgumentException(
                        "native preferred descriptor differs from its complete raw source map");
            }
            if (publicationSources.isPresent()) {
                requireSourceExtension(publicationSources.orElseThrow(), rawSources);
                publicationPreferred.ifPresent(
                        previous -> requireSelectedPredecessor(previous, descriptor, rawSources));
            } else {
                if (publicationPreferred.isPresent()) {
                    throw new IllegalArgumentException("recovered native tail retains a publication predecessor");
                }
                rawSources.selectedViewSha256();
                long start = rawSources.sealedRoots().isEmpty()
                        ? root.kafkaStartOffset()
                        : rawSources.sealedRoots().get(0).kafkaStartOffset();
                if (start != cut.coverage().exclusiveEnd()) {
                    throw new IllegalArgumentException(
                            "recovered native tail does not start at the exact selected cut");
                }
            }
        }

        private static void requireSelectedPredecessor(
                KafkaSealedBookKeeperDescriptorV2 previous,
                KafkaSealedBookKeeperDescriptorV2 descriptor,
                NativeReadSources raw) {
            var cut = descriptor.sourceCut();
            var first = cut.sources().get(0);
            long rawStart = raw.sealedRoots().isEmpty()
                    ? raw.activeRoot().kafkaStartOffset()
                    : raw.sealedRoots().get(0).kafkaStartOffset();
            if (first.kind()
                            != com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind
                                    .KAFKA_BK_COMPACTED_GENERATION_V2
                    || !first.formatRootSha256().equals(previous.descriptorSha256())
                    || !first.coverage().equals(previous.sourceCut().coverage())
                    || cut.coverage().inclusiveStart() != first.coverage().inclusiveStart()
                    || rawStart != first.coverage().exclusiveEnd()
                    || !previous.task().namespace().equals(descriptor.task().namespace())
                    || !previous.sourceCut()
                            .identity()
                            .binding()
                            .equals(cut.identity().binding())) {
                throw new IllegalArgumentException("native replacement differs from its actual selected predecessor");
            }
        }

        /** Remove only the exact closed prefix independently represented by this selected descriptor. */
        NativePreferredReadSources retiredTail() {
            if (publicationSources.isEmpty()) {
                return this;
            }
            var cut = descriptor.sourceCut();
            var remaining = new java.util.HashMap<
                    Sha256Digest,
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent>();
            for (var extent : cut.sources()) {
                if (publicationPreferred.isPresent()
                        && extent.equals(cut.sources().get(0))) {
                    continue;
                }
                if (extent.kind()
                                != com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind
                                        .BOOKKEEPER_LEDGER
                        || remaining.put(extent.ledgerIdentitySha256().orElseThrow(), extent) != null) {
                    throw new IllegalStateException("selected native prefix repeats or changes its physical source");
                }
            }
            long end = publicationPreferred
                    .map(previous -> previous.sourceCut().coverage().exclusiveEnd())
                    .orElse(cut.coverage().inclusiveStart());
            int covered = 0;
            for (var root : rawSources.sealedRoots()) {
                if (root.kafkaStartOffset() >= cut.coverage().exclusiveEnd()) {
                    break;
                }
                long next = root.kafkaEndOffsetExclusive().orElseThrow();
                if (root.kafkaStartOffset() != end || next > cut.coverage().exclusiveEnd()) {
                    throw new IllegalStateException("selected native prefix crosses a raw run boundary");
                }
                if (next != end) {
                    var physical = new com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger(
                            descriptor.task().namespace(), root.ledgerIdentity().ledgerId());
                    var extent = remaining.remove(physical.sha256());
                    if (extent == null
                            || extent.coverage().inclusiveStart() != end
                            || extent.coverage().exclusiveEnd() != next) {
                        throw new IllegalStateException(
                                "selected native prefix differs from its exact physical member");
                    }
                }
                end = next;
                covered++;
            }
            if (end != cut.coverage().exclusiveEnd() || !remaining.isEmpty()) {
                throw new IllegalStateException("selected native prefix lacks its complete original membership");
            }
            var tailRoots = rawSources
                    .sealedRoots()
                    .subList(covered, rawSources.sealedRoots().size());
            var tailIds =
                    tailRoots.stream().map(KafkaRunRootSnapshotV1::runId).collect(java.util.stream.Collectors.toSet());
            var tail = new NativeReadSources(
                    tailRoots,
                    rawSources.activeRoot(),
                    rawSources.sealedReadRuns().stream()
                            .filter(read -> tailIds.contains(read.runBinding().runId()))
                            .toList(),
                    rawSources.configurationSha256());
            return recovered(tail, descriptor);
        }

        public NativePreferredReadSources withRawSources(NativeReadSources successor) {
            requireSourceExtension(rawSources, successor);
            return new NativePreferredReadSources(successor, descriptor, publicationSources, publicationPreferred);
        }

        /** The actual original introduced fallback identities stay unchanged across raw tail rollover. */
        public List<com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity>
                fallbackSources() {
            var predecessor = descriptor.sourceCut().predecessorSelector();
            if (predecessor.mode() != SelectorMode.PREFERRED_ONLY
                    || predecessor.fallbackSetSha256().isPresent()) {
                throw new IllegalStateException("native preferred source lacks its original fallback introduction");
            }
            return descriptor.sourceCut().sources().stream()
                    .map(source ->
                            new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1
                                    .SourceProtectionIdentity(
                                    source.sourceIdentitySha256(),
                                    descriptor.sourceGeneration(),
                                    Math.addExact(predecessor.readAdmissionEpoch(), 1),
                                    descriptor.sourceGeneration(),
                                    predecessor.capability()))
                    .sorted(java.util.Comparator.comparing(
                            source -> source.sourceIdentitySha256().toHex()))
                    .toList();
        }

        static void requireSourceExtension(NativeReadSources previous, NativeReadSources successor) {
            // Hashing validates the complete scope, root lineage and one exact read index per non-empty sealed run.
            successor.selectedViewSha256();
            int sealed = previous.sealedRoots().size();
            int reads = previous.sealedReadRuns().size();
            if (!previous.configurationSha256().equals(successor.configurationSha256())
                    || successor.sealedRoots().size() < sealed
                    || successor.sealedReadRuns().size() < reads
                    || !successor.sealedRoots().subList(0, sealed).equals(previous.sealedRoots())
                    || !successor.sealedReadRuns().subList(0, reads).equals(previous.sealedReadRuns())) {
                throw new IllegalStateException("native preferred tail changed an existing sealed source/index");
            }
            var oldTail = previous.activeRoot();
            var retainedTail = successor.sealedRoots().size() == sealed
                    ? successor.activeRoot()
                    : successor.sealedRoots().get(sealed);
            if (!KafkaRunRootRecordV2.Scope.of(oldTail).equals(KafkaRunRootRecordV2.Scope.of(retainedTail))
                    || !oldTail.runId().equals(retainedTail.runId())
                    || !oldTail.ledgerIdentity().equals(retainedTail.ledgerIdentity())
                    || oldTail.creatorOwnerEpoch() != retainedTail.creatorOwnerEpoch()
                    || oldTail.kafkaLeaderEpoch() != retainedTail.kafkaLeaderEpoch()
                    || oldTail.kafkaStartOffset() != retainedTail.kafkaStartOffset()
                    || !oldTail.predecessorRunId().equals(retainedTail.predecessorRunId())) {
                throw new IllegalStateException("native preferred tail lost its original active run identity");
            }
        }
    }

    /** Check the actual raw members against the installed local projection before source tickets are released. */
    public synchronized void requireNativeReadPublication(NativeReadSources sources, BindingReadSelector selector) {
        requireNativeReadPublication(sources, selector, false);
    }

    private void requireNativeReadPublication(
            NativeReadSources sources, BindingReadSelector selector, boolean retainedPublication) {
        var scope = sources.activeRoot();
        var binding = new BindingIdentity(
                scope.bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(scope.topicIncarnation())),
                scope.storageEpochId().digest());
        if (!selector.binding().equals(binding)
                || selector.ownerEpoch() != owner.ownerEpoch()
                || selector.admissionState() != AdmissionState.ADMITTING) {
            throw new IllegalStateException("native source publication differs from its exact admitted selector");
        }
        if (m4ReadOwner == null) {
            if (recoveredObjectReadSources != null) {
                if (!recoveredObjectReadSources.rawSources().equals(sources)) {
                    throw new IllegalStateException("native Object Owner grant changed its exact BK sources");
                }
                recoveredObjectReadSources.requireSelector(selector);
            } else if (nativePreferredCache == null) {
                if (selector.mode() != SelectorMode.PREFERRED_ONLY
                        || !selector.selectedViewSha256().equals(sources.selectedViewSha256())) {
                    throw new IllegalStateException("initial native source publication is outside its raw selector");
                }
            } else {
                var preferred = NativePreferredReadSources.recovered(sources, nativePreferredCache.descriptor());
                if (!selector.selectedViewSha256().equals(preferred.descriptor().descriptorSha256())
                        || selector.sourceGeneration() < preferred.descriptor().sourceGeneration()
                        || selector.mode() != SelectorMode.PREFERRED_ONLY
                                && selector.mode() != SelectorMode.PREFERRED_WITH_FALLBACK
                        || selector.mode() == SelectorMode.PREFERRED_WITH_FALLBACK
                                && !selector.fallbackSetSha256()
                                        .equals(Optional.of(
                                                com.nereusstream.storage.object.read.control.M4ReadControlCodecV1
                                                        .calculateFallbackSetSha256(preferred.fallbackSources())))) {
                    throw new IllegalStateException(
                            "initial native source publication differs from selected output/tail");
                }
            }
            return;
        }
        var local = m4ReadOwner.selectorRuntime().currentAuthority().get();
        var payload = local.publicationCell().protocolStateReference();
        var raw = payload instanceof KafkaBookKeeperNativeObjectReadSourcesV1 object
                ? object.rawSources()
                : payload instanceof NativePreferredReadSources preferred ? preferred.rawSources() : payload;
        if (!raw.equals(sources)
                || !local.admitting() && !retainedPublication
                || !local.selectedViewSha256().equals(selector.selectedViewSha256())
                || local.ownerEpoch() != selector.ownerEpoch()
                || local.readAdmissionEpoch() != selector.readAdmissionEpoch()
                || local.sourceGeneration() != selector.sourceGeneration()
                || local.capabilityGeneration() != selector.capability().generation()
                || !local.capabilityEvidenceSha256()
                        .equals(selector.capability().evidenceSha256())) {
            throw new IllegalStateException("native source publication differs from its exact local source projection");
        }
    }

    /** Close local read/append admission before the finite remote publication, preserving accepted reads. */
    public synchronized CompletionStage<Void> beginNativePreferredPublication(
            KafkaSealedBookKeeperDescriptorV2 descriptor) {
        requireWritable();
        if (m4ReadOwner == null
                || rolling
                || nativeObjectPublication != null
                || nativeObjectRetirement != null
                || admissions != 0
                || !commit.capture().speculativeQueue().commits().isEmpty()) {
            throw new IllegalStateException("native preferred publication lacks a quiescent qualified owner");
        }
        nativePublicationSources(descriptor);
        var id = descriptor.task().taskIdSha256();
        if (nativePublicationTask != null && !nativePublicationTask.equals(id)) {
            throw new IllegalStateException("native preferred publication is owned by another Task");
        }
        var current = m4ReadOwner.selectorRuntime().currentAuthority().get();
        if (!current.selectedViewSha256().equals(descriptor.sourceCut().predecessorViewSha256())) {
            throw new IllegalStateException("native preferred publication lacks its exact predecessor source map");
        }
        if (current.admitting()) {
            m4ReadOwner.stopNativeRolloverAdmission();
        }
        if (nativePublicationTask == null) {
            nativePublicationPredecessor = nativePreferredCache;
        }
        nativePublicationTask = id;
        // Replacing the only selected cache requires actual accepted SDK/hazard drain before the CAS.
        // The initial raw publication keeps its existing finite fallback protection for accepted reads.
        return nativePreferredCache == null ? CompletableFuture.completedFuture(null) : m4ReadOwner.drain();
    }

    /** Begin only after Object IO and physical-reference admission; existing BK DATA can continue in this run. */
    public synchronized NativeObjectPublication beginNativeObjectPublication(
            KafkaObjectMaterializedPrefixV1 prefix,
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace bookKeeperNamespace,
            java.util.concurrent.Executor providerReads) {
        requireWritable();
        if (m4ReadOwner == null
                || nativePreferredCache != null
                || rolling
                || nativePublicationTask != null
                || nativeObjectRetirement != null
                || nativeObjectPublication != null) {
            throw new IllegalStateException("native Object publication requires its original raw BK reader");
        }
        var raw = nativeReadSourcesUnchecked();
        var current = m4ReadOwner.selectorRuntime().currentAuthority().get();
        var expected = prefix.sourceCut().predecessorSelector();
        if (!current.admitting()
                || !current.selectedViewSha256().equals(raw.selectedViewSha256())
                || !current.selectedViewSha256().equals(expected.selectedViewSha256())
                || current.ownerEpoch() != expected.ownerEpoch()
                || current.readAdmissionEpoch() != expected.readAdmissionEpoch()
                || current.sourceGeneration() != expected.sourceGeneration()) {
            throw new IllegalStateException("native Object publication has another raw BK predecessor");
        }
        var selected =
                new KafkaBookKeeperNativeObjectReadSourcesV1(raw, prefix, bookKeeperNamespace, true, providerReads);
        m4ReadOwner.stopNativeRolloverAdmission();
        nativeObjectPublication = new NativeObjectPublication(m4ReadOwner, selected);
        return nativeObjectPublication;
    }

    /** Reuses only the local fence for the original Object manifest after its exact durable selection is observed. */
    public void finishSelectedNativeObjectPublication(Sha256Digest manifest) {
        final NativeObjectPublication original;
        synchronized (this) {
            requireWritable();
            original = nativeObjectPublication;
            if (original == null) {
                if (m4ReadOwner != null) {
                    var current =
                            m4ReadOwner.selectorRuntime().currentAuthority().get();
                    if (current.admitting()
                            && current.publicationCell().protocolStateReference()
                                    instanceof KafkaBookKeeperNativeObjectReadSourcesV1 selected
                            && selected.prefix().manifestSha256().equals(manifest)) {
                        return;
                    }
                }
                throw new IllegalStateException("native Object recovery lacks its original local publication");
            }
            if (!original.sources.prefix().manifestSha256().equals(manifest)) {
                throw new IllegalStateException("native Object recovery cannot install another manifest");
            }
        }
        original.installSelected();
    }

    public final class NativeObjectPublication {
        private final KafkaBookKeeperM4BrokerReadOwnerV2 readOwner;
        private final KafkaBookKeeperNativeObjectReadSourcesV1 sources;

        private NativeObjectPublication(
                KafkaBookKeeperM4BrokerReadOwnerV2 readOwner, KafkaBookKeeperNativeObjectReadSourcesV1 sources) {
            this.readOwner = readOwner;
            this.sources = sources;
        }

        /** An unresolved transition keeps its original read fence and does not select another generation. */
        public void installSelected() {
            final long upper;
            synchronized (KafkaBookKeeperPartitionV1.this) {
                requireCurrent();
                upper = capture().root().frontiers().durableEndOffset();
            }
            readOwner.installNativeObjectPreferred(sources, upper);
            synchronized (KafkaBookKeeperPartitionV1.this) {
                requireCurrent();
                nativeObjectPublication = null;
            }
        }

        private void requireCurrent() {
            requireWritable();
            if (nativeObjectPublication != this
                    || m4ReadOwner != readOwner
                    || rolling
                    || !nativeSourceViewSha256.equals(sources.rawSources().selectedViewSha256())) {
                throw new IllegalStateException("native Object publication lost its exact original owner/source map");
            }
        }
    }

    private NativePreferredReadSources nativePublicationSources(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        var raw = nativeReadSourcesUnchecked();
        var previous = nativePublicationTask == null ? nativePreferredCache : nativePublicationPredecessor;
        return previous == null
                ? new NativePreferredReadSources(raw, descriptor)
                : new NativePreferredReadSources(raw, descriptor, Optional.of(raw), Optional.of(previous.descriptor()));
    }

    /** Privately created only after actual predecessor drain and exact successor actor activation. */
    public static final class NativePreferredActivation {
        private final KafkaSealedBookKeeperDescriptorV2 descriptor;
        private final Optional<Sha256Digest> replacedDescriptorSha256;

        private NativePreferredActivation(
                KafkaSealedBookKeeperDescriptorV2 descriptor, KafkaBookKeeperReadViewV2 previous) {
            this.descriptor = descriptor;
            replacedDescriptorSha256 = Optional.ofNullable(previous)
                    .map(value -> value.descriptor().descriptorSha256());
        }

        public void requireTask(Sha256Digest task) {
            if (!descriptor.task().taskIdSha256().equals(task)) {
                throw new IllegalStateException("native cache activation belongs to another Task");
            }
        }

        public Sha256Digest descriptorSha256() {
            return descriptor.descriptorSha256();
        }

        public Optional<Sha256Digest> replacedDescriptorSha256() {
            return replacedDescriptorSha256;
        }
    }

    /** Caller retains the actual Task's cache charge until this read owner has drained. */
    public NativePreferredActivation installNativePreferred(KafkaBookKeeperReadViewV2 preferred) {
        final KafkaBookKeeperM4BrokerReadOwnerV2 readOwner;
        final NativePreferredReadSources sources;
        final KafkaBookKeeperReadViewV2 previous;
        final long end;
        synchronized (this) {
            requireWritable();
            if (!preferred.descriptor().task().taskIdSha256().equals(nativePublicationTask)
                    || preferred.recoveryState().isEmpty()) {
                throw new IllegalStateException("native preferred cache lacks its owned publication/protocol state");
            }
            sources = nativePublicationSources(preferred.descriptor());
            end = capture().root().frontiers().durableEndOffset();
            readOwner = m4ReadOwner;
            previous = nativePublicationPredecessor;
            if (previous != null) {
                readOwner.requireConfirmedDrain();
            }
            nativePreferredCache = preferred;
        }
        // Exact metadata reads run on the existing owner control executor, outside the partition lock.
        var successor = readOwner.installNativePreferred(sources, end);
        synchronized (this) {
            requireWritable();
            if (m4ReadOwner != readOwner
                    || !nativeSourceViewSha256.equals(sources.rawSources().selectedViewSha256())) {
                throw new IllegalStateException("native preferred activation changed its owner/source map");
            }
            m4ReadOwner = successor;
            nativeFallbackRetirement = null;
            nativePublicationTask = null;
            nativePublicationPredecessor = null;
            return new NativePreferredActivation(preferred.descriptor(), previous);
        }
    }

    public void resumeNativeRawAfterNonSelection(Sha256Digest task) {
        final NativeReadSources sources;
        final KafkaBookKeeperM4BrokerReadOwnerV2 readOwner;
        final long end;
        synchronized (this) {
            requireWritable();
            if (!task.equals(nativePublicationTask)) {
                throw new IllegalStateException("native raw resume lacks its non-selected publication");
            }
            sources = nativeReadSourcesUnchecked();
            readOwner = m4ReadOwner;
            end = capture().root().frontiers().durableEndOffset();
        }
        var successor = nativePreferredCache == null
                ? readOwner.resumeNativeRaw(sources, end)
                : readOwner.resumeNativePreferred(
                        NativePreferredReadSources.recovered(sources, nativePreferredCache.descriptor()), end);
        synchronized (this) {
            requireWritable();
            if (m4ReadOwner != readOwner) {
                throw new IllegalStateException("native publication resume changed its read actor");
            }
            m4ReadOwner = successor;
            nativePublicationTask = null;
            nativePublicationPredecessor = null;
        }
    }

    public synchronized KafkaBookKeeperNativeFallbackRetirementV2 beginNativeFallbackRetirement() {
        requireWritable();
        if (nativePreferredCache == null || m4ReadOwner == null || rolling) {
            throw new IllegalStateException("native fallback retirement lacks a stable actual selected cache");
        }
        if (nativeFallbackRetirement != null) {
            if (!nativeFallbackRetirement
                    .descriptorSha256()
                    .equals(nativePreferredCache.descriptor().descriptorSha256())) {
                throw new IllegalStateException("native fallback retirement belongs to another descriptor");
            }
            return nativeFallbackRetirement;
        }
        if (nativePublicationTask != null) {
            throw new IllegalStateException("another native publication is active");
        }
        if (admissions != 0 || !commit.capture().speculativeQueue().commits().isEmpty()) {
            throw new IllegalStateException("native fallback retirement requires its actual append drain");
        }
        var current = m4ReadOwner.selectorRuntime().currentAuthority().get();
        if (!(current.publicationCell().protocolStateReference() instanceof NativePreferredReadSources preferred)
                || !preferred
                        .descriptor()
                        .descriptorSha256()
                        .equals(nativePreferredCache.descriptor().descriptorSha256())) {
            throw new IllegalStateException("native fallback retirement cache differs from its actual reader");
        }
        nativeFallbackRetirement = m4ReadOwner.beginNativeFallbackRetirement();
        nativePublicationTask = nativePreferredCache.descriptor().task().taskIdSha256();
        return nativeFallbackRetirement;
    }

    /** Closes only Object fallback admission; the original BK protocol recovery references remain live. */
    public synchronized KafkaBookKeeperNativeFallbackRetirementV2 beginNativeObjectFallbackRetirement(
            Sha256Digest selectedManifest) {
        requireWritable();
        if (m4ReadOwner == null || rolling || nativeObjectPublication != null || nativePublicationTask != null) {
            throw new IllegalStateException("native Object fallback retirement lacks a stable read owner");
        }
        if (nativeObjectRetirement != null) {
            if (!nativeObjectRetirement.descriptorSha256().equals(selectedManifest)) {
                throw new IllegalStateException("native Object fallback retirement belongs to another manifest");
            }
            return nativeObjectRetirement;
        }
        var current = m4ReadOwner.selectorRuntime().currentAuthority().get();
        if (!(current.publicationCell().protocolStateReference()
                        instanceof KafkaBookKeeperNativeObjectReadSourcesV1 sources)
                || !sources.fallback()
                || !sources.prefix().manifestSha256().equals(selectedManifest)) {
            throw new IllegalStateException("native Object fallback retirement requires its exact protected view");
        }
        nativeObjectRetirement = m4ReadOwner.beginNativeFallbackRetirement();
        return nativeObjectRetirement;
    }

    public CompletionStage<Void> closeNativeObjectFallbackAdmission(
            KafkaBookKeeperNativeFallbackRetirementV2 retirement, java.util.concurrent.Executor control) {
        synchronized (this) {
            requireObjectFallbackRetirement(retirement);
        }
        return retirement
                .closeObjectAdmission(control)
                .thenAccept(next -> installPreferredObjectOwner(retirement, next))
                .thenApply(ignored -> null);
    }

    private synchronized void installPreferredObjectOwner(
            KafkaBookKeeperNativeFallbackRetirementV2 retirement, KafkaBookKeeperM4BrokerReadOwnerV2 next) {
        requireObjectFallbackRetirement(retirement);
        if (m4ReadOwner == next) {
            return;
        }
        if (rolling || m4ReadOwner != retirement.predecessorOwner()) {
            throw new IllegalStateException("native Object fallback retirement changed its original read actor");
        }
        var authority = next.selectorRuntime().currentAuthority().get();
        if (!(authority.publicationCell().protocolStateReference()
                        instanceof KafkaBookKeeperNativeObjectReadSourcesV1 preferred)
                || preferred.fallback()
                || !preferred.rawSources().equals(nativeReadSourcesUnchecked())) {
            throw new IllegalStateException("native Object fallback retirement changed its BK recovery references");
        }
        m4ReadOwner = next;
    }

    public CompletionStage<KafkaBookKeeperNativeFallbackRetirementV2.Result> finishNativeObjectFallbackRetirement(
            KafkaBookKeeperNativeFallbackRetirementV2 retirement, java.util.concurrent.Executor control) {
        final KafkaBookKeeperM4BrokerReadOwnerV2 installed;
        synchronized (this) {
            requireObjectFallbackRetirement(retirement);
            installed = m4ReadOwner;
        }
        return retirement
                .releaseObjectAfterDrain(control, installed)
                .thenApply(result -> {
                    completeNativeObjectFallbackRetirement(retirement, result);
                    return result;
                })
                .thenApply(value -> value);
    }

    private synchronized void completeNativeObjectFallbackRetirement(
            KafkaBookKeeperNativeFallbackRetirementV2 retirement,
            KafkaBookKeeperNativeFallbackRetirementV2.Result result) {
        requireObjectFallbackRetirement(retirement);
        if (!retirement.owns(result) || m4ReadOwner != result.readOwner()) {
            throw new IllegalStateException("native Object fallback retirement changed its actual read actor");
        }
        nativeObjectRetirement = null;
    }

    private void requireObjectFallbackRetirement(KafkaBookKeeperNativeFallbackRetirementV2 retirement) {
        requireWritable();
        if (nativeObjectRetirement == null || nativeObjectRetirement != retirement) {
            throw new IllegalStateException("native Object fallback retirement changed its original manifest");
        }
    }

    /** Actual protocol-source transfer; no persistent retirement or physical delete authority is granted here. */
    public static final class ObjectBookKeeperSourceTransfer {
        private final KafkaBookKeeperNativeFallbackRetirementV2.Result release;
        private final NativeReadSources original;
        private final NativeReadSources retained;
        private final KafkaProtocolCheckpointStateV1 checkpoint;
        private volatile ObjectRetainedCheckpoint durableCheckpoint;

        private ObjectBookKeeperSourceTransfer(
                KafkaBookKeeperNativeFallbackRetirementV2.Result release,
                NativeReadSources original,
                NativeReadSources retained,
                KafkaProtocolCheckpointStateV1 checkpoint) {
            this.release = release;
            this.original = original;
            this.retained = retained;
            this.checkpoint = checkpoint;
        }

        public KafkaBookKeeperNativeFallbackRetirementV2.Result release() {
            return release;
        }

        public NativeReadSources original() {
            return original;
        }

        public NativeReadSources retained() {
            return retained;
        }

        public KafkaProtocolCheckpointStateV1 checkpoint() {
            return checkpoint;
        }

        public Optional<ObjectRetainedCheckpoint> durableCheckpoint() {
            return Optional.ofNullable(durableCheckpoint);
        }

        private boolean recordCheckpoint(
                KafkaRunRootSnapshotV1 root,
                KafkaProtocolCheckpointStateV1 sealed,
                long entry,
                byte[] frame,
                Sha256Digest footer) {
            var removed = removedBookKeeperRuns(original, retained);
            if (removed.contains(root.runId())
                    || !root.runId().equals(sealed.vector().runBinding().runId())
                    || root.kafkaEndOffsetExclusive().orElse(-1)
                            != sealed.vector().recoveryCoveredThrough()
                    || sealed.readIndexes().stream().anyMatch(index -> removed.contains(index.runId()))) {
                return false;
            }
            durableCheckpoint = new ObjectRetainedCheckpoint(
                    root, entry, Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(frame)), footer);
            return true;
        }
    }

    public record ObjectRetainedCheckpoint(
            KafkaRunRootSnapshotV1 sealedCarrier,
            long checkpointEntry,
            Sha256Digest checkpointSha256,
            Sha256Digest footerSha256) {}

    public synchronized ObjectBookKeeperSourceTransfer transferRetiredObjectBookKeeperPrefix(
            KafkaBookKeeperNativeFallbackRetirementV2.Result release) {
        requireWritable();
        if (nativeObjectRetirement != null
                || release.readOwner() != m4ReadOwner
                || rolling
                || admissions != 0
                || !commit.capture().speculativeQueue().commits().isEmpty()) {
            throw new IllegalStateException(
                    "Object BK source transfer requires its actual drained epoch and coherent protocol cut");
        }
        var authority = m4ReadOwner.selectorRuntime().currentAuthority().get();
        if (!authority.admitting()
                || !(authority.publicationCell().protocolStateReference()
                        instanceof KafkaBookKeeperNativeObjectReadSourcesV1 object)
                || object.fallback()
                || authority.readAdmissionEpoch() != release.selector().readAdmissionEpoch()
                || !object.prefix().manifestSha256().equals(release.selector().selectedViewSha256())
                || !object.prefix()
                        .originalFallbackSources()
                        .equals(release.batch().sources())) {
            throw new IllegalStateException("Object BK source transfer differs from its exact released fallback set");
        }
        var original = nativeReadSourcesUnchecked();
        if (!object.rawSources().equals(original)) {
            throw new IllegalStateException("Object BK source transfer has a different native source map");
        }
        var tail = object.retainedBookKeeperTail();
        var retained = tail.rawSources();
        var removed = removedBookKeeperRuns(original, retained);
        var current = commit.captureRecoveryCheckpoint(commit.capture());
        if (removed.isEmpty() || removed.contains(current.vector().runBinding().runId())) {
            throw new IllegalStateException(
                    "Object BK source transfer needs an independent retained checkpoint carrier");
        }
        var checkpoint = checkpointWithoutRemovedRuns(current, removed);
        var digest =
                Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(64)
                        .put(authority.selectedViewSha256().bytes().toByteArray())
                        .put(retained.selectedViewSha256().bytes().toByteArray())
                        .array()));
        commit.retireBookKeeperSources(checkpoint, digest);
        if (!m4ReadOwner.transferRetainedObjectTail(authority, tail)) {
            fence();
            throw new IllegalStateException("Object BK source transfer lost its original reader");
        }
        historyRuns.clear();
        historyRuns.addAll(retained.sealedReadRuns());
        sealedHistoryRoots.clear();
        sealedHistoryRoots.addAll(retained.sealedRoots());
        nativeSourceViewSha256 = retained.selectedViewSha256();
        pendingObjectSourceTransfer = new ObjectBookKeeperSourceTransfer(release, original, retained, checkpoint);
        return pendingObjectSourceTransfer;
    }

    public synchronized void finishNativeFallbackRetirement(
            KafkaBookKeeperNativeFallbackRetirementV2 retirement,
            KafkaBookKeeperNativeFallbackRetirementV2.Result result) {
        requireWritable();
        if (nativeFallbackRetirement != retirement
                || !retirement.owns(result)
                || m4ReadOwner != retirement.predecessorOwner() && m4ReadOwner != result.readOwner()) {
            throw new IllegalStateException("native fallback retirement changed its actual read actor");
        }
        if (m4ReadOwner != result.readOwner()) {
            var before = nativeInputSources();
            var transferred = (NativePreferredReadSources) result.readOwner()
                    .selectorRuntime()
                    .currentAuthority()
                    .get()
                    .publicationCell()
                    .protocolStateReference();
            var old = nativeReadSourcesUnchecked();
            var checkpoint = transferredCheckpoint(old, transferred.rawSources());
            var digest = Sha256Digest.hash(
                    com.nereusstream.domain.bytes.CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(64)
                            .put(transferred
                                    .descriptor()
                                    .descriptorSha256()
                                    .bytes()
                                    .toByteArray())
                            .put(transferred
                                    .rawSources()
                                    .selectedViewSha256()
                                    .bytes()
                                    .toByteArray())
                            .array()));
            commit.retireBookKeeperSources(checkpoint, digest);
            historyRuns.clear();
            historyRuns.addAll(transferred.rawSources().sealedReadRuns());
            sealedHistoryRoots.clear();
            sealedHistoryRoots.addAll(transferred.rawSources().sealedRoots());
            nativeSourceViewSha256 = transferred.rawSources().selectedViewSha256();
            m4ReadOwner = result.readOwner();
            result.recordTransfer(before, nativeInputSources());
        }
    }

    /** The native owner calls this only after exact immutable transfer evidence is authoritatively stored. */
    public synchronized void completeNativeFallbackRetirement(
            KafkaBookKeeperNativeFallbackRetirementV2 retirement,
            KafkaBookKeeperNativeFallbackRetirementV2.Result result) {
        requireWritable();
        if (nativeFallbackRetirement != retirement || !retirement.owns(result) || m4ReadOwner != result.readOwner()) {
            throw new IllegalStateException("native fallback evidence belongs to another actual transfer");
        }
        result.transfer();
        nativePublicationTask = null;
    }

    private KafkaProtocolCheckpointStateV1 transferredCheckpoint(NativeReadSources previous, NativeReadSources tail) {
        var checkpoint = commit.captureRecoveryCheckpoint(commit.capture());
        var removed = removedBookKeeperRuns(previous, tail);
        if (removed.contains(checkpoint.vector().runBinding().runId())) {
            var state = nativePreferredCache.recoveryState().orElseThrow();
            if (checkpoint.vector().recoveryCoveredThrough() != state.coveredThrough()
                    || !checkpoint.components().equals(state.components())
                    || !KafkaCoherentCommitCoordinatorV1.recoveryCheckpointContentDigest(checkpoint)
                            .equals(state.sourceCheckpointSha256())) {
                throw new IllegalStateException("native source transfer lacks its independent exact checkpoint");
            }
            var source = state.sourceFence();
            var output = nativePreferredCache.descriptor().sealedParts().get(0).handle();
            return state.checkpointFor(new Nbke2RunBindingV1(
                    source.bindingId(),
                    source.topicIncarnation(),
                    source.partitionId(),
                    source.storageEpochId(),
                    source.ownerEpoch(),
                    source.kafkaLeaderEpoch(),
                    output.providerScopeId(),
                    output.runId()));
        }
        return checkpointWithoutRemovedRuns(checkpoint, removed);
    }

    private static java.util.Set<StorageRunId> removedBookKeeperRuns(
            NativeReadSources previous, NativeReadSources tail) {
        var retained = tail.sealedRoots().stream()
                .map(KafkaRunRootSnapshotV1::runId)
                .collect(java.util.stream.Collectors.toSet());
        return previous.sealedRoots().stream()
                .map(KafkaRunRootSnapshotV1::runId)
                .filter(id -> !retained.contains(id))
                .collect(java.util.stream.Collectors.toSet());
    }

    private static KafkaProtocolCheckpointStateV1 checkpointWithoutRemovedRuns(
            KafkaProtocolCheckpointStateV1 checkpoint, java.util.Set<StorageRunId> removed) {
        return new KafkaProtocolCheckpointStateV1(
                checkpoint.vector(),
                checkpoint.producerState(),
                checkpoint.transactionState(),
                checkpoint.leaderEpochIndex(),
                checkpoint.readIndexes().stream()
                        .filter(index -> !removed.contains(index.runId()))
                        .toList());
    }

    public synchronized NativeInputSources nativeInputSources() {
        var sources = nativeReadSources();
        var protocol = capture();
        // A fresh, unsealed run has only the bootstrap checkpoint seed. No checkpoint is manufactured for it.
        boolean objectSelected = m4ReadOwner != null
                && m4ReadOwner
                                .selectorRuntime()
                                .currentAuthority()
                                .get()
                                .publicationCell()
                                .protocolStateReference()
                        instanceof KafkaBookKeeperNativeObjectReadSourcesV1;
        var checkpoint = sources.sealedRoots().isEmpty() && nativePreferredCache == null && !objectSelected
                ? Optional.<KafkaProtocolCheckpointStateV1>empty()
                : Optional.of(commit.captureRecoveryCheckpoint(protocol));
        return new NativeInputSources(sources, protocol, checkpoint);
    }

    /** Capture the actual selected Object reader and the retained native protocol cut for physical reference checks. */
    public synchronized NativeInputSources nativeObjectRetainedInputSources(
            BindingReadSelector selector, KafkaBookKeeperObjectReadScopeV1 scope) {
        requireWritable();
        if (m4ReadOwner == null || nativeObjectPublication != null || nativeObjectRetirement != null || rolling) {
            throw new IllegalStateException("native Object reference capture lacks a stable reader");
        }
        var inputs = nativeInputSources();
        requireNativeReadPublication(inputs.sources(), selector);
        var selected = m4ReadOwner
                .selectorRuntime()
                .currentAuthority()
                .get()
                .publicationCell()
                .protocolStateReference();
        if (!(selected instanceof KafkaBookKeeperNativeObjectReadSourcesV1 object) || object.fallback()) {
            throw new IllegalStateException("native Object reference capture lacks its preferred-only reader");
        }
        object.requireSelector(selector);
        scope.requireSelection(object.prefix(), selector);
        var roots = inputs.sources().sealedRoots();
        if (roots.stream().anyMatch(root -> root.kafkaStartOffset() < scope.prefixEnd())
                || roots.stream().noneMatch(scope::namesRetainedCarrier)
                || inputs.recoveryCheckpoint().isEmpty()
                || !inputs.protocol().speculativeQueue().commits().isEmpty()) {
            throw new IllegalStateException("native Object reference capture lacks its complete retained protocol cut");
        }
        var checkpoint = inputs.recoveryCheckpoint().orElseThrow();
        var retainedRuns = new java.util.HashSet<StorageRunId>();
        roots.forEach(root -> retainedRuns.add(root.runId()));
        retainedRuns.add(inputs.sources().activeRoot().runId());
        if (!retainedRuns.contains(checkpoint.vector().runBinding().runId())
                || checkpoint.readIndexes().stream().anyMatch(index -> !retainedRuns.contains(index.runId()))) {
            throw new IllegalStateException(
                    "native Object reference capture still depends on a nonretained checkpoint run");
        }
        return inputs;
    }

    /** Captures the selected input only from this partition's actually installed read actor. */
    public synchronized Optional<NativeSelectedInputSources> nativeSelectedInputSources(
            NativeInputSources expected, BindingReadSelector selector) {
        return nativeSelectedInputSources(expected, selector, false);
    }

    private synchronized Optional<NativeSelectedInputSources> nativeSelectedInputSources(
            NativeInputSources expected, BindingReadSelector selector, boolean retained) {
        requireWritable();
        var actual = nativeInputSources();
        if (!actual.sources().equals(expected.sources())
                || !actual.protocol()
                        .root()
                        .fence()
                        .equals(expected.protocol().root().fence())) {
            throw new IllegalStateException("native selected input differs from its coherent source cut");
        }
        boolean stoppedPublication = retained
                && nativePublicationTask != null
                && nativePublicationPredecessor != null
                && nativePublicationPredecessor == nativePreferredCache;
        requireNativeReadPublication(expected.sources(), selector, stoppedPublication);
        if (nativePreferredCache == null) {
            return Optional.empty();
        }
        if (selector.mode() != SelectorMode.PREFERRED_ONLY
                || nativePublicationTask != null && !stoppedPublication
                || rolling
                || m4ReadOwner == null) {
            throw new IllegalStateException("native selected input requires its released stable read actor");
        }
        var payload = m4ReadOwner
                .selectorRuntime()
                .currentAuthority()
                .get()
                .publicationCell()
                .protocolStateReference();
        if (!(payload instanceof NativePreferredReadSources preferred)
                || !preferred.descriptor().equals(nativePreferredCache.descriptor())
                || !selector.selectedViewSha256().equals(preferred.descriptor().descriptorSha256())) {
            throw new IllegalStateException("native selected input differs from its installed descriptor");
        }
        return Optional.of(
                new NativeSelectedInputSources(this, expected, selector, preferred.descriptor(), nativeOwnerRunIds));
    }

    /** Data constructors cannot authorize a selected input outside the installed native read actor. */
    public static final class NativeSelectedInputSources {
        private final KafkaBookKeeperPartitionV1 partition;
        private final NativeInputSources inputs;
        private final BindingReadSelector selector;
        private final KafkaSealedBookKeeperDescriptorV2 descriptor;
        private final List<StorageRunId> ownerRunIds;

        private NativeSelectedInputSources(
                KafkaBookKeeperPartitionV1 partition,
                NativeInputSources inputs,
                BindingReadSelector selector,
                KafkaSealedBookKeeperDescriptorV2 descriptor,
                List<StorageRunId> ownerRunIds) {
            this.partition = partition;
            this.inputs = inputs;
            this.selector = selector;
            this.descriptor = descriptor;
            this.ownerRunIds = List.copyOf(ownerRunIds);
        }

        public List<StorageRunId> ownerRunIds() {
            return ownerRunIds;
        }

        public KafkaSealedBookKeeperDescriptorV2 descriptor() {
            return descriptor;
        }

        public BindingReadSelector selector() {
            return selector;
        }

        /** Exact recovery state for a low-frequency replacement cut, rechecked against the live partition. */
        public NativeInputSources recoveryInputs() {
            synchronized (partition) {
                if (!partition.nativeInputSources().equals(inputs)) {
                    throw new IllegalStateException("native recovery state changed during fact capture");
                }
                requireCurrent();
                requireCheckpointCarriers(inputs, descriptor);
                return inputs;
            }
        }

        public void requireCurrent() {
            var actual =
                    partition.nativeSelectedInputSources(inputs, selector, true).orElseThrow();
            if (!descriptor.equals(actual.descriptor) || !ownerRunIds.equals(actual.ownerRunIds)) {
                throw new IllegalStateException("native selected input actor changed during capture");
            }
        }
    }

    /** Captured only from this owner's actually installed cold selected actor, before native readiness. */
    public synchronized NativeRecoveryReadScope nativeRecoveryReadScope(BindingReadSelector selector) {
        requireWritable();
        if (selectedRecoveryOwner == null
                || nativePreferredCache == null
                || !nativePreferredCache.descriptor().descriptorSha256().equals(selectedRecoveryDescriptor)
                || m4ReadOwner == null
                || nativePublicationTask != null) {
            throw new IllegalStateException("native recovery scope lacks its installed cold selected actor");
        }
        var inputs = nativeInputSources();
        requireNativeReadPublication(inputs.sources(), selector);
        var payload = m4ReadOwner
                .selectorRuntime()
                .currentAuthority()
                .get()
                .publicationCell()
                .protocolStateReference();
        if (!(payload instanceof NativePreferredReadSources preferred)
                || preferred.publicationSources().isPresent()
                || !preferred.descriptor().equals(nativePreferredCache.descriptor())) {
            throw new IllegalStateException("native recovery scope differs from its cold source projection");
        }
        return new NativeRecoveryReadScope(
                this, inputs, nativePreferredCache.descriptor(), selector, selectedRecoveryOwner);
    }

    public synchronized void requireNativeRecoveryReadScope(NativeRecoveryReadScope observation) {
        if (observation.partition != this
                || !nativeRecoveryReadScope(observation.selector()).inputs().equals(observation.inputs())) {
            throw new IllegalStateException("native cold read scope changed during evidence persistence");
        }
    }

    /** Data constructors cannot create this qualification of an actual cold read actor. */
    public static final class NativeRecoveryReadScope {
        private final KafkaBookKeeperPartitionV1 partition;
        private final NativeInputSources inputs;
        private final KafkaSealedBookKeeperDescriptorV2 descriptor;
        private final BindingReadSelector selector;
        private final KafkaOwnerAdmissionV1 predecessor;

        private NativeRecoveryReadScope(
                KafkaBookKeeperPartitionV1 partition,
                NativeInputSources inputs,
                KafkaSealedBookKeeperDescriptorV2 descriptor,
                BindingReadSelector selector,
                KafkaOwnerAdmissionV1 predecessor) {
            this.partition = partition;
            this.inputs = inputs;
            this.descriptor = descriptor;
            this.selector = selector;
            this.predecessor = predecessor;
            requireCheckpointCarriers(inputs, descriptor);
        }

        public NativeInputSources inputs() {
            return inputs;
        }

        public KafkaSealedBookKeeperDescriptorV2 descriptor() {
            return descriptor;
        }

        public BindingReadSelector selector() {
            return selector;
        }

        public KafkaOwnerAdmissionV1 predecessor() {
            return predecessor;
        }
    }

    private static void requireCheckpointCarriers(
            NativeInputSources inputs, KafkaSealedBookKeeperDescriptorV2 descriptor) {
        var carriers = new java.util.HashSet<StorageRunId>();
        carriers.add(inputs.sources().activeRoot().runId());
        inputs.sources().sealedRoots().forEach(root -> carriers.add(root.runId()));
        descriptor.sealedParts().forEach(part -> carriers.add(part.handle().runId()));
        var checkpoint = inputs.recoveryCheckpoint().orElseThrow();
        if (!carriers.contains(checkpoint.vector().runBinding().runId())
                || checkpoint.readIndexes().stream().anyMatch(index -> !carriers.contains(index.runId()))) {
            throw new IllegalStateException("checkpoint names a physical carrier outside its actual read scope");
        }
    }

    /** One coherent root set for a low-frequency M4 source-map publication. */
    public synchronized NativeReadSources nativeReadSources() {
        requireWritable();
        if (rolling) {
            throw new IllegalStateException("native source map is changing during BK rollover");
        }
        return nativeReadSourcesUnchecked();
    }

    private NativeReadSources nativeReadSourcesUnchecked() {
        return new NativeReadSources(
                sealedHistoryRoots,
                run.snapshot().root(),
                historyRuns,
                session.capabilitySnapshot().configurationDigest());
    }

    public CompletionStage<KafkaBookKeeperReadResultV1> read(KafkaBookKeeperSequentialReadRequestV1 request) {
        return read(request, ignored -> {});
    }

    public record PlannedRead(
            CompletionStage<KafkaCoherentProtocolSnapshotV1> pinned,
            CompletionStage<KafkaBookKeeperReadResultV1> result) {}

    /** Signals that this Fetch has pinned its actual selector and coherent protocol state, before any Provider read. */
    public PlannedRead planRead(KafkaBookKeeperSequentialReadRequestV1 request) {
        var pinned = new CompletableFuture<KafkaCoherentProtocolSnapshotV1>();
        var result = read(request, snapshot -> pinned.complete(snapshot));
        result.whenComplete((value, failure) -> {
            if (failure != null) {
                pinned.completeExceptionally(failure);
            }
        });
        return new PlannedRead(pinned, result);
    }

    private CompletionStage<KafkaBookKeeperReadResultV1> read(
            KafkaBookKeeperSequentialReadRequestV1 request,
            java.util.function.Consumer<KafkaCoherentProtocolSnapshotV1> onPinned) {
        KafkaBookKeeperM4BrokerReadOwnerV2 guarded;
        synchronized (this) {
            guarded = m4ReadOwner;
        }
        if (guarded != null) {
            if (historicalReads != null) {
                return guarded.readOwned(captured -> {
                    var scope = new HistoricalFetchScope();
                    CompletionStage<KafkaBookKeeperReadResultV1> result;
                    try {
                        result = readNative(request, Optional.of(captured), scope, onPinned);
                    } catch (Throwable failure) {
                        scope.workFinished();
                        result = CompletableFuture.failedFuture(failure);
                    }
                    return new KafkaBookKeeperM4BrokerReadOwnerV2.ReadLifetime<>(result, scope.drained, scope.unknown);
                });
            }
            return guarded.read(captured -> readNative(request, Optional.of(captured), null, onPinned));
        }
        return readNative(
                request, Optional.empty(), historicalReads == null ? null : new HistoricalFetchScope(), onPinned);
    }

    private CompletionStage<KafkaBookKeeperReadResultV1> readNative(
            KafkaBookKeeperSequentialReadRequestV1 request,
            Optional<BindingReadAuthorityV1> capturedView,
            HistoricalFetchScope scope,
            java.util.function.Consumer<KafkaCoherentProtocolSnapshotV1> onPinned) {
        final KafkaCoherentProtocolSnapshotV1 snapshot;
        final List<KafkaBookKeeperReadRunV1> history;
        final KafkaBookKeeperReadViewV2 preferred;
        final KafkaBookKeeperNativeObjectReadSourcesV1 object;
        synchronized (this) {
            requireWritable();
            preferred = capturedView.map(this::capturedPreferred).orElse(null);
            object =
                    capturedView.map(KafkaBookKeeperPartitionV1::capturedObject).orElse(null);
            var expectedRawView = capturedView.map(this::capturedRawView);
            if (expectedRawView.isPresent() && !expectedRawView.orElseThrow().equals(nativeSourceViewSha256)) {
                throw new IllegalStateException("native BK read sources differ from captured M4 selector");
            }
            snapshot = capture();
            history = List.copyOf(historyRuns);
            if (scope != null) {
                scope.roots = List.copyOf(sealedHistoryRoots);
                scope.activeHandle = run.snapshot().handle();
                scope.reads = historicalReads;
            }
            activeReads++;
        }
        if (scope != null) {
            scope.drained.thenRun(this::completeRead);
            scope.unknown.whenComplete((ignored, failure) -> {
                if (failure != null) {
                    readsDrained.completeExceptionally(failure);
                }
            });
        }
        try {
            onPinned.accept(snapshot);
            var active = KafkaBookKeeperReadSnapshotV1.fromActive(snapshot);
            long generation = snapshot.root().references().activeTail().generation();
            var runs = new ArrayList<KafkaBookKeeperReadRunV1>();
            for (var run : history) {
                runs.add(new KafkaBookKeeperReadRunV1(
                        run.runBinding(),
                        run.handle(),
                        run.startOffset(),
                        run.endOffsetExclusive(),
                        generation,
                        run.activeIndex(),
                        run.sealedDirectory()));
            }
            runs.addAll(active.runTable().runs());
            var readSnapshot = new KafkaBookKeeperReadSnapshotV1(
                    snapshot.root(), new KafkaBookKeeperRunTableV1(runs), snapshot.transactionState());
            CompletionStage<KafkaBookKeeperReadResultV1> accepted;
            if (object != null) {
                if (scope == null) {
                    throw new IllegalStateException("native Object Fetch lacks its owned source lifetime");
                }
                accepted = readObjectOrProtectedFallback(readSnapshot, request, object, scope);
            } else if (preferred != null) {
                accepted = scope == null
                        ? reader.readSequential(readSnapshot, request, preferred)
                        : reader.readSequential(readSnapshot, request, preferred, scope::readExact);
            } else {
                accepted = scope == null
                        ? reader.readSequential(readSnapshot, request)
                        : reader.readSequential(readSnapshot, request, scope::readExact);
            }
            CompletionStage<KafkaBookKeeperReadResultV1> operation = accepted.thenApply(result -> {
                synchronized (this) {
                    requireWritable();
                }
                return result;
            });
            operation.whenComplete((result, failure) -> {
                if (scope == null) {
                    completeRead();
                } else {
                    scope.workFinished();
                }
            });
            // A caller cancelling this observer cannot terminate the accepted Provider operation.
            return operation.thenApply(result -> result);
        } catch (RuntimeException | Error failure) {
            if (scope == null) {
                completeRead();
            } else {
                scope.workFinished();
            }
            throw failure;
        }
    }

    /** Check immutable preferred bytes under the actual M4 generation pin without BK fallback. */
    public CompletionStage<Void> verifySelectedObjectPrefix(Sha256Digest manifest) {
        final KafkaBookKeeperM4BrokerReadOwnerV2 readOwner;
        synchronized (this) {
            requireWritable();
            readOwner = Objects.requireNonNull(m4ReadOwner, "native Object verification has no read owner");
        }
        return readOwner.readOwned(authority -> {
            var object = capturedObject(authority);
            if (object == null || !manifest.equals(authority.selectedViewSha256())) {
                throw new IllegalStateException("native Object verification has another selected manifest");
            }
            var lifetime = new HistoricalFetchScope();
            var verification = CompletableFuture.runAsync(
                    () -> {
                        try {
                            object.prefix().verifyComplete();
                        } catch (KafkaObjectMaterializedPrefixV1.UnresolvedRead unresolved) {
                            lifetime.retainObjectResponse(unresolved.originalDrain());
                            throw new CompletionException(unresolved);
                        } catch (java.io.IOException failure) {
                            throw new CompletionException(failure);
                        }
                    },
                    object.providerReads());
            verification.whenComplete((ignored, failure) -> lifetime.workFinished());
            return new KafkaBookKeeperM4BrokerReadOwnerV2.ReadLifetime<>(
                    verification, lifetime.drained, lifetime.unknown);
        });
    }

    private CompletionStage<KafkaBookKeeperReadResultV1> readObjectOrProtectedFallback(
            KafkaBookKeeperReadSnapshotV1 snapshot,
            KafkaBookKeeperSequentialReadRequestV1 request,
            KafkaBookKeeperNativeObjectReadSourcesV1 sources,
            HistoricalFetchScope scope) {
        return reader.readObjectPreferred(
                        snapshot, request, sources.prefix(), sources.providerReads(), scope::readExact)
                .exceptionallyCompose(failure -> {
                    Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                            ? failure.getCause()
                            : failure;
                    if (cause instanceof KafkaObjectMaterializedPrefixV1.UnresolvedRead unresolved) {
                        scope.retainObjectResponse(unresolved.originalDrain());
                    } else if (sources.fallback()
                            && sources.prefix().rawFallbackEquivalent()
                            && cause instanceof KafkaObjectMaterializedPrefixV1.ReadFailure object
                            && object.failureClass() != FailureClass.NOT_ELIGIBLE) {
                        // No response was observable. Restart the entire requested range on its captured protected BK
                        // view.
                        return readProtectedBookKeeperFallback(snapshot, request, scope, object);
                    }
                    return CompletableFuture.failedFuture(cause);
                });
    }

    private CompletionStage<KafkaBookKeeperReadResultV1> readProtectedBookKeeperFallback(
            KafkaBookKeeperReadSnapshotV1 snapshot,
            KafkaBookKeeperSequentialReadRequestV1 request,
            HistoricalFetchScope scope,
            KafkaObjectMaterializedPrefixV1.ReadFailure primary) {
        final CompletionStage<KafkaBookKeeperReadResultV1> fallback;
        try {
            fallback = reader.readSequential(snapshot, request, scope::readExact);
        } catch (RuntimeException failure) {
            primary.addSuppressed(failure);
            return CompletableFuture.failedFuture(primary);
        }
        return fallback.handle((result, failure) -> {
            if (failure != null) {
                primary.addSuppressed(
                        failure instanceof CompletionException && failure.getCause() != null
                                ? failure.getCause()
                                : failure);
                throw new CompletionException(primary);
            }
            return switch (result.outcome()) {
                case FOUND, END_OF_SNAPSHOT -> result;
                default -> {
                    primary.addSuppressed(new java.io.IOException(
                            "captured BK fallback failed: " + result.outcome() + " (" + result.detail() + ")"));
                    throw new CompletionException(primary);
                }
            };
        });
    }

    private KafkaBookKeeperReadViewV2 capturedPreferred(BindingReadAuthorityV1 captured) {
        if (!(captured.publicationCell().protocolStateReference() instanceof NativePreferredReadSources sources)) {
            return null;
        }
        if (nativePreferredCache == null
                || !nativePreferredCache.descriptor().equals(sources.descriptor())
                || !captured.selectedViewSha256().equals(sources.descriptor().descriptorSha256())) {
            throw new IllegalStateException("native preferred read cache differs from its captured M4 source map");
        }
        return nativePreferredCache;
    }

    private Sha256Digest capturedRawView(BindingReadAuthorityV1 authority) {
        var object = capturedObject(authority);
        if (object != null) {
            return object.rawSources().selectedViewSha256();
        }
        if (capturedPreferred(authority) != null) {
            return ((NativePreferredReadSources) authority.publicationCell().protocolStateReference())
                    .rawSources()
                    .selectedViewSha256();
        }
        return authority.selectedViewSha256();
    }

    private static KafkaBookKeeperNativeObjectReadSourcesV1 capturedObject(BindingReadAuthorityV1 authority) {
        if (!(authority.publicationCell().protocolStateReference()
                instanceof KafkaBookKeeperNativeObjectReadSourcesV1 object)) {
            return null;
        }
        if (!authority.selectedViewSha256().equals(object.prefix().manifestSha256())) {
            throw new IllegalStateException("native Object prefix differs from its captured M4 selector");
        }
        return object;
    }

    /** A serial Fetch retains at most one historical SDK handle, including a held/unknown close. */
    private final class HistoricalFetchScope {
        private HistoricalReads reads;
        private final CompletableFuture<Void> drained = new CompletableFuture<>();
        private final CompletableFuture<Void> unknown = new CompletableFuture<>();
        private List<KafkaRunRootSnapshotV1> roots;
        private RunLedgerHandleV1 activeHandle;
        private int handles;
        private boolean finished;

        private CompletionStage<RunLedgerReadResultV1> readExact(RunLedgerHandleV1 handle, long entryId) {
            if (handle.equals(activeHandle)) {
                return session.readExactEntry(handle, entryId);
            }
            var root = roots.stream()
                    .filter(value -> value.runId().equals(handle.runId())
                            && value.ledgerIdentity().equals(handle.ledgerIdentity()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("historical Fetch has an unselected ledger"));
            synchronized (this) {
                if (finished || handles != 0) {
                    throw new IllegalStateException("historical Fetch has an unresolved prior native handle");
                }
                handles++;
            }
            CompletionStage<HistoricalEntryRead> opening;
            try {
                opening = Objects.requireNonNull(reads.read(root, handle, entryId), "historical read stage");
            } catch (Throwable failure) {
                closedHandle();
                return CompletableFuture.failedFuture(failure);
            }
            return opening.handle((read, failure) -> {
                        if (failure != null) {
                            closedHandle();
                            return CompletableFuture.<RunLedgerReadResultV1>failedFuture(failure);
                        }
                        read.closed().whenComplete((ignored, closeFailure) -> {
                            if (closeFailure == null) {
                                closedHandle();
                            } else {
                                unknown.completeExceptionally(closeFailure);
                            }
                        });
                        read.unknown().whenComplete((ignored, closeFailure) -> {
                            if (closeFailure != null) {
                                unknown.completeExceptionally(closeFailure);
                            }
                        });
                        return read.result();
                    })
                    .thenCompose(read -> read);
        }

        private synchronized void closedHandle() {
            handles--;
            completeDrain();
        }

        private void retainObjectResponse(CompletionStage<Void> originalDrain) {
            synchronized (this) {
                if (finished || handles != 0) {
                    throw new IllegalStateException("Object Fetch cannot release its unresolved original response");
                }
                handles++;
            }
            originalDrain.whenComplete((ignored, failure) -> {
                if (failure == null) {
                    closedHandle();
                } else {
                    unknown.completeExceptionally(failure);
                }
            });
        }

        private synchronized void workFinished() {
            finished = true;
            completeDrain();
        }

        private void completeDrain() {
            if (finished && handles == 0) {
                drained.complete(null);
            }
        }
    }

    public void fence() {
        pipeline.fence();
        boolean drained;
        synchronized (this) {
            writable = false;
            if (m4ReadOwner != null) {
                m4ReadOwner.closeAdmission();
            }
            drained = activeReads == 0;
        }
        if (drained) {
            readsDrained.complete(null);
        }
    }

    public CompletionStage<Void> resign() {
        fence();
        CompletableFuture<Void> attempt;
        boolean start = false;
        synchronized (this) {
            if (resignation == null || resignation.isCompletedExceptionally()) {
                resignation = new CompletableFuture<>();
                start = true;
            }
            attempt = resignation;
        }
        if (start) {
            CompletionStage<Void> m4Drained = drainNativeReadActors();
            readsDrained
                    .thenCombine(m4Drained, (ignored, alsoIgnored) -> null)
                    .thenCompose(ignored -> ownerAuthority.closeOwner(owner))
                    .thenCompose(result -> {
                        exact(result);
                        // A fenced rollover may have sealed the old run before its successor was installed.
                        return run.snapshot().state() == KafkaBookKeeperRunStateV1.SEALED
                                ? CompletableFuture.completedFuture(null)
                                : run.drain();
                    })
                    .whenComplete((ignored, failure) -> {
                        if (failure == null) {
                            synchronized (this) {
                                nativePreferredCache = null;
                            }
                            attempt.complete(null);
                        } else {
                            attempt.completeExceptionally(failure);
                        }
                    });
        }
        return attempt.thenApply(ignored -> null);
    }

    private synchronized CompletionStage<Void> drainNativeReadActors() {
        CompletionStage<Void> current =
                m4ReadOwner == null ? CompletableFuture.completedFuture(null) : m4ReadOwner.drain();
        if (nativeObjectRetirement == null || nativeObjectRetirement.predecessorOwner() == m4ReadOwner) {
            return current;
        }
        return current.thenCombine(nativeObjectRetirement.predecessorOwner().drain(), (ignored, old) -> null);
    }

    private void completeRead() {
        boolean drained;
        synchronized (this) {
            activeReads--;
            drained = !writable && activeReads == 0;
        }
        if (drained) {
            readsDrained.complete(null);
        }
    }

    public KafkaOwnerIdentityV1 owner() {
        return owner;
    }

    private void requireWritable() {
        if (!writable || pipeline.fenced()) {
            throw new IllegalStateException("native BK partition owner is fenced");
        }
    }

    private static void requireCurrent(BooleanSupplier current) {
        if (!current.getAsBoolean()) {
            throw new IllegalStateException("native Controller assignment was superseded during BK recovery");
        }
    }

    private static <T> T exact(ProviderMutationResultV1<T> result) {
        return result.exactProof()
                .orElseThrow(
                        () -> new IllegalStateException("BK authority mutation is unresolved: " + result.outcome()));
    }

    private static Id128 randomId() {
        var id = UUID.randomUUID();
        return new Id128(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    public final class AdmittedAppend implements AutoCloseable {
        private final KafkaBookKeeperPartitionV1 owner = KafkaBookKeeperPartitionV1.this;
        private KafkaAppendAdmissionRequestV1 request;
        private List<KafkaBookKeeperDataAdmissionTicketV1> tickets;
        private final KafkaBookKeeperOrderedPipelineV1.AdmissionLease lease;
        private boolean handedOff;
        private boolean closed;

        private AdmittedAppend(
                KafkaAppendAdmissionRequestV1 request,
                List<KafkaBookKeeperDataAdmissionTicketV1> tickets,
                KafkaBookKeeperOrderedPipelineV1.AdmissionLease lease) {
            this.request = request;
            this.tickets = List.copyOf(tickets);
            this.lease = lease;
        }

        private void handoff(KafkaBookKeeperPartitionV1 submitting, List<KafkaNativeAssignedRecordBatchV1> batches) {
            if (owner != submitting || handedOff || closed) {
                throw new IllegalArgumentException("native append admission is stale or belongs to another partition");
            }
            var actualTickets = new ArrayList<KafkaBookKeeperDataAdmissionTicketV1>();
            long actualBytes = 0;
            for (int index = 0; index < batches.size(); index++) {
                var ticket = dataAdmission.admitBeforeOffsetAllocation(
                        batches.get(index).rawAssignedRecordBatch().length(), index, batches.size());
                actualTickets.add(ticket);
                actualBytes = Math.addExact(actualBytes, ticket.encodedDataFrameBytes());
            }
            if (batches.isEmpty()
                    || batches.size() > request.memberCount()
                    || actualBytes > request.encodedDataBytes()) {
                throw new IllegalArgumentException("native converted batches exceed pre-offset capacity");
            }
            admittedEntries -= request.memberCount() - batches.size();
            admittedBytes -= request.encodedDataBytes() - actualBytes;
            runEntries -= request.memberCount() - batches.size();
            runBytes -= request.encodedDataBytes() - actualBytes;
            request = new KafkaAppendAdmissionRequestV1(batches.size(), actualBytes);
            tickets = List.copyOf(actualTickets);
            handedOff = true;
        }

        @Override
        public void close() {
            synchronized (owner) {
                if (closed) {
                    return;
                }
                closed = true;
                if (!handedOff) {
                    admittedEntries -= request.memberCount();
                    admittedBytes -= request.encodedDataBytes();
                    runEntries -= request.memberCount();
                    runBytes -= request.encodedDataBytes();
                    admissions--;
                }
            }
            lease.close();
        }
    }
}
