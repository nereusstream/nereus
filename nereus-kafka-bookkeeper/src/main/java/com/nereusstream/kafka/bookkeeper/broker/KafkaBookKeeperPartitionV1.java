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
import com.nereusstream.domain.identity.Id128;
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
import com.nereusstream.kafka.bookkeeper.read.KafkaPackedIndexDirectoryV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRecoveryOutcomeV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootAuthority;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** One native Broker's recovered BK owner, composing the existing run, commit, pipeline and read paths. */
public final class KafkaBookKeeperPartitionV1 {
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
    private long runEntries;
    private long runBytes;
    private long checkpointBaseBytes;
    private final KafkaBookKeeperTargetedReaderV1 reader;
    private final List<KafkaBookKeeperReadRunV1> historyRuns;
    private final KafkaBookKeeperRecoveryEnvelopeV1 envelope;
    private long admittedEntries;
    private long admittedBytes;
    private boolean writable = true;

    private KafkaBookKeeperPartitionV1(
            BookKeeperCellSession session,
            KafkaBookKeeperOwnerAuthorityV1 ownerAuthority,
            KafkaOwnerIdentityV1 owner,
            KafkaBookKeeperRunLifecycleV1 run,
            KafkaCoherentCommitCoordinatorV1 commit,
            Optional<KafkaBookKeeperClosedHistoryRecoveryV1.Result> history,
            KafkaAppendCapacityControllerV1 partitionCapacity,
            KafkaAppendCapacityControllerV1 globalCapacity,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            int maximumBatchBytes) {
        this.ownerAuthority = ownerAuthority;
        this.owner = owner;
        this.run = run;
        this.commit = commit;
        this.envelope = envelope;
        this.session = session;
        this.partitionCapacity = partitionCapacity;
        this.globalCapacity = globalCapacity;
        this.maximumBatchBytes = maximumBatchBytes;
        pipeline = new KafkaBookKeeperOrderedPipelineV1(session, run, partitionCapacity, globalCapacity, commit);
        dataAdmission = KafkaBookKeeperDataAdmissionV1.admitProfile(
                run.snapshot().runBinding(), session.capabilitySnapshot(), maximumBatchBytes);
        reader = new KafkaBookKeeperTargetedReaderV1(session, 32);
        historyRuns =
                new ArrayList<>(history.map(value -> value.readTable(0).runs()).orElse(List.of()));
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
            BooleanSupplier isCurrent) {
        Objects.requireNonNull(isCurrent, "isCurrent");
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
        CompletionStage<Optional<KafkaBookKeeperClosedHistoryRecoveryV1.Result>> recovery = closed.isEmpty()
                ? CompletableFuture.completedFuture(Optional.empty())
                : new KafkaBookKeeperClosedHistoryRecoveryV1(
                                session, owners, new KafkaNativeProtocolBatchAdapterV1(), System::nanoTime)
                        .recover(closed.orElseThrow(), fence, 0, envelope)
                        .thenApply(Optional::of);
        return recovery.thenCompose(
                history -> sealRecoveredTail(owners, history, isCurrent).thenCompose(predecessor -> {
                    requireCurrent(isCurrent);
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
                                            partitionCapacity,
                                            globalCapacity,
                                            envelope,
                                            maximumBatchBytes);
                                }));
                    });
                }));
    }

    private static CompletionStage<Optional<KafkaRunRootSnapshotV1>> sealRecoveredTail(
            KafkaBookKeeperOwnerAuthorityV1 owners,
            Optional<KafkaBookKeeperClosedHistoryRecoveryV1.Result> history,
            BooleanSupplier isCurrent) {
        CompletionStage<Optional<KafkaRunRootSnapshotV1>> tail = CompletableFuture.completedFuture(
                history.flatMap(KafkaBookKeeperClosedHistoryRecoveryV1.Result::lastRoot)
                        .filter(root -> root.state() == KafkaRunRootStateV1.SEALED));
        if (history.isPresent()) {
            for (var recovered : history.orElseThrow().runs()) {
                tail = tail.thenCompose(ignored -> {
                    requireCurrent(isCurrent);
                    if (recovered.root().root().state() == KafkaRunRootStateV1.SEALED) {
                        return CompletableFuture.completedFuture(
                                Optional.of(recovered.root().root()));
                    }
                    var residue = recovered.recovery().outcome()
                                    == KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_WITH_INERT_RESIDUE
                            ? recovered.recovery().conflictEntryId()
                            : OptionalLong.empty();
                    return owners.sealRecoveredRun(
                                    recovered.root().root(),
                                    recovered.closedOwner(),
                                    recovered.fenceProof(),
                                    recovered.recovery().newLeaderLeo().orElseThrow(),
                                    residue)
                            .thenApply(result -> Optional.of(exact(result)));
                });
            }
        }
        return tail;
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
        long controlBytes = Nbke2CodecV1.encode(
                                old.snapshot().handle().ledgerIdentity().ledgerId(),
                                checkpointEntry,
                                checkpoint.toNbke2())
                        .length
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
                .thenAccept(successor -> {
                    synchronized (this) {
                        requireWritable();
                        if (run != old || admissions != 0) {
                            throw new IllegalStateException("run switch became stale");
                        }
                        var digest = Sha256Digest.hash(
                                com.nereusstream.domain.bytes.CanonicalBytes.copyOf(Nbke2CodecV1.encode(
                                        old.snapshot().handle().ledgerIdentity().ledgerId(), footerEntry, footer)));
                        if (block != null) {
                            historyRuns.add(new KafkaBookKeeperReadRunV1(
                                    binding,
                                    old.snapshot().handle(),
                                    anchor,
                                    end,
                                    0,
                                    Optional.empty(),
                                    Optional.of(new KafkaPackedIndexDirectoryV1(directory))));
                        }
                        commit.switchBookKeeperRun(
                                old.snapshot().handle(), successor.snapshot().handle(), checkpoint, digest);
                        run = successor;
                        pipeline = new KafkaBookKeeperOrderedPipelineV1(
                                session, run, partitionCapacity, globalCapacity, commit);
                        dataAdmission = KafkaBookKeeperDataAdmissionV1.admitProfile(
                                run.snapshot().runBinding(), session.capabilitySnapshot(), maximumBatchBytes);
                        checkpointBaseBytes = Nbke2CodecV1.encode(
                                        old.snapshot().handle().ledgerIdentity().ledgerId(),
                                        checkpointEntry,
                                        checkpoint.toNbke2())
                                .length;
                        admittedEntries = runEntries = 1;
                        admittedBytes = runBytes = Nbke2CodecV1.encode(
                                        run.snapshot().handle().ledgerIdentity().ledgerId(),
                                        0,
                                        new Nbke2RunHeaderV1(
                                                run.snapshot().runBinding(),
                                                end,
                                                1,
                                                session.capabilitySnapshot().configurationDigest()))
                                .length;
                    }
                });
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
            var adapter = new KafkaNativeProtocolBatchAdapterV1();
            var plan = new KafkaProtocolAppendPlanV1(
                    snapshot.root().fence(),
                    batches.stream().map(adapter::protocolDelta).toList());
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

    public CompletionStage<KafkaBookKeeperReadResultV1> read(KafkaBookKeeperSequentialReadRequestV1 request) {
        final KafkaCoherentProtocolSnapshotV1 snapshot;
        final List<KafkaBookKeeperReadRunV1> history;
        synchronized (this) {
            requireWritable();
            snapshot = capture();
            history = List.copyOf(historyRuns);
        }
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
        return reader.readSequential(readSnapshot, request).thenApply(result -> {
            synchronized (this) {
                requireWritable();
            }
            return result;
        });
    }

    public void fence() {
        synchronized (this) {
            writable = false;
        }
        pipeline.fence();
    }

    public CompletionStage<Void> resign() {
        fence();
        return ownerAuthority.closeOwner(owner).thenCompose(result -> {
            exact(result);
            return run.drain();
        });
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
