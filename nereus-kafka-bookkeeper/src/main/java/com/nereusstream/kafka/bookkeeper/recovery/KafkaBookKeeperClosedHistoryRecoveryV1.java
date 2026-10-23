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

package com.nereusstream.kafka.bookkeeper.recovery;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryProgressV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaActiveTailStateV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionPublicationV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadViewV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorCodecV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadRunV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperRunTableV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaPackedBatchLocatorIndexV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaPackedIndexDirectoryV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.LongSupplier;

/** Discovers immutable closed lineage, fences all its runs, then replays one continuous Kafka history. */
public final class KafkaBookKeeperClosedHistoryRecoveryV1 {
    public static final int MAX_CLOSED_OWNERS = 1024;
    public static final int MAX_HISTORY_RUNS = 1024;

    /** Fencing may retain an original and replacement handle; subsequent reads need one closed handle. */
    public enum ReadPhase {
        FENCE(2),
        FOOTER(1),
        CHECKPOINT(1),
        REPLAY(1),
        FETCH(1),
        M5_INPUT(1);

        private final int maximumHandles;

        ReadPhase(int maximumHandles) {
            this.maximumHandles = maximumHandles;
        }

        public int maximumHandles() {
            return maximumHandles;
        }
    }

    /** Owns one run's native work through actual close and exact invocation cleanup. */
    public interface ReadOwner {
        <T> CompletionStage<T> read(
                KafkaRunRootRecordV2 record,
                ReadPhase phase,
                Function<BookKeeperCellSession, CompletionStage<T>> operation);
    }

    /** An independently read output and its exact immutable generation provenance; grants no native admission. */
    public static final class SelectedPrefix {
        private final KafkaBookKeeperReadViewV2 view;
        private final Optional<KafkaSealedBookKeeperDescriptorV2> predecessorDescriptor;

        private SelectedPrefix(
                KafkaBookKeeperReadViewV2 view, Optional<KafkaSealedBookKeeperDescriptorV2> predecessorDescriptor) {
            this.view = Objects.requireNonNull(view, "view");
            this.predecessorDescriptor = predecessorDescriptor;
        }

        public KafkaBookKeeperReadViewV2 view() {
            return view;
        }

        public Optional<KafkaSealedBookKeeperDescriptorV2> predecessorDescriptor() {
            return predecessorDescriptor;
        }

        public static SelectedPrefix verify(KafkaBookKeeperReadViewV2 view, CanonicalControlMetadataStore metadata) {
            var descriptor = view.descriptor();
            var sources = descriptor.sourceCut().sources();
            var first = sources.get(0);
            if (first.kind() == SourceKind.BOOKKEEPER_LEDGER) {
                return raw(view);
            }
            require(
                    first.kind() == SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2,
                    "selected recovery has an unsupported prefix source");
            var key = KafkaBookKeeperCompactionPublicationV2.descriptorKey(first.formatRootSha256());
            var bytes = metadata.get(key)
                    .orElseThrow(() ->
                            new IllegalStateException("selected recovery lacks its immutable predecessor descriptor"));
            var previous = KafkaSealedBookKeeperDescriptorCodecV2.decode(bytes);
            require(bytes.equals(previous.encode()), "selected recovery predecessor descriptor is noncanonical");
            requirePredecessor(descriptor, Optional.of(previous));
            return new SelectedPrefix(view, Optional.of(previous));
        }

        public static void requirePredecessor(
                KafkaSealedBookKeeperDescriptorV2 descriptor, Optional<KafkaSealedBookKeeperDescriptorV2> predecessor) {
            var sources = descriptor.sourceCut().sources();
            if (predecessor.isEmpty()) {
                require(
                        sources.stream().allMatch(source -> source.kind() == SourceKind.BOOKKEEPER_LEDGER),
                        "selected recovery generation lacks immutable predecessor qualification");
                return;
            }
            var first = sources.get(0);
            var previous = predecessor.orElseThrow();
            var bytes = previous.encode();
            var key = KafkaBookKeeperCompactionPublicationV2.descriptorKey(previous.descriptorSha256());
            require(
                    first.kind() == SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2
                            && sources.subList(1, sources.size()).stream()
                                    .allMatch(source -> source.kind() == SourceKind.BOOKKEEPER_LEDGER),
                    "selected recovery requires one complete generation followed by raw sources");
            var identity = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(36)
                    .putInt(0x4d355347)
                    .put(previous.descriptorSha256().bytes().toByteArray())
                    .array()));
            require(
                    first.sourceIdentitySha256().equals(identity)
                            && first.formatRootSha256().equals(previous.descriptorSha256())
                            && descriptor.sourceCut().predecessorViewSha256().equals(previous.descriptorSha256())
                            && first.physicalKey().equals(key)
                            && first.canonicalLength() == bytes.length()
                            && first.bodySha256().equals(Sha256Digest.hash(bytes))
                            && first.coverage().equals(previous.sourceCut().coverage())
                            && first.coverage().inclusiveStart()
                                    == descriptor.sourceCut().coverage().inclusiveStart()
                            && previous.task()
                                    .namespace()
                                    .equals(descriptor.task().namespace())
                            && previous.task()
                                    .capability()
                                    .equals(descriptor.task().capability())
                            && previous.sourceCut()
                                    .identity()
                                    .binding()
                                    .equals(descriptor.sourceCut().identity().binding()),
                    "selected recovery generation differs from its immutable predecessor");
        }

        public static SelectedPrefix raw(KafkaBookKeeperReadViewV2 view) {
            requirePredecessor(view.descriptor(), Optional.empty());
            return new SelectedPrefix(view, Optional.empty());
        }
    }

    private final BookKeeperCellSession session;
    private final KafkaBookKeeperOwnerAuthorityV1 authority;
    private final KafkaRecoveryBatchProtocolAdapterV1 adapter;
    private final ReadOwner readOwner;
    private final LongSupplier nanoTime;
    private final KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence;

    public KafkaBookKeeperClosedHistoryRecoveryV1(
            BookKeeperCellSession session,
            KafkaBookKeeperOwnerAuthorityV1 authority,
            KafkaRecoveryBatchProtocolAdapterV1 adapter,
            LongSupplier nanoTime,
            KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence) {
        this(session, authority, adapter, nanoTime, unselectedFence, borrowedReads(session));
    }

    public KafkaBookKeeperClosedHistoryRecoveryV1(
            BookKeeperCellSession session,
            KafkaBookKeeperOwnerAuthorityV1 authority,
            KafkaRecoveryBatchProtocolAdapterV1 adapter,
            LongSupplier nanoTime,
            KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence,
            ReadOwner readOwner) {
        this.session = Objects.requireNonNull(session, "session");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        this.readOwner = Objects.requireNonNull(readOwner, "readOwner");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.unselectedFence = Objects.requireNonNull(unselectedFence, "unselectedFence");
    }

    /** Compatibility entry for callers that own the supplied session for their complete recovery. */
    public static ReadOwner borrowedReads(BookKeeperCellSession session) {
        Objects.requireNonNull(session, "session");
        return new ReadOwner() {
            @Override
            public <T> CompletionStage<T> read(
                    KafkaRunRootRecordV2 record,
                    ReadPhase phase,
                    Function<BookKeeperCellSession, CompletionStage<T>> operation) {
                return operation.apply(session);
            }
        };
    }

    /** The caller must recheck the exact native assignment before opening a new owner or publishing ready. */
    public CompletionStage<Result> recover(
            KafkaOwnerAdmissionV1 closed,
            KafkaPartitionFenceV1 newFence,
            long historyStartOffset,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope) {
        return recover(closed, newFence, historyStartOffset, envelope, Optional.empty(), List.of());
    }

    /**
     * Replays only the raw suffix of an independently verified KBSD3 prefix. The caller must qualify the exact
     * durable selection and own its output reads/cache charge; this method grants no M4 selection or GC authority.
     * The complete closed Owner and admitted-run metadata lineage is still required, including retired roots.
     */
    public CompletionStage<Result> recoverSelected(
            KafkaOwnerAdmissionV1 closed,
            KafkaPartitionFenceV1 newFence,
            long historyStartOffset,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            KafkaBookKeeperReadViewV2 selectedView) {
        return recoverSelected(closed, newFence, historyStartOffset, envelope, SelectedPrefix.raw(selectedView));
    }

    public CompletionStage<Result> recoverSelected(
            KafkaOwnerAdmissionV1 closed,
            KafkaPartitionFenceV1 newFence,
            long historyStartOffset,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            SelectedPrefix prefix) {
        var selectedView = Objects.requireNonNull(prefix, "prefix").view();
        var descriptor = selectedView.descriptor();
        var state = selectedView
                .recoveryState()
                .orElseThrow(() -> new IllegalArgumentException("selected recovery requires independent KBSD3 state"));
        var source = state.sourceFence();
        if (descriptor.sourceCut().coverage().inclusiveStart() != historyStartOffset
                || state.coveredThrough() <= historyStartOffset
                || !source.bindingId().equals(newFence.bindingId())
                || !source.topicIncarnation().equals(newFence.topicIncarnation())
                || source.partitionId() != newFence.partitionId()
                || !source.storageEpochId().equals(newFence.storageEpochId())
                || source.ownerEpoch() > closed.owner().ownerEpoch()
                || source.kafkaLeaderEpoch() > closed.owner().kafkaLeaderEpoch()
                || !descriptor.task().capability().equals(session.capabilitySnapshot())) {
            throw new IllegalArgumentException("selected recovery changes the closed prefix scope or bounds");
        }
        var output = descriptor.sealedParts().get(0).handle();
        var checkpoint = state.checkpointFor(new Nbke2RunBindingV1(
                source.bindingId(),
                source.topicIncarnation(),
                source.partitionId(),
                source.storageEpochId(),
                source.ownerEpoch(),
                source.kafkaLeaderEpoch(),
                output.providerScopeId(),
                output.runId()));
        return recover(
                closed,
                newFence,
                historyStartOffset,
                envelope,
                Optional.of(checkpoint),
                descriptor.sourceCut().sources());
    }

    private CompletionStage<Result> recover(
            KafkaOwnerAdmissionV1 closed,
            KafkaPartitionFenceV1 newFence,
            long historyStartOffset,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope,
            Optional<KafkaProtocolCheckpointStateV1> selectedState,
            List<SourceExtent> selectedSources) {
        Objects.requireNonNull(closed, "closed");
        Objects.requireNonNull(newFence, "newFence");
        Objects.requireNonNull(envelope, "envelope");
        var scope = new KafkaRunRootRecordV2.Scope(
                newFence.bindingId(),
                newFence.topicIncarnation(),
                newFence.partitionId(),
                newFence.storageEpochId(),
                session.providerScopeId());
        if (!closed.closed()
                || historyStartOffset < 0
                || !closed.scopeSha256().equals(Sha256Digest.hash(scope.encode()))
                || newFence.ownerEpoch() <= closed.owner().ownerEpoch()
                || newFence.kafkaLeaderEpoch() <= closed.owner().kafkaLeaderEpoch()) {
            throw new IllegalArgumentException("recovery requires exact closed history and a newer native fence");
        }
        Context context = new Context(
                newFence, historyStartOffset, envelope, nanoTime.getAsLong(), selectedState, selectedSources);
        return authority
                .readClosedOwner(closed.owner().ownerEpoch())
                .thenCompose(stored -> {
                    require(stored.equals(Optional.of(closed)), "closed owner archive differs from the selected cut");
                    return discoverOwners(context, closed);
                })
                .thenCompose(ignored -> {
                    Collections.reverse(context.owners);
                    return inspectBirths(context, 0);
                })
                .thenCompose(ignored -> discoverRuns(context, 0, 0))
                .thenRun(() -> require(
                        selectedState.isEmpty()
                                || context.selectedSources.isEmpty()
                                        && context.generationBoundaryChecked
                                        && context.selectedPrefixAnchor
                                                .filter(root -> root.kafkaEndOffsetExclusive()
                                                                .orElseThrow()
                                                        == context.end)
                                                .isPresent(),
                        "selected recovery cut lacks its exact sealed raw prefix boundary"))
                .thenCompose(ignored -> fenceAll(context, 0))
                .thenCompose(ignored -> discoverFooters(context, 0))
                // The selected generation supplies the compacted prefix. A later native checkpoint may
                // cover the raw suffix; replaying that entire suffix would discard its bounded recovery cut.
                .thenCompose(ignored -> selectCheckpoint(context, context.runs.size() - 1))
                .thenCompose(ignored -> replay(
                        context,
                        context.checkpointRun >= 0
                                        && context.runs
                                                        .get(context.checkpointRun)
                                                        .record
                                                        .root()
                                                        .state()
                                                == KafkaRunRootStateV1.ACTIVE
                                ? context.checkpointRun
                                : context.checkpointRun + 1))
                .thenApply(ignored -> new Result(
                        context.owners,
                        context.recoveredRuns,
                        context.runs.stream().map(run -> run.record.root()).toList(),
                        context.end,
                        context.state,
                        context.progress(),
                        context.readRuns,
                        selectedState.isPresent() || context.checkpointRun >= 0
                                ? OptionalLong.of(context.checkpointEnd)
                                : OptionalLong.empty(),
                        context.catalogTail.map(KafkaRunRootRecordV2::root),
                        context.selectedPrefixAnchor));
    }

    private CompletionStage<Void> discoverOwners(Context context, KafkaOwnerAdmissionV1 closed) {
        context.checkTime();
        require(context.owners.size() < MAX_CLOSED_OWNERS, "closed owner discovery exceeds its bound");
        require(
                closed.closed() && context.ownerEpochs.add(closed.owner().ownerEpoch()),
                "closed owner lineage repeats or contains an open owner");
        context.owners.add(closed);
        if (closed.previous().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        var previous = closed.previous().orElseThrow();
        return authority.readClosedOwner(previous.ownerEpoch()).thenCompose(found -> {
            var prior = found.orElseThrow(() -> new IllegalStateException("closed owner predecessor is absent"));
            require(
                    prior.closed()
                            && prior.owner().ownerEpoch() == previous.ownerEpoch()
                            && closed.owner().succeeds(prior.owner())
                            && prior.scopeSha256().equals(closed.scopeSha256())
                            && Sha256Digest.hash(prior.encode()).equals(previous.closureSha256())
                            && prior.tail().equals(previous.tail()),
                    "closed owner predecessor identity or digest differs");
            return discoverOwners(context, prior);
        });
    }

    private CompletionStage<Void> inspectBirths(Context context, int ownerIndex) {
        context.checkTime();
        if (ownerIndex == context.owners.size()) {
            return CompletableFuture.completedFuture(null);
        }
        return new KafkaBookKeeperRunBirthRecoveryV1(session, authority, unselectedFence)
                .observe(context.owners.get(ownerIndex))
                .thenCompose(observations -> {
                    for (var observation : observations) {
                        if (observation.kind() == KafkaBookKeeperRunBirthRecoveryV1.Kind.SELECTED
                                || observation.kind() == KafkaBookKeeperRunBirthRecoveryV1.Kind.RETIRED_SELECTED) {
                            require(
                                    context.selectedBirths.putIfAbsent(
                                                    observation.handle().runId(), observation.record())
                                            == null,
                                    "closed owners repeat a selected run birth identity");
                        }
                        if (observation.kind()
                                == KafkaBookKeeperRunBirthRecoveryV1.Kind.PHYSICALLY_CREATED_UNSELECTED) {
                            require(
                                    context.runIds.add(observation.handle().runId())
                                            && context.ledgerIds.add(observation
                                                    .handle()
                                                    .ledgerIdentity()
                                                    .ledgerId()),
                                    "closed owners reuse an unselected physical run identity");
                        }
                    }
                    return inspectBirths(context, ownerIndex + 1);
                });
    }

    private CompletionStage<Void> discoverRuns(Context context, int ownerIndex, int runIndex) {
        // Covered metadata may complete synchronously for every root; iterate the bounded prefix without growing
        // the callback stack. The raw suffix keeps its actual admission read before any native I/O.
        while (ownerIndex < context.owners.size()) {
            context.checkTime();
            var owner = context.owners.get(ownerIndex);
            if (runIndex == owner.runs().size()) {
                ownerIndex++;
                runIndex = 0;
                continue;
            }
            require(context.discoveredRuns++ < MAX_HISTORY_RUNS, "closed run discovery exceeds its bound");
            var link = owner.runs().get(runIndex);
            var birth = context.selectedBirths.get(link.runId());
            require(birth != null && birth.initialLink().equals(link), "admitted run lacks its closed birth identity");
            boolean covered = context.selectedEnd.isPresent()
                    && birth.root().state() == KafkaRunRootStateV1.SEALED
                    && birth.root().kafkaEndOffsetExclusive().orElseThrow() <= context.selectedEnd.getAsLong();
            if (covered) {
                admitDiscoveredRun(context, owner, link, birth, true);
                runIndex++;
                continue;
            }
            int nextOwner = ownerIndex;
            int nextRun = runIndex + 1;
            return authority.readAdmittedRun(link).thenCompose(found -> {
                var record = found.orElseThrow(() -> new IllegalStateException("legally admitted run is absent"));
                admitDiscoveredRun(context, owner, link, record, false);
                return discoverRuns(context, nextOwner, nextRun);
            });
        }
        return CompletableFuture.completedFuture(null);
    }

    private void admitDiscoveredRun(
            Context context,
            KafkaOwnerAdmissionV1 owner,
            KafkaRunRootRecordV2.Link link,
            KafkaRunRootRecordV2 record,
            boolean covered) {
        var root = record.root();
        Optional<com.nereusstream.storage.api.bookkeeper.StorageRunId> predecessor =
                context.catalogTail.map(parent -> parent.root().runId());
        // Owner membership may select an ACTIVE pending root after an attachment response was lost. A covered
        // SEALED root cannot be pending; its record representation already requires the admitted flag.
        require(
                record.initialLink().equals(link)
                        && (covered || !record.retired())
                        && owner.scopeSha256()
                                .equals(Sha256Digest.hash(
                                        KafkaRunRootRecordV2.Scope.of(root).encode()))
                        && owner.owner().ownerEpoch() == root.creatorOwnerEpoch()
                        && owner.owner().kafkaLeaderEpoch() == root.kafkaLeaderEpoch()
                        && root.predecessorRunId().equals(predecessor)
                        && context.runIds.add(root.runId())
                        && context.ledgerIds.add(root.ledgerIdentity().ledgerId()),
                "admitted run chain identity differs or repeats");
        if (context.catalogTail.isPresent()) {
            var parent = context.catalogTail.orElseThrow();
            require(
                    parent.root().state() == KafkaRunRootStateV1.SEALED
                            && parent.root().kafkaEndOffsetExclusive().orElseThrow() == root.kafkaStartOffset()
                            && parent.successor()
                                    .filter(value -> !value.equals(link))
                                    .isEmpty(),
                    "run predecessor lacks the exact sealed end or selects another child");
        } else {
            require(root.kafkaStartOffset() == context.historyStart, "history begins after an unverified prefix");
        }
        context.catalogTail = Optional.of(record);
        if (covered) {
            if (context.selectedGeneration.isPresent()
                    && root.kafkaStartOffset()
                            < context.selectedGeneration
                                    .orElseThrow()
                                    .coverage()
                                    .exclusiveEnd()) {
                long boundary =
                        context.selectedGeneration.orElseThrow().coverage().exclusiveEnd();
                require(
                        root.kafkaEndOffsetExclusive().orElseThrow() <= boundary,
                        "selected recovery generation crosses a closed raw run boundary");
                if (root.kafkaEndOffsetExclusive().orElseThrow() == boundary) {
                    context.generationBoundaryChecked = true;
                }
            } else if (root.kafkaStartOffset() != root.kafkaEndOffsetExclusive().orElseThrow()) {
                var extent = context.selectedSources.remove(record.resource().sha256());
                require(
                        extent != null
                                && extent.coverage().inclusiveStart() == root.kafkaStartOffset()
                                && extent.coverage().exclusiveEnd()
                                        == root.kafkaEndOffsetExclusive().orElseThrow(),
                        "selected recovery source differs from its closed raw prefix member");
            }
            context.selectedPrefixAnchor = Optional.of(root);
            return;
        }
        require(
                context.selectedEnd.isEmpty() || root.kafkaStartOffset() >= context.selectedEnd.getAsLong(),
                "selected recovery cut crosses a raw run boundary");
        var binding = new Nbke2RunBindingV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId());
        var handle = new RunLedgerHandleV1(
                root.providerScopeId(),
                root.runId(),
                root.ledgerIdentity(),
                session.capabilitySnapshot().configurationDigest());
        context.runs.add(new Run(owner, record, binding, handle));
    }

    private CompletionStage<Void> fenceAll(Context context, int index) {
        context.checkTime();
        if (index == context.runs.size()) {
            return CompletableFuture.completedFuture(null);
        }
        var run = context.runs.get(index);
        return readOwner
                .read(run.record, ReadPhase.FENCE, owned -> openSelectedRun(owned, run)
                        .thenCompose(ignored -> owned.fenceAndRecoverRunLedger(run.handle)))
                .thenCompose(fenced -> {
                    require(
                            fenced.outcome() == ProviderMutationOutcomeV1.APPLIED_EXACT,
                            "not every admitted ledger has a definitive fenced recovery");
                    var proof = fenced.exactProof().orElseThrow();
                    require(proof.handle().equals(run.handle), "fenced recovery belongs to another run");
                    run.record
                            .recoveryCut()
                            .ifPresent(cut -> require(
                                    cut.closedOwnerSha256().equals(Sha256Digest.hash(run.owner.encode()))
                                            && cut.recoveredLastAddConfirmed() == proof.lastAddConfirmed(),
                                    "persisted recovery cut differs from its closed Owner or native fenced ledger"));
                    context.proofs.add(proof);
                    return fenceAll(context, index + 1);
                });
    }

    private CompletionStage<Void> discoverFooters(Context context, int index) {
        context.checkTime();
        if (index == context.runs.size()) {
            return CompletableFuture.completedFuture(null);
        }
        var run = context.runs.get(index);
        long entryId = run.record
                .recoveryCut()
                .map(cut -> cut.inertFromEntryId().isPresent()
                        ? cut.inertFromEntryId().getAsLong() - 1
                        : cut.recoveredLastAddConfirmed())
                .orElse(context.proofs.get(index).lastAddConfirmed());
        return readOwner
                .read(run.record, ReadPhase.FOOTER, owned -> openSelectedRun(owned, run)
                        .thenCompose(ignored -> exactFrame(owned, context, run, entryId)))
                .thenCompose(frame -> {
                    if (frame instanceof Nbke2ProtocolCheckpointV1 checkpoint
                            && checkpoint.runBinding().equals(run.binding)) {
                        context.checkpointEntries.put(index, entryId);
                        return discoverFooters(context, index + 1);
                    }
                    if (!(frame instanceof Nbke2RunFooterV1)) {
                        require(
                                run.record.root().state() == KafkaRunRootStateV1.ACTIVE
                                        || run.record.recoveryCut().isPresent(),
                                "sealed normal run lacks its native footer");
                        if (frame instanceof com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1 data) {
                            context.dataProbeBytes.put(index, (long) Nbke2CodecV1.encode(
                                            run.handle.ledgerIdentity().ledgerId(), entryId, data)
                                    .length);
                        }
                        return discoverFooters(context, index + 1);
                    }
                    var footer = (Nbke2RunFooterV1) frame;
                    require(
                            footer.runBinding().equals(run.binding)
                                    && (run.record
                                                    .root()
                                                    .kafkaEndOffsetExclusive()
                                                    .isEmpty()
                                            || footer.kafkaEndOffsetExclusive()
                                                    == run.record
                                                            .root()
                                                            .kafkaEndOffsetExclusive()
                                                            .orElseThrow())
                                    && footer.lastPhysicalEntryIdExclusive() == entryId + 1
                                    && footer.sealOwnerEpoch() >= run.binding.creatorOwnerEpoch(),
                            "native footer differs from admitted root");
                    if (!footer.indexDirectory().isEmpty()) {
                        require(
                                footer.indexDirectory().get(0).blockStartOffset()
                                        == run.record.root().kafkaStartOffset(),
                                "footer index directory omits the run prefix");
                        for (var pointer : footer.indexDirectory()) {
                            require(
                                    pointer.indexBlockEntryId() > 0 && pointer.indexBlockEntryId() < entryId,
                                    "footer index pointer escapes native closed entries");
                        }
                    }
                    context.footers.put(index, footer);
                    if (footer.protocolCheckpointEntryId() >= 0) {
                        context.checkpointEntries.put(index, footer.protocolCheckpointEntryId());
                    }
                    return discoverFooters(context, index + 1);
                });
    }

    private static CompletionStage<Void> openSelectedRun(BookKeeperCellSession owned, Run run) {
        return owned.openRunLedger(run.handle)
                .thenAccept(open -> require(
                        open.exactHandle().equals(Optional.of(run.handle)), "admitted run did not open exactly"));
    }

    private CompletionStage<com.nereusstream.kafka.bookkeeper.nbke2.Nbke2FrameV1> exactFrame(
            BookKeeperCellSession owned, Context context, Run run, long entryId) {
        context.checkTime();
        return owned.readExactEntry(run.handle, entryId).thenApply(read -> {
            require(read.outcome() == RunLedgerReadOutcomeV1.FOUND_EXACT, "checkpoint metadata entry is unavailable");
            var entry = read.exactEntry().orElseThrow();
            require(
                    entry.handle().equals(run.handle) && entry.entryId() == entryId,
                    "metadata read has another identity");
            context.checkTime();
            return Nbke2CodecV1.decode(
                    entry.payload().toByteArray(), run.handle.ledgerIdentity().ledgerId(), entryId);
        });
    }

    private CompletionStage<Void> selectCheckpoint(Context context, int index) {
        context.checkTime();
        if (index < 0) {
            return CompletableFuture.completedFuture(null);
        }
        var footer = context.footers.get(index);
        if (!context.checkpointEntries.containsKey(index)) {
            return selectCheckpoint(context, index - 1);
        }
        var run = context.runs.get(index);
        long entryId = context.checkpointEntries.get(index);
        require(
                entryId > 0 && entryId <= context.proofs.get(index).lastAddConfirmed(),
                "footer checkpoint pointer escapes native closed entries");
        return readOwner
                .read(run.record, ReadPhase.CHECKPOINT, owned -> openSelectedRun(owned, run)
                        .thenCompose(ignored -> exactFrame(owned, context, run, entryId))
                        .handle((frame, failure) -> {
                            if (failure != null || !(frame instanceof Nbke2ProtocolCheckpointV1 checkpoint)) {
                                return false;
                            }
                            try {
                                var state = KafkaProtocolCheckpointStateV1.fromNbke2(checkpoint);
                                if (!checkpoint.runBinding().equals(run.binding)
                                        || !state.vector().isAlignedCompoundCheckpoint()
                                        || state.vector().recoveryCoveredThrough()
                                                < run.record.root().kafkaStartOffset()
                                        || run.record
                                                        .root()
                                                        .kafkaEndOffsetExclusive()
                                                        .isPresent()
                                                && state.vector().recoveryCoveredThrough()
                                                        != run.record
                                                                .root()
                                                                .kafkaEndOffsetExclusive()
                                                                .orElseThrow()
                                        || footer != null
                                                && state.vector().recoveryCoveredThrough()
                                                        != footer.kafkaEndOffsetExclusive()) {
                                    return false;
                                }
                                var sources = new HashMap<
                                        com.nereusstream.storage.api.bookkeeper.StorageRunId,
                                        com.nereusstream.kafka.bookkeeper.checkpoint.KafkaCheckpointReadIndexV1>();
                                for (var source : state.readIndexes()) {
                                    sources.put(source.runId(), source);
                                }
                                var table = new ArrayList<KafkaBookKeeperReadRunV1>();
                                for (int prior = 0; prior <= index; prior++) {
                                    var sourceRun = context.runs.get(prior);
                                    long start = sourceRun.record.root().kafkaStartOffset();
                                    long end = sourceRun
                                                    .record
                                                    .root()
                                                    .kafkaEndOffsetExclusive()
                                                    .isPresent()
                                            ? sourceRun
                                                    .record
                                                    .root()
                                                    .kafkaEndOffsetExclusive()
                                                    .getAsLong()
                                            : state.vector().recoveryCoveredThrough();
                                    if (start == end) {
                                        continue;
                                    }
                                    var nativeFooter = context.footers.get(prior);
                                    var checkpointSource = sources.remove(sourceRun.binding.runId());
                                    if (checkpointSource != null) {
                                        var packed = KafkaPackedBatchLocatorIndexV1.fromCheckpoint(checkpointSource);
                                        long physicalEnd = sourceRun
                                                .record
                                                .recoveryCut()
                                                .flatMap(cut ->
                                                        cut.inertFromEntryId().isPresent()
                                                                ? Optional.of(cut.inertFromEntryId()
                                                                                .getAsLong()
                                                                        - 1)
                                                                : Optional.empty())
                                                .orElse(context.proofs
                                                        .get(prior)
                                                        .lastAddConfirmed());
                                        if (packed.startOffset() != start
                                                || packed.coveredThroughOffset() != end
                                                || packed.at(packed.size() - 1).entryId() > physicalEnd) {
                                            return false;
                                        }
                                        boolean nativeIndex =
                                                sourceRun.record.root().state() == KafkaRunRootStateV1.SEALED
                                                        && nativeFooter != null
                                                        && !nativeFooter
                                                                .indexDirectory()
                                                                .isEmpty();
                                        table.add(new KafkaBookKeeperReadRunV1(
                                                sourceRun.binding,
                                                sourceRun.handle,
                                                start,
                                                end,
                                                0,
                                                nativeIndex ? Optional.empty() : Optional.of(packed),
                                                nativeIndex
                                                        ? Optional.of(new KafkaPackedIndexDirectoryV1(
                                                                nativeFooter.indexDirectory()))
                                                        : Optional.empty()));
                                    } else if (nativeFooter != null
                                            && !nativeFooter.indexDirectory().isEmpty()) {
                                        table.add(new KafkaBookKeeperReadRunV1(
                                                sourceRun.binding,
                                                sourceRun.handle,
                                                start,
                                                end,
                                                0,
                                                Optional.empty(),
                                                Optional.of(new KafkaPackedIndexDirectoryV1(
                                                        nativeFooter.indexDirectory()))));
                                    } else {
                                        return false;
                                    }
                                }
                                if (!sources.isEmpty()) {
                                    return false;
                                }
                                context.state = Optional.of(state);
                                context.end =
                                        context.checkpointEnd = state.vector().recoveryCoveredThrough();
                                context.checkpointRun = index;
                                context.readRuns.addAll(table);
                                return true;
                            } catch (RuntimeException invalid) {
                                return false;
                            }
                        }))
                .thenCompose(selected ->
                        selected ? CompletableFuture.completedFuture(null) : selectCheckpoint(context, index - 1));
    }

    private CompletionStage<Void> replay(Context context, int index) {
        context.checkTime();
        if (index == context.runs.size()) {
            return CompletableFuture.completedFuture(null);
        }
        var run = context.runs.get(index);
        boolean checkpointTail = index == context.checkpointRun;
        require(
                checkpointTail || run.record.root().kafkaStartOffset() == context.end,
                "run history has a Kafka offset gap");
        var probeBytes = context.dataProbeBytes.get(index);
        if (probeBytes != null) {
            context.entries = Math.addExact(context.entries, 1);
            context.bytes = Math.addExact(context.bytes, probeBytes);
        }
        long remainingEntries = context.envelope.maximumEntries() - context.entries;
        long remainingBytes = context.envelope.maximumEncodedBytes() - context.bytes;
        long remainingNanos = context.envelope.maximumElapsedNanos() - context.elapsed();
        require(
                remainingEntries > 0 && remainingBytes > 0 && remainingNanos > 0,
                "closed history replay exhausted its remaining envelope: entries=" + remainingEntries + ", bytes="
                        + remainingBytes + ", nanos=" + remainingNanos);
        var remaining = new KafkaBookKeeperRecoveryEnvelopeV1(remainingEntries, remainingBytes, remainingNanos);
        var request = new KafkaBookKeeperRecoveryRequestV1(
                run.binding,
                run.handle,
                run.record.root().kafkaStartOffset(),
                checkpointTail ? OptionalLong.of(context.checkpointEntries.get(index)) : OptionalLong.empty(),
                remaining,
                run.owner,
                run.record,
                context.fence,
                checkpointTail ? Optional.empty() : context.state);
        return readOwner
                .read(run.record, ReadPhase.REPLAY, owned -> openSelectedRun(owned, run)
                        .thenCompose(ignored -> new KafkaBookKeeperTakeoverRecoveryV1(owned, adapter, nanoTime)
                                .recoverFenced(request, context.proofs.get(index))))
                .thenCompose(result -> {
                    require(
                            result.recovered(),
                            "closed run replay failed: " + result.outcome() + ": " + result.detail());
                    context.entries =
                            Math.addExact(context.entries, result.progress().entries());
                    context.bytes =
                            Math.addExact(context.bytes, result.progress().encodedBytes());
                    context.checkTime();
                    context.end = result.newLeaderLeo().orElseThrow();
                    context.state = result.recoveredProtocolState();
                    context.recoveredRuns.add(
                            new RecoveredRun(run.owner, run.record, context.proofs.get(index), result));
                    if (!result.recoveredLocators().isEmpty()) {
                        var tail = new KafkaActiveTailStateV1(
                                run.record.root().kafkaStartOffset(), context.end, result.recoveredLocators());
                        var footer = context.footers.get(index);
                        boolean indexed =
                                footer != null && !footer.indexDirectory().isEmpty();
                        context.readRuns.add(new KafkaBookKeeperReadRunV1(
                                run.binding,
                                run.handle,
                                tail.startOffset(),
                                tail.endOffsetExclusive(),
                                0,
                                indexed
                                        ? Optional.empty()
                                        : Optional.of(KafkaPackedBatchLocatorIndexV1.fromActiveTail(tail)),
                                indexed
                                        ? Optional.of(new KafkaPackedIndexDirectoryV1(footer.indexDirectory()))
                                        : Optional.empty()));
                    }
                    return replay(context, index + 1);
                });
    }

    public record RecoveredRun(
            KafkaOwnerAdmissionV1 closedOwner,
            KafkaRunRootRecordV2 root,
            RunLedgerRecoveryProofV1 fenceProof,
            KafkaBookKeeperRecoveryResultV1 recovery) {}

    public record Result(
            List<KafkaOwnerAdmissionV1> closedOwners,
            List<RecoveredRun> runs,
            List<KafkaRunRootSnapshotV1> admittedRoots,
            long endOffset,
            Optional<KafkaProtocolCheckpointStateV1> protocolState,
            KafkaBookKeeperRecoveryProgressV1 progress,
            List<KafkaBookKeeperReadRunV1> readRuns,
            OptionalLong selectedCheckpointEndOffset,
            Optional<KafkaRunRootSnapshotV1> lastRoot,
            Optional<KafkaRunRootSnapshotV1> selectedPrefixAnchor) {
        public Result {
            closedOwners = List.copyOf(closedOwners);
            runs = List.copyOf(runs);
            admittedRoots = List.copyOf(admittedRoots);
            readRuns = List.copyOf(readRuns);
            Objects.requireNonNull(protocolState, "protocolState");
            Objects.requireNonNull(progress, "progress");
            Objects.requireNonNull(selectedPrefixAnchor, "selectedPrefixAnchor");
        }

        /** Rebuilt indexes share the publication generation captured by this read view. */
        public KafkaBookKeeperRunTableV1 readTable(long sourceGeneration) {
            var table = new ArrayList<KafkaBookKeeperReadRunV1>();
            for (var run : readRuns) {
                table.add(new KafkaBookKeeperReadRunV1(
                        run.runBinding(),
                        run.handle(),
                        run.startOffset(),
                        run.endOffsetExclusive(),
                        sourceGeneration,
                        run.activeIndex(),
                        run.sealedDirectory()));
            }
            return new KafkaBookKeeperRunTableV1(table);
        }
    }

    private record Run(
            KafkaOwnerAdmissionV1 owner,
            KafkaRunRootRecordV2 record,
            Nbke2RunBindingV1 binding,
            RunLedgerHandleV1 handle) {}

    private final class Context {
        private final KafkaPartitionFenceV1 fence;
        private final KafkaBookKeeperRecoveryEnvelopeV1 envelope;
        private final long started;
        private final List<KafkaOwnerAdmissionV1> owners = new ArrayList<>();
        private final java.util.Set<Long> ownerEpochs = new HashSet<>();
        private final java.util.Set<com.nereusstream.storage.api.bookkeeper.StorageRunId> runIds = new HashSet<>();
        private final java.util.Set<Long> ledgerIds = new HashSet<>();
        private final List<Run> runs = new ArrayList<>();
        private final Map<com.nereusstream.storage.api.bookkeeper.StorageRunId, KafkaRunRootRecordV2> selectedBirths =
                new HashMap<>();
        private Optional<KafkaRunRootRecordV2> catalogTail = Optional.empty();
        private Optional<KafkaRunRootSnapshotV1> selectedPrefixAnchor = Optional.empty();
        private int discoveredRuns;
        private final long historyStart;
        private final OptionalLong selectedEnd;
        private final Map<Sha256Digest, SourceExtent> selectedSources = new HashMap<>();
        private Optional<SourceExtent> selectedGeneration = Optional.empty();
        private boolean generationBoundaryChecked = true;
        private final List<RunLedgerRecoveryProofV1> proofs = new ArrayList<>();
        private final List<RecoveredRun> recoveredRuns = new ArrayList<>();
        private Optional<KafkaProtocolCheckpointStateV1> state = Optional.empty();
        private final Map<Integer, Nbke2RunFooterV1> footers = new HashMap<>();
        private final List<KafkaBookKeeperReadRunV1> readRuns = new ArrayList<>();
        private int checkpointRun = -1;
        private long checkpointEnd;
        private final Map<Integer, Long> checkpointEntries = new HashMap<>();
        private final Map<Integer, Long> dataProbeBytes = new HashMap<>();
        private long end;
        private long entries;
        private long bytes;

        private Context(
                KafkaPartitionFenceV1 fence,
                long startOffset,
                KafkaBookKeeperRecoveryEnvelopeV1 envelope,
                long started,
                Optional<KafkaProtocolCheckpointStateV1> selectedState,
                List<SourceExtent> selectedSources) {
            this.fence = fence;
            this.historyStart = startOffset;
            this.state = selectedState;
            this.selectedEnd = selectedState.isPresent()
                    ? OptionalLong.of(selectedState.orElseThrow().vector().recoveryCoveredThrough())
                    : OptionalLong.empty();
            this.end = selectedEnd.orElse(startOffset);
            this.checkpointEnd = end;
            for (var source : selectedSources) {
                if (source.kind() == SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2) {
                    require(
                            this.selectedGeneration.isEmpty()
                                    && source.coverage().inclusiveStart() == startOffset,
                            "selected recovery repeats or displaces its generation prefix");
                    this.selectedGeneration = Optional.of(source);
                    this.generationBoundaryChecked = false;
                    continue;
                }
                require(
                        this.selectedSources.putIfAbsent(
                                        source.ledgerIdentitySha256().orElseThrow(), source)
                                == null,
                        "selected recovery repeats a raw physical source");
            }
            this.envelope = envelope;
            this.started = started;
        }

        private long elapsed() {
            return Math.max(0, nanoTime.getAsLong() - started);
        }

        private void checkTime() {
            require(elapsed() < envelope.maximumElapsedNanos(), "closed history recovery exceeded elapsed bound");
        }

        private KafkaBookKeeperRecoveryProgressV1 progress() {
            return new KafkaBookKeeperRecoveryProgressV1(entries, bytes, elapsed());
        }
    }

    private static void require(boolean condition, String detail) {
        if (!condition) {
            throw new IllegalStateException(detail);
        }
    }
}
