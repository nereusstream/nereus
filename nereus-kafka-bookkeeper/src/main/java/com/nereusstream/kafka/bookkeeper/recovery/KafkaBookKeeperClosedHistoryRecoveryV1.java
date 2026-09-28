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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryProgressV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaActiveTailStateV1;
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
import java.util.function.LongSupplier;

/** Discovers immutable closed lineage, fences all its runs, then replays one continuous Kafka history. */
public final class KafkaBookKeeperClosedHistoryRecoveryV1 {
    public static final int MAX_CLOSED_OWNERS = 1024;
    public static final int MAX_HISTORY_RUNS = 1024;
    private final BookKeeperCellSession session;
    private final KafkaBookKeeperOwnerAuthorityV1 authority;
    private final KafkaBookKeeperTakeoverRecoveryV1 recovery;
    private final LongSupplier nanoTime;

    public KafkaBookKeeperClosedHistoryRecoveryV1(
            BookKeeperCellSession session,
            KafkaBookKeeperOwnerAuthorityV1 authority,
            KafkaRecoveryBatchProtocolAdapterV1 adapter,
            LongSupplier nanoTime) {
        this.session = Objects.requireNonNull(session, "session");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        recovery = new KafkaBookKeeperTakeoverRecoveryV1(session, adapter, nanoTime);
    }

    /** The caller must recheck the exact native assignment before opening a new owner or publishing ready. */
    public CompletionStage<Result> recover(
            KafkaOwnerAdmissionV1 closed,
            KafkaPartitionFenceV1 newFence,
            long historyStartOffset,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope) {
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
        Context context = new Context(newFence, historyStartOffset, envelope, nanoTime.getAsLong());
        return authority
                .readClosedOwner(closed.owner().ownerEpoch())
                .thenCompose(stored -> {
                    require(stored.equals(Optional.of(closed)), "closed owner archive differs from the selected cut");
                    return discoverOwners(context, closed);
                })
                .thenCompose(ignored -> {
                    Collections.reverse(context.owners);
                    return discoverRuns(context, 0, 0);
                })
                .thenCompose(ignored -> fenceAll(context, 0))
                .thenCompose(ignored -> discoverFooters(context, 0))
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
                        context.end,
                        context.state,
                        context.progress(),
                        context.readRuns,
                        context.checkpointRun < 0 ? OptionalLong.empty() : OptionalLong.of(context.checkpointEnd),
                        context.runs.isEmpty()
                                ? Optional.empty()
                                : Optional.of(context.runs
                                        .get(context.runs.size() - 1)
                                        .record
                                        .root())));
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

    private CompletionStage<Void> discoverRuns(Context context, int ownerIndex, int runIndex) {
        context.checkTime();
        if (ownerIndex == context.owners.size()) {
            return CompletableFuture.completedFuture(null);
        }
        var owner = context.owners.get(ownerIndex);
        if (runIndex == owner.runs().size()) {
            return discoverRuns(context, ownerIndex + 1, 0);
        }
        require(context.runs.size() < MAX_HISTORY_RUNS, "closed run discovery exceeds its bound");
        var link = owner.runs().get(runIndex);
        return authority.readAdmittedRun(link).thenCompose(found -> {
            var record = found.orElseThrow(() -> new IllegalStateException("legally admitted run is absent"));
            var root = record.root();
            Optional<com.nereusstream.storage.api.bookkeeper.StorageRunId> predecessor = context.runs.isEmpty()
                    ? Optional.empty()
                    : Optional.of(context.runs
                            .get(context.runs.size() - 1)
                            .record
                            .root()
                            .runId());
            require(
                    record.initialLink().equals(link)
                            && !record.retired()
                            && owner.scopeSha256()
                                    .equals(Sha256Digest.hash(
                                            KafkaRunRootRecordV2.Scope.of(root).encode()))
                            && owner.owner().ownerEpoch() == root.creatorOwnerEpoch()
                            && owner.owner().kafkaLeaderEpoch() == root.kafkaLeaderEpoch()
                            && root.predecessorRunId().equals(predecessor)
                            && context.runIds.add(root.runId())
                            && context.ledgerIds.add(root.ledgerIdentity().ledgerId()),
                    "admitted run chain identity differs or repeats");
            if (!context.runs.isEmpty()) {
                var parent = context.runs.get(context.runs.size() - 1).record;
                require(
                        parent.root().state() == KafkaRunRootStateV1.SEALED
                                && parent.root().kafkaEndOffsetExclusive().orElseThrow() == root.kafkaStartOffset()
                                && parent.successor()
                                        .filter(value -> !value.equals(link))
                                        .isEmpty(),
                        "run predecessor lacks the exact sealed end or selects another child");
            } else {
                require(root.kafkaStartOffset() == context.end, "history begins after an unverified prefix");
            }
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
            return discoverRuns(context, ownerIndex, runIndex + 1);
        });
    }

    private CompletionStage<Void> fenceAll(Context context, int index) {
        context.checkTime();
        if (index == context.runs.size()) {
            return CompletableFuture.completedFuture(null);
        }
        var run = context.runs.get(index);
        return session.openRunLedger(run.handle)
                .thenCompose(open -> {
                    require(open.exactHandle().equals(Optional.of(run.handle)), "admitted run did not open exactly");
                    return session.fenceAndRecoverRunLedger(run.handle);
                })
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
        return exactFrame(context, run, entryId).thenCompose(frame -> {
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
                    context.dataProbeBytes.put(index, (long)
                            Nbke2CodecV1.encode(run.handle.ledgerIdentity().ledgerId(), entryId, data).length);
                }
                return discoverFooters(context, index + 1);
            }
            var footer = (Nbke2RunFooterV1) frame;
            require(
                    footer.runBinding().equals(run.binding)
                            && (run.record.root().kafkaEndOffsetExclusive().isEmpty()
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

    private CompletionStage<com.nereusstream.kafka.bookkeeper.nbke2.Nbke2FrameV1> exactFrame(
            Context context, Run run, long entryId) {
        context.checkTime();
        return session.readExactEntry(run.handle, entryId).thenApply(read -> {
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
        return exactFrame(context, run, entryId)
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
                                || run.record.root().kafkaEndOffsetExclusive().isPresent()
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
                                        .flatMap(cut -> cut.inertFromEntryId().isPresent()
                                                ? Optional.of(
                                                        cut.inertFromEntryId().getAsLong() - 1)
                                                : Optional.empty())
                                        .orElse(context.proofs.get(prior).lastAddConfirmed());
                                if (packed.startOffset() != start
                                        || packed.coveredThroughOffset() != end
                                        || packed.at(packed.size() - 1).entryId() > physicalEnd) {
                                    return false;
                                }
                                boolean nativeIndex = sourceRun.record.root().state() == KafkaRunRootStateV1.SEALED
                                        && nativeFooter != null
                                        && !nativeFooter.indexDirectory().isEmpty();
                                table.add(new KafkaBookKeeperReadRunV1(
                                        sourceRun.binding,
                                        sourceRun.handle,
                                        start,
                                        end,
                                        0,
                                        nativeIndex ? Optional.empty() : Optional.of(packed),
                                        nativeIndex
                                                ? Optional.of(
                                                        new KafkaPackedIndexDirectoryV1(nativeFooter.indexDirectory()))
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
                                        Optional.of(new KafkaPackedIndexDirectoryV1(nativeFooter.indexDirectory()))));
                            } else {
                                return false;
                            }
                        }
                        if (!sources.isEmpty()) {
                            return false;
                        }
                        context.state = Optional.of(state);
                        context.end = context.checkpointEnd = state.vector().recoveryCoveredThrough();
                        context.checkpointRun = index;
                        context.readRuns.addAll(table);
                        return true;
                    } catch (RuntimeException invalid) {
                        return false;
                    }
                })
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
        var remaining = new KafkaBookKeeperRecoveryEnvelopeV1(
                context.envelope.maximumEntries() - context.entries,
                context.envelope.maximumEncodedBytes() - context.bytes,
                context.envelope.maximumElapsedNanos() - context.elapsed());
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
        return recovery.recoverFenced(request, context.proofs.get(index)).thenCompose(result -> {
            require(result.recovered(), "closed run replay failed: " + result.outcome() + ": " + result.detail());
            context.entries = Math.addExact(context.entries, result.progress().entries());
            context.bytes = Math.addExact(context.bytes, result.progress().encodedBytes());
            context.checkTime();
            context.end = result.newLeaderLeo().orElseThrow();
            context.state = result.recoveredProtocolState();
            context.recoveredRuns.add(new RecoveredRun(run.owner, run.record, context.proofs.get(index), result));
            if (!result.recoveredLocators().isEmpty()) {
                var tail = new KafkaActiveTailStateV1(
                        run.record.root().kafkaStartOffset(), context.end, result.recoveredLocators());
                var footer = context.footers.get(index);
                boolean indexed = footer != null && !footer.indexDirectory().isEmpty();
                context.readRuns.add(new KafkaBookKeeperReadRunV1(
                        run.binding,
                        run.handle,
                        tail.startOffset(),
                        tail.endOffsetExclusive(),
                        0,
                        indexed ? Optional.empty() : Optional.of(KafkaPackedBatchLocatorIndexV1.fromActiveTail(tail)),
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
            long endOffset,
            Optional<KafkaProtocolCheckpointStateV1> protocolState,
            KafkaBookKeeperRecoveryProgressV1 progress,
            List<KafkaBookKeeperReadRunV1> readRuns,
            OptionalLong selectedCheckpointEndOffset,
            Optional<KafkaRunRootSnapshotV1> lastRoot) {
        public Result {
            closedOwners = List.copyOf(closedOwners);
            runs = List.copyOf(runs);
            readRuns = List.copyOf(readRuns);
            Objects.requireNonNull(protocolState, "protocolState");
            Objects.requireNonNull(progress, "progress");
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
                long started) {
            this.fence = fence;
            this.end = startOffset;
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
