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
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryProgressV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryStatusV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaAssignedProtocolBatchV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2FrameV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RangeIndexBlockV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ExactLedgerEntryV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.LongSupplier;

/** K7 fenced prior-run recovery with cumulative tail bounds and native-election-limited adoption. */
public final class KafkaBookKeeperTakeoverRecoveryV1 {
    private final BookKeeperCellSession session;
    private final KafkaRecoveryBatchProtocolAdapterV1 protocolAdapter;
    private final LongSupplier nanoTime;

    public KafkaBookKeeperTakeoverRecoveryV1(
            BookKeeperCellSession session, KafkaRecoveryBatchProtocolAdapterV1 protocolAdapter, LongSupplier nanoTime) {
        this.session = Objects.requireNonNull(session, "session");
        this.protocolAdapter = Objects.requireNonNull(protocolAdapter, "protocolAdapter");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    public CompletionStage<KafkaBookKeeperRecoveryResultV1> recover(KafkaBookKeeperRecoveryRequestV1 request) {
        Objects.requireNonNull(request, "request");
        long startedNanos = nanoTime.getAsLong();
        if (!request.handle().providerScopeId().equals(session.providerScopeId())) {
            return completed(failure(
                    request,
                    KafkaBookKeeperRecoveryOutcomeV1.OPEN_FAILED,
                    KafkaBookKeeperRecoveryProgressV1.ZERO,
                    OptionalLong.empty(),
                    "prior run belongs to another provider scope"));
        }
        CompletionStage<RunLedgerOpenResultV1> opened;
        try {
            opened = Objects.requireNonNull(session.openRunLedger(request.handle()), "null open stage");
        } catch (RuntimeException failure) {
            return completed(failure(
                    request,
                    KafkaBookKeeperRecoveryOutcomeV1.OPEN_FAILED,
                    KafkaBookKeeperRecoveryProgressV1.ZERO,
                    OptionalLong.empty(),
                    "provider rejected prior-run open"));
        }
        return opened.handle((result, error) -> error == null && exactOpen(result, request))
                .thenCompose(exact -> exact
                        ? fence(request, startedNanos)
                        : completed(failure(
                                request,
                                KafkaBookKeeperRecoveryOutcomeV1.OPEN_FAILED,
                                KafkaBookKeeperRecoveryProgressV1.ZERO,
                                OptionalLong.empty(),
                                "prior run did not open with the exact handle")));
    }

    private CompletionStage<KafkaBookKeeperRecoveryResultV1> fence(
            KafkaBookKeeperRecoveryRequestV1 request, long startedNanos) {
        CompletionStage<ProviderMutationResultV1<RunLedgerRecoveryProofV1>> fenced;
        try {
            fenced = Objects.requireNonNull(session.fenceAndRecoverRunLedger(request.handle()), "null fence stage");
        } catch (RuntimeException failure) {
            return completed(failure(
                    request,
                    KafkaBookKeeperRecoveryOutcomeV1.FENCE_FAILED,
                    KafkaBookKeeperRecoveryProgressV1.ZERO,
                    OptionalLong.empty(),
                    "provider rejected prior-run fencing"));
        }
        return fenced.handle(
                        (result, error) -> error == null ? exactRecoveryProof(result, request) : OptionalLong.empty())
                .thenCompose(lastAddConfirmed -> {
                    if (lastAddConfirmed.isEmpty()) {
                        return completed(failure(
                                request,
                                KafkaBookKeeperRecoveryOutcomeV1.FENCE_FAILED,
                                KafkaBookKeeperRecoveryProgressV1.ZERO,
                                OptionalLong.empty(),
                                "prior run fencing/recovery was not established exactly"));
                    }
                    Context context = new Context(request, lastAddConfirmed.getAsLong(), startedNanos);
                    return selectCheckpoint(context);
                });
    }

    private CompletionStage<KafkaBookKeeperRecoveryResultV1> selectCheckpoint(Context context) {
        OptionalLong hint = context.request.hintedCheckpointEntryId();
        if (hint.isEmpty() || hint.getAsLong() > context.lastAddConfirmed) {
            return initializeFromHeader(context);
        }
        long entryId = hint.getAsLong();
        return read(context, entryId).thenCompose(read -> {
            if (read.terminalOutcome().isPresent()) {
                return completed(readFailure(context, read, entryId));
            }
            boolean hintedEntryIsCheckpointControl = read.entry().isEmpty();
            if (read.entry().isPresent()) {
                try {
                    Nbke2FrameV1 frame = decode(context, read.entry().orElseThrow());
                    if (frame instanceof Nbke2ProtocolCheckpointV1 checkpoint) {
                        hintedEntryIsCheckpointControl = true;
                        KafkaProtocolCheckpointStateV1 state = KafkaProtocolCheckpointStateV1.fromNbke2(checkpoint);
                        long coveredThrough = state.vector().recoveryCoveredThrough();
                        if (checkpoint.runBinding().equals(context.request.runBinding())
                                && state.vector().isAlignedCompoundCheckpoint()
                                && coveredThrough >= context.request.kafkaStartOffset()
                                && coveredThrough
                                        <= context.request.electionBoundary().electionAdoptableEndOffset()) {
                            context.protocolState = state;
                            context.physicalRecoveredEndOffset = coveredThrough;
                            context.nextPhysicalOffset = coveredThrough;
                            return scan(context, Math.incrementExact(entryId));
                        }
                    }
                } catch (RuntimeException invalidCheckpoint) {
                    hintedEntryIsCheckpointControl = true;
                    // The exact hinted control entry is unusable; bounded replay starts from RUN_HEADER.
                }
            }
            if (hintedEntryIsCheckpointControl) {
                context.ignoredCheckpointEntryId = OptionalLong.of(entryId);
            }
            return initializeFromHeader(context);
        });
    }

    private CompletionStage<KafkaBookKeeperRecoveryResultV1> initializeFromHeader(Context context) {
        return read(context, 0).thenCompose(read -> {
            if (read.terminalOutcome().isPresent()) {
                return completed(readFailure(context, read, 0));
            }
            try {
                ExactLedgerEntryV1 entry = read.entry().orElseThrow();
                Nbke2FrameV1 decoded = decode(context, entry);
                if (!(decoded instanceof Nbke2RunHeaderV1 header)
                        || !header.runBinding().equals(context.request.runBinding())
                        || header.kafkaStartOffset() != context.request.kafkaStartOffset()
                        || header.firstDataEntryId() != 1
                        || !header.ledgerConfigurationDigest()
                                .equals(context.request.handle().configurationDigest())) {
                    throw new IllegalArgumentException("RUN_HEADER differs from the prior run identity");
                }
            } catch (RuntimeException invalidHeader) {
                return completed(failure(
                        context.request,
                        KafkaBookKeeperRecoveryOutcomeV1.CORRUPT_HEADER,
                        context.progress,
                        OptionalLong.of(0),
                        "RUN_HEADER failed exact identity or NBKE2 validation"));
            }
            context.protocolState = KafkaProtocolCheckpointStateV1.empty(
                    context.request.runBinding(), context.request.kafkaStartOffset());
            context.physicalRecoveredEndOffset = context.request.kafkaStartOffset();
            context.nextPhysicalOffset = context.request.kafkaStartOffset();
            return scan(context, 1);
        });
    }

    private CompletionStage<KafkaBookKeeperRecoveryResultV1> scan(Context context, long entryId) {
        if (entryId > context.lastAddConfirmed) {
            if (context.pendingGroup != null) {
                context.conflictEntryId = OptionalLong.of(context.pendingGroup.firstEntryId);
                context.pendingGroup = null;
            }
            return completed(finish(context));
        }
        if (context.ignoredCheckpointEntryId.isPresent() && context.ignoredCheckpointEntryId.getAsLong() == entryId) {
            return scan(context, Math.incrementExact(entryId));
        }
        return read(context, entryId).thenCompose(read -> {
            if (read.terminalOutcome().isPresent()) {
                return completed(readFailure(context, read, entryId));
            }
            if (read.entry().isEmpty()) {
                context.conflictEntryId = OptionalLong.of(entryId);
                return completed(finish(context));
            }
            Nbke2FrameV1 frame;
            try {
                frame = decode(context, read.entry().orElseThrow());
            } catch (RuntimeException corruption) {
                context.conflictEntryId = OptionalLong.of(entryId);
                return completed(finish(context));
            }
            if (!frame.runBinding().equals(context.request.runBinding())) {
                context.conflictEntryId = OptionalLong.of(entryId);
                return completed(finish(context));
            }
            if (frame instanceof Nbke2DataV1 data) {
                DataAcceptance acceptance = acceptData(context, entryId, data);
                if (acceptance == DataAcceptance.ELECTION_BOUNDARY_SPLITS_BATCH) {
                    return completed(failure(
                            context.request,
                            KafkaBookKeeperRecoveryOutcomeV1.ELECTION_BOUNDARY_NOT_BATCH_ALIGNED,
                            context.progress,
                            OptionalLong.of(entryId),
                            "native election boundary splits one complete RecordBatch group"));
                }
                if (acceptance == DataAcceptance.CONFLICT) {
                    context.conflictEntryId = OptionalLong.of(entryId);
                    return completed(finish(context));
                }
                return scan(context, Math.incrementExact(entryId));
            }
            if (context.pendingGroup != null || frame instanceof Nbke2RunHeaderV1) {
                context.conflictEntryId = OptionalLong.of(entryId);
                return completed(finish(context));
            }
            if (frame instanceof Nbke2ProtocolCheckpointV1 checkpoint) {
                try {
                    KafkaProtocolCheckpointStateV1.fromNbke2(checkpoint);
                } catch (RuntimeException ignoredCorruptAcceleration) {
                    // A suffix checkpoint is disposable acceleration after replay has already covered its range.
                }
                return scan(context, Math.incrementExact(entryId));
            }
            if (frame instanceof Nbke2RangeIndexBlockV1) {
                return scan(context, Math.incrementExact(entryId));
            }
            if (frame instanceof Nbke2RunFooterV1 footer) {
                if (footer.kafkaEndOffsetExclusive() != context.physicalRecoveredEndOffset) {
                    context.conflictEntryId = OptionalLong.of(entryId);
                }
                return completed(finish(context));
            }
            context.conflictEntryId = OptionalLong.of(entryId);
            return completed(finish(context));
        });
    }

    private DataAcceptance acceptData(Context context, long entryId, Nbke2DataV1 data) {
        try {
            KafkaNativeAssignedRecordBatchV1 batch = KafkaNativeAssignedRecordBatchV1.validate(
                    KafkaRawAssignedRecordBatchFactsV1.parse(data.rawAssignedRecordBatch()));
            if (batch.baseOffset() != data.baseOffset()
                    || batch.endOffsetExclusive() != data.endOffsetExclusive()
                    || batch.partitionLeaderEpoch() != data.runBinding().kafkaLeaderEpoch()) {
                return DataAcceptance.CONFLICT;
            }
            var delta = Objects.requireNonNull(protocolAdapter.protocolDelta(batch), "protocol delta");
            if (delta.logicalOffsetCount() != data.endOffsetExclusive() - data.baseOffset()) {
                return DataAcceptance.CONFLICT;
            }
            PendingGroup group = context.pendingGroup;
            if (group == null) {
                if (data.memberOrdinal() != 0 || data.baseOffset() != context.nextPhysicalOffset) {
                    return DataAcceptance.CONFLICT;
                }
                group = new PendingGroup(
                        entryId, data.baseOffset(), data.memberCount(), data.appendGroupId(), data.storageAttemptId());
                context.pendingGroup = group;
            }
            if (entryId != Math.addExact(group.firstEntryId, group.batches.size())
                    || data.memberOrdinal() != group.batches.size()
                    || data.memberCount() != group.memberCount
                    || !data.appendGroupId().equals(group.appendGroupId)
                    || !data.storageAttemptId().equals(group.storageAttemptId)
                    || data.baseOffset() != group.nextOffset) {
                return DataAcceptance.CONFLICT;
            }
            group.payloadDigest.update(data.rawAssignedRecordBatch().toByteArray());
            group.batches.add(new KafkaAssignedProtocolBatchV1(data.baseOffset(), data.endOffsetExclusive(), delta));
            group.nextOffset = data.endOffsetExclusive();
            if (data.memberOrdinal() != data.memberCount() - 1) {
                return DataAcceptance.ACCEPTED;
            }
            var descriptor = data.terminalDescriptor().orElseThrow();
            if (descriptor.groupStartOffset() != group.startOffset
                    || descriptor.groupEndOffsetExclusive() != group.nextOffset
                    || descriptor.firstDataEntryId() != group.firstEntryId
                    || descriptor.lastDataEntryId() != entryId
                    || !descriptor
                            .aggregateAssignedPayloadSha256()
                            .equals(Sha256Digest.copyOf(group.payloadDigest.digest()))) {
                return DataAcceptance.CONFLICT;
            }
            long adoptable = context.request.electionBoundary().electionAdoptableEndOffset();
            if (adoptable > group.startOffset && adoptable < group.nextOffset) {
                return DataAcceptance.ELECTION_BOUNDARY_SPLITS_BATCH;
            }
            context.physicalRecoveredEndOffset = group.nextOffset;
            context.nextPhysicalOffset = group.nextOffset;
            if (group.nextOffset <= adoptable) {
                KafkaSpeculativeCommitV1 commit = new KafkaSpeculativeCommitV1(
                        group.startOffset, group.nextOffset, context.request.recoveredStateFence(), group.batches);
                KafkaCommittedProducerStateV1 producers =
                        context.protocolState.producerState().apply(commit);
                KafkaTransactionStateV1 transactions =
                        context.protocolState.transactionState().apply(commit);
                KafkaLeaderEpochIndexV1 leaderEpochs = context.protocolState
                        .leaderEpochIndex()
                        .observe(context.request.runBinding().kafkaLeaderEpoch(), group.startOffset);
                KafkaRecoveryCheckpointVectorV1 vector = new KafkaRecoveryCheckpointVectorV1(
                        context.request.runBinding(),
                        group.nextOffset,
                        group.nextOffset,
                        group.nextOffset,
                        group.nextOffset);
                context.protocolState =
                        new KafkaProtocolCheckpointStateV1(vector, producers, transactions, leaderEpochs);
            }
            context.pendingGroup = null;
            return DataAcceptance.ACCEPTED;
        } catch (RuntimeException invalidData) {
            return DataAcceptance.CONFLICT;
        }
    }

    private KafkaBookKeeperRecoveryResultV1 finish(Context context) {
        long adoptable = context.request.electionBoundary().electionAdoptableEndOffset();
        if (context.physicalRecoveredEndOffset < adoptable) {
            return failure(
                    context,
                    KafkaBookKeeperRecoveryOutcomeV1.PHYSICAL_SHORTFALL,
                    context.progress,
                    context.conflictEntryId,
                    "verified physical candidate ends before the native election boundary");
        }
        if (!context.request.electionBoundary().appliedThroughAdoptableBoundary()) {
            return failure(
                    context,
                    KafkaBookKeeperRecoveryOutcomeV1.REPLICA_APPLIED_SHORTFALL,
                    context.progress,
                    context.conflictEntryId,
                    "elected replica has not applied through its native adoption boundary");
        }
        if (context.protocolState.vector().recoveryCoveredThrough() != adoptable) {
            return failure(
                    context,
                    KafkaBookKeeperRecoveryOutcomeV1.ELECTION_BOUNDARY_NOT_BATCH_ALIGNED,
                    context.progress,
                    context.conflictEntryId,
                    "recovered protocol components do not end at the native election boundary");
        }
        boolean residue = context.physicalRecoveredEndOffset > adoptable || context.conflictEntryId.isPresent();
        return new KafkaBookKeeperRecoveryResultV1(
                residue
                        ? KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_WITH_INERT_RESIDUE
                        : KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_EXACT,
                context.physicalRecoveredEndOffset,
                OptionalLong.of(Math.min(context.physicalRecoveredEndOffset, adoptable)),
                Optional.of(context.protocolState),
                context.progress,
                context.conflictEntryId,
                residue
                        ? "later physical bytes are inert old-epoch residue"
                        : "physical and native election cuts agree");
    }

    private CompletionStage<RecoveryRead> read(Context context, long entryId) {
        ExactLedgerEntryV1 cached = context.entries.get(entryId);
        if (cached != null) {
            return completed(RecoveryRead.found(cached));
        }
        if (context.absentEntries.contains(entryId)) {
            return completed(RecoveryRead.absent());
        }
        if (context.progress.entries() >= context.request.envelope().maximumEntries()) {
            return completed(RecoveryRead.terminal(KafkaBookKeeperRecoveryOutcomeV1.ENVELOPE_EXCEEDED));
        }
        CompletionStage<RunLedgerReadResultV1> accepted;
        try {
            accepted = Objects.requireNonNull(
                    session.readExactEntry(context.request.handle(), entryId), "null read stage");
        } catch (RuntimeException failure) {
            return completed(RecoveryRead.terminal(KafkaBookKeeperRecoveryOutcomeV1.PROVIDER_FAILURE));
        }
        return accepted.handle((result, failure) -> {
            long observedElapsed = Math.max(0, nanoTime.getAsLong() - context.startedNanos);
            long elapsed = Math.max(context.progress.elapsedNanos(), observedElapsed);
            long bytes = 0;
            if (failure == null && result != null && result.outcome() == RunLedgerReadOutcomeV1.FOUND_EXACT) {
                bytes = result.exactEntry().orElseThrow().payload().length();
            }
            context.progress = new KafkaBookKeeperRecoveryProgressV1(
                    Math.incrementExact(context.progress.entries()),
                    Math.addExact(context.progress.encodedBytes(), bytes),
                    elapsed);
            KafkaBookKeeperRecoveryStatusV1 envelopeStatus =
                    context.request.envelope().classify(context.progress);
            if (envelopeStatus != KafkaBookKeeperRecoveryStatusV1.WITHIN_ENVELOPE) {
                return RecoveryRead.terminal(KafkaBookKeeperRecoveryOutcomeV1.ENVELOPE_EXCEEDED);
            }
            if (failure != null || result == null || result.outcome() == RunLedgerReadOutcomeV1.PROVIDER_FAILURE) {
                return RecoveryRead.terminal(KafkaBookKeeperRecoveryOutcomeV1.PROVIDER_FAILURE);
            }
            if (result.outcome() == RunLedgerReadOutcomeV1.FENCED) {
                return RecoveryRead.terminal(KafkaBookKeeperRecoveryOutcomeV1.FENCE_FAILED);
            }
            if (result.outcome() == RunLedgerReadOutcomeV1.DEFINITIVELY_ABSENT) {
                context.absentEntries.add(entryId);
                return RecoveryRead.absent();
            }
            ExactLedgerEntryV1 entry = result.exactEntry().orElseThrow();
            if (!entry.handle().equals(context.request.handle()) || entry.entryId() != entryId) {
                return RecoveryRead.terminal(KafkaBookKeeperRecoveryOutcomeV1.PROVIDER_FAILURE);
            }
            context.entries.put(entryId, entry);
            return RecoveryRead.found(entry);
        });
    }

    private Nbke2FrameV1 decode(Context context, ExactLedgerEntryV1 entry) {
        return Nbke2CodecV1.decode(
                entry.payload().toByteArray(),
                context.request.handle().ledgerIdentity().ledgerId(),
                entry.entryId());
    }

    private KafkaBookKeeperRecoveryResultV1 readFailure(Context context, RecoveryRead read, long entryId) {
        return failure(
                context,
                read.terminalOutcome().orElseThrow(),
                context.progress,
                OptionalLong.of(entryId),
                "prior-run exact-entry read could not continue recovery");
    }

    private static boolean exactOpen(RunLedgerOpenResultV1 result, KafkaBookKeeperRecoveryRequestV1 request) {
        return result != null
                && result.outcome() == RunLedgerOpenOutcomeV1.OPENED_EXACT
                && result.exactHandle().orElseThrow().equals(request.handle());
    }

    private static OptionalLong exactRecoveryProof(
            ProviderMutationResultV1<RunLedgerRecoveryProofV1> result, KafkaBookKeeperRecoveryRequestV1 request) {
        if (result == null || result.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
            return OptionalLong.empty();
        }
        RunLedgerRecoveryProofV1 proof = result.exactProof().orElseThrow();
        return proof.handle().equals(request.handle())
                ? OptionalLong.of(proof.lastAddConfirmed())
                : OptionalLong.empty();
    }

    private static KafkaBookKeeperRecoveryResultV1 failure(
            KafkaBookKeeperRecoveryRequestV1 request,
            KafkaBookKeeperRecoveryOutcomeV1 outcome,
            KafkaBookKeeperRecoveryProgressV1 progress,
            OptionalLong conflictEntryId,
            String detail) {
        return new KafkaBookKeeperRecoveryResultV1(
                outcome,
                request.kafkaStartOffset(),
                OptionalLong.empty(),
                Optional.empty(),
                progress,
                conflictEntryId,
                detail);
    }

    private static KafkaBookKeeperRecoveryResultV1 failure(
            Context context,
            KafkaBookKeeperRecoveryOutcomeV1 outcome,
            KafkaBookKeeperRecoveryProgressV1 progress,
            OptionalLong conflictEntryId,
            String detail) {
        return new KafkaBookKeeperRecoveryResultV1(
                outcome,
                context.physicalRecoveredEndOffset,
                OptionalLong.empty(),
                Optional.empty(),
                progress,
                conflictEntryId,
                detail);
    }

    private static <T> CompletionStage<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }

    private enum DataAcceptance {
        ACCEPTED,
        CONFLICT,
        ELECTION_BOUNDARY_SPLITS_BATCH
    }

    private static final class Context {
        private final KafkaBookKeeperRecoveryRequestV1 request;
        private final long lastAddConfirmed;
        private final long startedNanos;
        private final Map<Long, ExactLedgerEntryV1> entries = new HashMap<>();
        private final Set<Long> absentEntries = new HashSet<>();
        private KafkaBookKeeperRecoveryProgressV1 progress = KafkaBookKeeperRecoveryProgressV1.ZERO;
        private KafkaProtocolCheckpointStateV1 protocolState;
        private long physicalRecoveredEndOffset;
        private long nextPhysicalOffset;
        private OptionalLong ignoredCheckpointEntryId = OptionalLong.empty();
        private OptionalLong conflictEntryId = OptionalLong.empty();
        private PendingGroup pendingGroup;

        private Context(KafkaBookKeeperRecoveryRequestV1 request, long lastAddConfirmed, long startedNanos) {
            this.request = request;
            this.lastAddConfirmed = lastAddConfirmed;
            this.startedNanos = startedNanos;
            this.physicalRecoveredEndOffset = request.kafkaStartOffset();
            this.nextPhysicalOffset = request.kafkaStartOffset();
        }
    }

    private static final class PendingGroup {
        private final long firstEntryId;
        private final long startOffset;
        private final int memberCount;
        private final Id128 appendGroupId;
        private final Id128 storageAttemptId;
        private final MessageDigest payloadDigest;
        private final List<KafkaAssignedProtocolBatchV1> batches = new ArrayList<>();
        private long nextOffset;

        private PendingGroup(
                long firstEntryId, long startOffset, int memberCount, Id128 appendGroupId, Id128 storageAttemptId) {
            this.firstEntryId = firstEntryId;
            this.startOffset = startOffset;
            this.memberCount = memberCount;
            this.appendGroupId = appendGroupId;
            this.storageAttemptId = storageAttemptId;
            this.nextOffset = startOffset;
            try {
                this.payloadDigest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
        }
    }

    private record RecoveryRead(
            Optional<ExactLedgerEntryV1> entry,
            boolean definitivelyAbsent,
            Optional<KafkaBookKeeperRecoveryOutcomeV1> terminalOutcome) {
        static RecoveryRead found(ExactLedgerEntryV1 entry) {
            return new RecoveryRead(Optional.of(entry), false, Optional.empty());
        }

        static RecoveryRead absent() {
            return new RecoveryRead(Optional.empty(), true, Optional.empty());
        }

        static RecoveryRead terminal(KafkaBookKeeperRecoveryOutcomeV1 outcome) {
            return new RecoveryRead(Optional.empty(), false, Optional.of(outcome));
        }
    }
}
