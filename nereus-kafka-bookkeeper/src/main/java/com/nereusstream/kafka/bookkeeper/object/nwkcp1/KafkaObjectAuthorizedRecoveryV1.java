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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaAssignedProtocolBatchV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaNwg1ObjectPipelineV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Common checkpoint plus only the finite metadata-authorized NWG1 tail; LIST is never an authorization source. */
public final class KafkaObjectAuthorizedRecoveryV1 {
    public record Source(WalRunObjectSession session, Nwg1VerificationContextV1 verification) {}

    @FunctionalInterface
    public interface Resolver {
        Source resolve(KafkaObjectAuthorizationV1.Grant grant);
    }

    public record Result(KafkaProtocolCheckpointStateV1 state, KafkaObjectRecoveredTailV1 tail) {}

    private KafkaObjectAuthorizedRecoveryV1() {}

    public static Result recover(
            KafkaObjectAuthorizationV1 authority,
            KafkaObjectAuthorizationV1.ClosedHistory closed,
            Nbke2RunBindingV1 recoveryBinding,
            Sha256Digest emptyRoot,
            Resolver resolver)
            throws IOException {
        var state = recoverFrom(
                closed,
                recoveryBinding,
                resolver,
                authority.checkpoint(closed),
                closed.head().checkpointEnd(),
                null);
        return new Result(
                state,
                KafkaObjectRecoveredTailV1.authorized(
                        closed, emptyRoot, authority.headKey(closed.head().fence()), state));
    }

    /** Compares complete replayed protocol state; the caller checks selected checkpoint provenance. */
    public static void verifyCheckpointTransfer(
            KafkaObjectAuthorizationV1 authority,
            KafkaObjectAuthorizationV1.ClosedHistory closed,
            KafkaProtocolCheckpointStateV1 original,
            Resolver resolver,
            WalRunObjectSession budgetOwner)
            throws IOException {
        var selected = authority
                .checkpoint(closed)
                .orElseThrow(() -> new IllegalStateException("protocol transfer lacks a selected common checkpoint"));
        var initial = original.vector();
        var target = selected.vector();
        var before = initial.runBinding();
        var after = target.runBinding();
        if (!initial.isAlignedCompoundCheckpoint()
                || !target.isAlignedCompoundCheckpoint()
                || !before.bindingId().equals(after.bindingId())
                || !before.topicIncarnation().equals(after.topicIncarnation())
                || before.partitionId() != after.partitionId()
                || !before.storageEpochId().equals(after.storageEpochId())
                || !before.providerScopeId().equals(after.providerScopeId())
                || initial.recoveryCoveredThrough() > target.recoveryCoveredThrough()) {
            throw new IllegalStateException("protocol transfer substituted or regressed the checkpoint Binding");
        }
        var restored = recoverFrom(
                closed,
                after,
                resolver,
                Optional.of(original),
                initial.recoveryCoveredThrough(),
                Objects.requireNonNull(budgetOwner, "budgetOwner"));
        if (!restored.equals(selected)) {
            throw new IllegalStateException("full protocol state did not transfer to the selected common checkpoint");
        }
    }

    private static KafkaProtocolCheckpointStateV1 recoverFrom(
            KafkaObjectAuthorizationV1.ClosedHistory closed,
            Nbke2RunBindingV1 recoveryBinding,
            Resolver resolver,
            Optional<KafkaProtocolCheckpointStateV1> checkpoint,
            long checkpointEnd,
            WalRunObjectSession budgetOwner)
            throws IOException {
        Objects.requireNonNull(resolver, "resolver");
        var head = closed.head();
        var fence = head.fence();
        if (!head.closed()
                || !recoveryBinding.bindingId().equals(fence.bindingId())
                || !recoveryBinding.topicIncarnation().equals(fence.topicIncarnation())
                || recoveryBinding.partitionId() != fence.partitionId()
                || !recoveryBinding.storageEpochId().equals(fence.storageEpochId())
                || recoveryBinding.creatorOwnerEpoch() != fence.ownerEpoch()
                || recoveryBinding.kafkaLeaderEpoch() != fence.kafkaLeaderEpoch()) {
            throw new IllegalArgumentException("Object recovery context differs from selected closed Owner");
        }
        var producers = checkpoint
                .map(KafkaProtocolCheckpointStateV1::producerState)
                .orElseGet(KafkaCommittedProducerStateV1::empty);
        var transactions = checkpoint
                .map(KafkaProtocolCheckpointStateV1::transactionState)
                .orElseGet(KafkaTransactionStateV1::empty);
        var leaders = checkpoint
                .map(KafkaProtocolCheckpointStateV1::leaderEpochIndex)
                .orElseGet(KafkaLeaderEpochIndexV1::empty);
        long next = checkpointEnd;
        var adapter = new KafkaNativeProtocolBatchAdapterV1();
        for (var grant : head.grants()) {
            var locator = grant.locator();
            if (locator.endOffsetExclusive() <= next) {
                continue;
            }
            if (locator.startOffset() != next) {
                throw new IllegalStateException("Object authorized tail has a gap");
            }
            Source source = resolver.resolve(grant);
            if (!source.session().rootSha256().equals(locator.extent().walRunRootSha())) {
                throw new IllegalArgumentException("Object source resolver substituted the physical Root");
            }
            var batches = new ArrayList<KafkaAssignedProtocolBatchV1>();
            com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer =
                    (frame, payload) -> {
                        byte[] raw = new byte[payload.remaining()];
                        payload.get(raw);
                        try {
                            var batch = KafkaNativeAssignedRecordBatchV1.validate(
                                    KafkaRawAssignedRecordBatchFactsV1.parse(CanonicalBytes.copyOf(raw)));
                            if (batch.baseOffset() != frame.coverage0()
                                    || batch.endOffsetExclusive() != frame.coverage1()
                                    || batch.partitionLeaderEpoch()
                                            != grant.fence().kafkaLeaderEpoch()) {
                                throw new IllegalStateException("Object authorized native frame coverage differs");
                            }
                            batches.add(new KafkaAssignedProtocolBatchV1(
                                    batch.baseOffset(), batch.endOffsetExclusive(), adapter.protocolDelta(batch)));
                        } finally {
                            Arrays.fill(raw, (byte) 0);
                        }
                    };
            var verified = (budgetOwner == null
                            ? source.session()
                                    .readRoutineNwg1AppendUnit(
                                            grant.object(),
                                            source.verification(),
                                            locator.firstDirectoryRow(),
                                            consumer)
                            : source.session()
                                    .readNwg1AppendUnitForTransfer(
                                            grant.object(),
                                            source.verification(),
                                            locator.firstDirectoryRow(),
                                            consumer,
                                            budgetOwner))
                    .appendUnit();
            var commit = new KafkaSpeculativeCommitV1(
                    locator.startOffset(), locator.endOffsetExclusive(), grant.fence(), batches);
            if (verified.firstFrameOrdinal() != locator.firstDirectoryRow()
                    || verified.frameCount() != locator.directoryRowCount()
                    || verified.coverage0() != locator.startOffset()
                    || verified.coverage1() != locator.endOffsetExclusive()
                    || !Sha256Digest.copyOf(verified.assignedPayloadSha256()).equals(grant.payloadSha())
                    || !Arrays.equals(
                            verified.appendCommitSetId(), grant.commitSetId().toByteArray())
                    || !Arrays.equals(
                            KafkaNwg1ObjectPipelineV1.commitSetId(commit),
                            grant.commitSetId().toByteArray())) {
                throw new IllegalStateException("Object recovered commit differs from the exact grant");
            }
            producers = producers.apply(commit);
            transactions = transactions.apply(commit);
            leaders = leaders.observe(grant.fence().kafkaLeaderEpoch(), locator.startOffset());
            next = locator.endOffsetExclusive();
        }
        if (next != head.endOffset()) {
            throw new IllegalStateException("Object recovery did not reach closed prefix");
        }
        var state = new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(recoveryBinding, next, next, next, next),
                producers,
                transactions,
                leaders);
        return state;
    }
}
