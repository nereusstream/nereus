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
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeAppendV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaAssignedProtocolBatchV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaNwg1ObjectPipelineV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1.Grant;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentIdentityV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentLocatorV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.object.control.ObjectWalLeafKeyV1;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.recovery.BoundedObjectTailRecovery;
import com.nereusstream.storage.object.recovery.OwnerOpenRecoveryCoordinator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Builds Kafka state from the closed grant prefix while common recovery verifies every physical append unit. */
public final class KafkaObjectAuthorizedOwnerOpenV1 implements OwnerOpenRecoveryCoordinator.ProtocolRecoveryHandler {
    private final WalRunRootRecord root;
    private final Sha256Digest rootSha;
    private final KafkaObjectAuthorizationV1 authority;
    private final KafkaObjectAuthorizationV1.ClosedHistory closed;
    private final KafkaPartitionFenceV1 origin;
    private final Map<Long, Grant> remaining = new HashMap<>();
    private final KafkaNativeProtocolBatchAdapterV1 adapter = new KafkaNativeProtocolBatchAdapterV1();
    private KafkaCommittedProducerStateV1 producers;
    private KafkaTransactionStateV1 transactions;
    private KafkaLeaderEpochIndexV1 leaderEpochs;
    private long next;
    private KafkaObjectAuthorizedRecoveryV1.Result result;
    private boolean aborted;

    public KafkaObjectAuthorizedOwnerOpenV1(
            WalRunRootRecord root,
            KafkaObjectAuthorizationV1 authority,
            KafkaObjectAuthorizationV1.ClosedHistory closed) {
        this(root, authority, closed, closed.head().fence());
    }

    public KafkaObjectAuthorizedOwnerOpenV1(
            WalRunRootRecord root,
            KafkaObjectAuthorizationV1 authority,
            KafkaObjectAuthorizationV1.ClosedHistory closed,
            KafkaPartitionFenceV1 origin) {
        this.root = Objects.requireNonNull(root, "root");
        this.rootSha = WalRunControlCodec.rootSha256(root);
        this.authority = Objects.requireNonNull(authority, "authority");
        this.closed = Objects.requireNonNull(closed, "closed");
        this.origin = Objects.requireNonNull(origin, "origin");
        var head = closed.head();
        if (!origin.samePartitionIdentity(head.fence())
                || !origin.storageEpochId().equals(head.fence().storageEpochId())
                || origin.bindingGeneration() != head.fence().bindingGeneration()
                || origin.ownerEpoch() < head.fence().ownerEpoch()
                || origin.kafkaLeaderEpoch() < head.fence().kafkaLeaderEpoch()) {
            throw new IllegalArgumentException("closed Object WAL authorization differs from the physical origin");
        }
        var checkpoint = authority.checkpoint(closed);
        next = head.checkpointEnd();
        producers = checkpoint
                .map(KafkaProtocolCheckpointStateV1::producerState)
                .orElseGet(KafkaCommittedProducerStateV1::empty);
        transactions = checkpoint
                .map(KafkaProtocolCheckpointStateV1::transactionState)
                .orElseGet(KafkaTransactionStateV1::empty);
        leaderEpochs = checkpoint
                .map(KafkaProtocolCheckpointStateV1::leaderEpochIndex)
                .orElseGet(KafkaLeaderEpochIndexV1::empty);
        for (var grant : head.grants()) {
            var locator = grant.locator();
            if (locator.extent().walRunRootSha().equals(rootSha)) {
                if (!grant.fence().equals(origin)
                        || locator.extent().laneId() != 0
                        || remaining.put(locator.startOffset(), grant) != null) {
                    throw new IllegalStateException("closed native Object WAL grant differs from its original run");
                }
            } else if (locator.endOffsetExclusive() > next) {
                throw new IllegalStateException("closed Object WAL requires a checkpoint for its retained prefix");
            }
        }
    }

    @Override
    public void stage(
            ProviderResolvedExtentRowV1 row,
            Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
            BoundedObjectTailRecovery.SelectedAppendUnitReader reader)
            throws IOException {
        requireStaging();
        var extent = new KafkaObjectExtentIdentityV1(
                rootSha,
                row.laneId().code(),
                row.laneSequence(),
                row.directoryPrefixEnd(),
                row.bodyLength(),
                row.objectSha256());
        for (var candidate : prefix.directory().appendUnits()) {
            var unit = (Nwg1DirectoryV1.KafkaAppendUnit) candidate;
            var grant = remaining.get(unit.startOffset());
            if (grant == null || !grant.locator().extent().equals(extent)) {
                // Physical persistence never authorizes logical offsets. Still authenticate the entire extent.
                reader.read(unit.firstFrameOrdinal(), (frame, bytes) -> {});
                continue;
            }
            apply(grant, row, unit, reader);
            remaining.remove(unit.startOffset());
        }
    }

    private void apply(
            Grant grant,
            ProviderResolvedExtentRowV1 row,
            Nwg1DirectoryV1.KafkaAppendUnit unit,
            BoundedObjectTailRecovery.SelectedAppendUnitReader reader)
            throws IOException {
        KafkaObjectExtentLocatorV1 locator = grant.locator();
        String exactKey = ObjectWalLeafKeyV1.fromRow(row).fullKey(root.providerConfiguration());
        if (!exactKey.equals(grant.object().key())
                || locator.endOffsetExclusive() != unit.endOffsetExclusive()
                || locator.firstDirectoryRow() != unit.firstFrameOrdinal()
                || locator.directoryRowCount() != unit.frameCount()) {
            throw new IllegalStateException("authenticated NWG1 Directory differs from its exact closed grant");
        }
        var batches = new ArrayList<KafkaAssignedProtocolBatchV1>();
        var verified = reader.read(
                unit.firstFrameOrdinal(),
                (frame, bytes) ->
                        batches.add(protocolBatch(frame, bytes, grant.fence().kafkaLeaderEpoch())));
        var commit = new KafkaSpeculativeCommitV1(
                locator.startOffset(), locator.endOffsetExclusive(), grant.fence(), batches);
        if (!grant.payloadSha().equals(Sha256Digest.copyOf(verified.assignedPayloadSha256()))
                || !Arrays.equals(grant.commitSetId().toByteArray(), verified.appendCommitSetId())
                || !Arrays.equals(grant.commitSetId().toByteArray(), KafkaNwg1ObjectPipelineV1.commitSetId(commit))) {
            throw new IllegalStateException("authenticated NWG1 payload differs from the exact closed grant");
        }
        if (commit.endOffsetExclusive() <= next) {
            return;
        }
        if (commit.startOffset() != next) {
            throw new IllegalStateException("native authorized Object WAL recovery prefix has a gap");
        }
        producers = producers.apply(commit);
        transactions = transactions.apply(commit);
        leaderEpochs = leaderEpochs.observe(grant.fence().kafkaLeaderEpoch(), commit.startOffset());
        next = commit.endOffsetExclusive();
    }

    private KafkaAssignedProtocolBatchV1 protocolBatch(
            Nwg1ObjectReaderV1.VerifiedFrame frame, ByteBuffer borrowed, int expectedEpoch) {
        byte[] raw = new byte[borrowed.remaining()];
        borrowed.get(raw);
        try {
            var batch = KafkaNativeAssignedRecordBatchV1.validate(
                    KafkaRawAssignedRecordBatchFactsV1.parse(CanonicalBytes.copyOf(raw)));
            if (batch.partitionLeaderEpoch() != expectedEpoch
                    || batch.baseOffset() != frame.coverage0()
                    || batch.endOffsetExclusive() != frame.coverage1()) {
                throw new IllegalStateException("recovered Kafka RecordBatch differs from the authenticated range");
            }
            return new KafkaAssignedProtocolBatchV1(
                    batch.baseOffset(), batch.endOffsetExclusive(), adapter.protocolDelta(batch));
        } finally {
            Arrays.fill(raw, (byte) 0);
        }
    }

    @Override
    public void install(WalRunObjectSession recovered) {
        requireStaging();
        if (!recovered.rootSha256().equals(rootSha)
                || !remaining.isEmpty()
                || next != closed.head().endOffset()) {
            throw new IllegalStateException("physical recovery did not verify the full exact Kafka grant prefix");
        }
        var fence = closed.head().fence();
        var state = new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(
                        KafkaObjectNativeAppendV1.runBinding(root, origin), next, next, next, next),
                producers,
                transactions,
                leaderEpochs);
        result = new KafkaObjectAuthorizedRecoveryV1.Result(
                state, KafkaObjectRecoveredTailV1.authorized(closed, rootSha, authority.headKey(fence), state));
    }

    @Override
    public void abort() {
        aborted = true;
        remaining.clear();
        result = null;
    }

    public KafkaObjectAuthorizedRecoveryV1.Result result() {
        if (result == null || aborted) {
            throw new IllegalStateException("native authorized Object WAL recovery was not installed");
        }
        return result;
    }

    private void requireStaging() {
        if (result != null || aborted) {
            throw new IllegalStateException("native authorized Object WAL recovery no longer accepts staged data");
        }
    }
}
