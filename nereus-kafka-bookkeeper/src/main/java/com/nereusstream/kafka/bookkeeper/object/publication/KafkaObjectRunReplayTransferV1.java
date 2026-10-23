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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaAssignedProtocolBatchV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaCheckpointCoverageV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectAuthorizedRecoveryV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ObjectWalLeafKeyV1;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Complete authenticated physical inventory joined to actual selected per-Binding closed histories. */
final class KafkaObjectRunReplayTransferV1 {
    private record Member(KafkaObjectAuthorizationV1.ClosedHistory selected, List<KafkaPartitionFenceV1> owners) {}

    private final KafkaObjectAuthorizationV1 authority;
    private final WalRunRootRecord root;
    private final List<Member> members = new ArrayList<>();
    private final Set<KafkaObjectAuthorizationV1.Grant> expected = new HashSet<>();
    private final Set<KafkaObjectAuthorizationV1.Grant> observed = new HashSet<>();
    private final Set<Member> used = new HashSet<>();
    private final Optional<KafkaProtocolCheckpointStateV1> protocolCheckpoint;

    KafkaObjectRunReplayTransferV1(
            KafkaObjectAuthorizationV1 authority,
            WalRunRootRecord root,
            List<CanonicalBytes> snapshots,
            Optional<KafkaProtocolCheckpointStateV1> protocolCheckpoint) {
        this.protocolCheckpoint = protocolCheckpoint;
        this.authority = authority;
        this.root = root;
        var bindings = new HashSet<String>();
        for (var snapshot : snapshots) {
            var selected = authority.verifyRetirementSnapshot(snapshot);
            requireCheckpoint(authority, root, selected);
            if (!bindings.add(authority.headKey(selected.head().fence()))) {
                throw new IllegalArgumentException("retirement has duplicate Binding snapshots");
            }
            var member = new Member(selected, authority.retirementClosedOwners(selected));
            members.add(member);
            selected.head().grants().stream()
                    .filter(grant ->
                            grant.locator().extent().walRunRootSha().equals(WalRunControlCodec.rootSha256(root)))
                    .forEach(expected::add);
        }
    }

    List<CanonicalBytes> verify(
            WalRunObjectSession session,
            CanonicalControlMetadataStore metadata,
            WalRunSealRecord seal,
            Nwg1VerificationContextV1 verification,
            KafkaObjectAuthorizedRecoveryV1.Resolver resolver)
            throws IOException {
        if (protocolCheckpoint.isPresent()) {
            var state = protocolCheckpoint.orElseThrow();
            var member = protocolMember(KafkaCheckpointCoverageV1.from(state.vector()));
            KafkaObjectAuthorizedRecoveryV1.verifyCheckpointTransfer(
                    authority, member.selected(), state, resolver, session);
            used.add(member);
        }
        session.chargeRecoveryControlMetadata(KafkaObjectRunRetirementCertificateV1.MAX_BYTES);
        var headBytes = metadata.get(seal.finalCheckpointHeadKey()).orElseThrow();
        if (headBytes.length() > KafkaObjectRunRetirementCertificateV1.MAX_BYTES) {
            throw new IllegalStateException("retirement physical checkpoint Head exceeds its control bound");
        }
        if (!Sha256Digest.hash(headBytes).equals(seal.finalCheckpointHeadSha256())) {
            throw new IllegalStateException("retirement physical checkpoint Head differs from Seal");
        }
        var head = WalRunControlCodec.decodeCheckpointHead(headBytes);
        if (!head.rootSha256().equals(seal.root().rootSha256())
                || head.shardRunEpoch() != root.shardRunEpoch()
                || !head.coveredThrough().equals(seal.terminalSequence())) {
            throw new IllegalStateException("retirement physical checkpoint Head substituted its Root/vector");
        }
        try {
            var chain = session.verifyCheckpointChainStreaming(metadata, head, row -> {
                try {
                    verifyExtent(session, row, verification);
                } catch (IOException failure) {
                    throw new UncheckedIOException(failure);
                }
            });
            if (chain.aggregateExtentCount() != seal.aggregateExtentCount()
                    || chain.aggregateCanonicalBodyBytes() != seal.aggregateCanonicalBodyBytes()
                    || !chain.coveredThrough().equals(seal.terminalSequence())) {
                throw new IllegalStateException("retirement physical inventory is not the complete sealed Run");
            }
        } catch (UncheckedIOException failure) {
            throw failure.getCause();
        }
        if (!expected.equals(observed) || used.size() != members.size()) {
            throw new IllegalStateException("retirement member/grant inventory is incomplete or expanded");
        }
        var result = new ArrayList<CanonicalBytes>();
        for (var member : members) {
            var snapshot = KafkaObjectAuthorizationV1.closedSnapshotBytes(member.selected());
            // Selected closed history remains protected through the monotonic Head inheritance chain.
            result.add(snapshot);
        }
        result.sort(Comparator.comparing(value -> Sha256Digest.hash(value).toHex()));
        return List.copyOf(result);
    }

    private Member protocolMember(KafkaCheckpointCoverageV1 coverage) {
        return members.stream()
                .filter(member -> member.owners().stream()
                        .anyMatch(fence -> fence.bindingId().equals(coverage.bindingId())
                                && fence.topicIncarnation().topicId().equals(coverage.topicId())
                                && fence.partitionId() == coverage.partitionId()
                                && fence.storageEpochId().equals(coverage.storageEpochId())
                                && fence.ownerEpoch() == coverage.ownerEpoch()
                                && fence.kafkaLeaderEpoch() == coverage.kafkaLeaderEpoch()))
                .findFirst()
                .orElseThrow(() ->
                        new IllegalStateException("protocol checkpoint member lacks its actual selected closed Owner"));
    }

    static void requireProtocolMember(
            KafkaObjectAuthorizationV1 authority,
            com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadV1 head,
            List<KafkaObjectAuthorizationV1.ClosedHistory> snapshots) {
        var coverage = head.coveredThroughVector().get(0);
        if (coverage.rangeIndexCoveredThrough() != coverage.producerStateCoveredThrough()
                || coverage.rangeIndexCoveredThrough() != coverage.transactionIndexCoveredThrough()
                || coverage.rangeIndexCoveredThrough() != coverage.leaderEpochCoveredThrough()) {
            throw new IllegalStateException("terminal protocol domain did not select one complete compound checkpoint");
        }
        boolean found = snapshots.stream()
                .anyMatch(selected -> selected.head().fence().bindingId().equals(coverage.bindingId())
                        && selected.head().checkpointEnd() >= coverage.rangeIndexCoveredThrough()
                        && authority.retirementClosedOwners(selected).stream()
                                .anyMatch(fence -> fence.bindingId().equals(coverage.bindingId())
                                        && fence.topicIncarnation().topicId().equals(coverage.topicId())
                                        && fence.partitionId() == coverage.partitionId()
                                        && fence.storageEpochId().equals(coverage.storageEpochId())
                                        && fence.ownerEpoch() == coverage.ownerEpoch()
                                        && fence.kafkaLeaderEpoch() == coverage.kafkaLeaderEpoch()));
        if (!found) {
            throw new IllegalStateException("terminal protocol domain lost its complete selected checkpoint member");
        }
    }

    static void requireCheckpoint(
            KafkaObjectAuthorizationV1 authority,
            WalRunRootRecord root,
            KafkaObjectAuthorizationV1.ClosedHistory selected) {
        var head = selected.head();
        var checkpoint = authority
                .checkpoint(selected)
                .orElseThrow(() -> new IllegalStateException("retirement member lacks its selected common checkpoint"));
        var vector = checkpoint.vector();
        var binding = vector.runBinding();
        var fence = head.fence();
        if (!vector.isAlignedCompoundCheckpoint()
                || vector.recoveryCoveredThrough() != head.endOffset()
                || head.checkpointEnd() != head.endOffset()
                || !binding.bindingId().equals(fence.bindingId())
                || !binding.topicIncarnation().equals(fence.topicIncarnation())
                || binding.partitionId() != fence.partitionId()
                || !binding.storageEpochId().equals(fence.storageEpochId())
                || binding.creatorOwnerEpoch() != fence.ownerEpoch()
                || binding.kafkaLeaderEpoch() != fence.kafkaLeaderEpoch()
                || !binding.providerScopeId().equals(root.providerScopeId())) {
            throw new IllegalStateException("retirement member checkpoint does not cover its exact closed Owner");
        }
    }

    private void verifyExtent(
            WalRunObjectSession session, ProviderResolvedExtentRowV1 row, Nwg1VerificationContextV1 verification)
            throws IOException {
        var identity = new ObjectIdentity(
                ObjectWalLeafKeyV1.fromRow(row).fullKey(root.providerConfiguration()),
                row.bodyLength(),
                row.objectSha256());
        try (var ignored = session.acquireRecoveryWorkingSet(Math.multiplyExact(3L, row.directoryPrefixEnd()))) {
            var prefix = session.recoverAndVerifyNwg1Directory(identity, verification);
            if (prefix.directory().appendUnits().isEmpty()) {
                throw new IllegalStateException("retirement extent has no authenticated append units");
            }
            var contexts = new HashSet<Long>();
            prefix.directory().appendUnits().forEach(unit -> contexts.add(unit.contextOrdinal()));
            if (contexts.size() != prefix.directory().bindings().size()) {
                throw new IllegalStateException("retirement directory contains an unaccounted Binding context");
            }
            for (var unit : prefix.directory().appendUnits()) {
                if (!(unit instanceof Nwg1DirectoryV1.KafkaAppendUnit kafka)) {
                    throw new IllegalStateException("Kafka retirement contains a non-Kafka append unit");
                }
                var context = prefix.directory().bindings().get(Math.toIntExact(unit.contextOrdinal()));
                long maximumFrame = 0;
                for (long index = unit.firstFrameOrdinal();
                        index < unit.firstFrameOrdinal() + unit.frameCount();
                        index++) {
                    maximumFrame = Math.max(
                            maximumFrame,
                            prefix.directory()
                                    .frames()
                                    .get(Math.toIntExact(index))
                                    .decodedPayloadBytes());
                }
                try (var payload = session.acquireRecoveryWorkingSet(Math.multiplyExact(3L, maximumFrame))) {
                    var batches = new ArrayList<KafkaAssignedProtocolBatchV1>();
                    var verified = session.recoverAndVerifyNwg1AppendUnit(
                            identity, prefix, verification, unit.firstFrameOrdinal(), (frame, bytes) -> {
                                byte[] raw = new byte[bytes.remaining()];
                                bytes.get(raw);
                                try {
                                    var batch = KafkaNativeAssignedRecordBatchV1.validate(
                                            KafkaRawAssignedRecordBatchFactsV1.parse(CanonicalBytes.copyOf(raw)));
                                    if (batch.baseOffset() != frame.coverage0()
                                            || batch.endOffsetExclusive() != frame.coverage1()
                                            || batch.partitionLeaderEpoch() != kafka.kafkaLeaderEpoch()) {
                                        throw new IllegalStateException(
                                                "retirement native payload differs from Directory");
                                    }
                                    batches.add(new KafkaAssignedProtocolBatchV1(
                                            batch.baseOffset(),
                                            batch.endOffsetExclusive(),
                                            new KafkaNativeProtocolBatchAdapterV1().protocolDelta(batch)));
                                } finally {
                                    Arrays.fill(raw, (byte) 0);
                                }
                            });
                    if (!Arrays.equals(verified.assignedPayloadSha256(), unit.assignedPayloadSha256())) {
                        throw new IllegalStateException(
                                "retirement payload root differs from authenticated append unit");
                    }
                    verifyUnit(identity, context, kafka, batches);
                }
            }
        }
    }

    private void verifyUnit(
            ObjectIdentity identity,
            Nwg1DirectoryV1.BindingContext context,
            Nwg1DirectoryV1.KafkaAppendUnit unit,
            List<KafkaAssignedProtocolBatchV1> batches) {
        Member selected = null;
        KafkaPartitionFenceV1 owner = null;
        for (var member : members) {
            for (var fence : member.owners()) {
                if (matches(context, unit, fence)
                        && Arrays.equals(
                                KafkaNwg1ObjectPipelineV1.commitSetId(new KafkaSpeculativeCommitV1(
                                        unit.startOffset(), unit.endOffsetExclusive(), fence, batches)),
                                unit.appendCommitSetId())) {
                    if (owner != null && !owner.equals(fence)) {
                        throw new IllegalStateException("retirement append unit has ambiguous closed Owner provenance");
                    }
                    selected = member;
                    owner = fence;
                }
            }
        }
        if (selected == null) {
            throw new IllegalStateException("retirement lacks the actual selected closed Owner for a physical member");
        }
        used.add(selected);
        for (var grant : selected.selected().head().grants()) {
            if (grant.fence().equals(owner)
                    && grant.object().equals(identity)
                    && grant.locator().startOffset() == unit.startOffset()
                    && grant.locator().endOffsetExclusive() == unit.endOffsetExclusive()
                    && grant.locator().firstDirectoryRow() == unit.firstFrameOrdinal()
                    && grant.locator().directoryRowCount() == unit.frameCount()
                    && grant.commitSetId().equals(CanonicalBytes.copyOf(unit.appendCommitSetId()))
                    && grant.payloadSha().equals(Sha256Digest.copyOf(unit.assignedPayloadSha256()))) {
                observed.add(grant);
            }
        }
        // Non-selected units are safe only because their exact Owner was closed in the selected immutable chain.
    }

    private static boolean matches(
            Nwg1DirectoryV1.BindingContext context, Nwg1DirectoryV1.KafkaAppendUnit unit, KafkaPartitionFenceV1 fence) {
        return Arrays.equals(
                        context.bindingId(), fence.bindingId().digest().bytes().toByteArray())
                && Arrays.equals(
                        context.storageEpochId(),
                        fence.storageEpochId().digest().bytes().toByteArray())
                && Arrays.equals(
                        context.nti1Bytes(),
                        TopicIncarnationIdentityCodecV1.encode(fence.topicIncarnation())
                                .toByteArray())
                && unit.partitionId() == fence.partitionId()
                && unit.kafkaLeaderEpoch() == fence.kafkaLeaderEpoch();
    }
}
