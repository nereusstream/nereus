/*
 * Copyright 2026 NereusStream.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nereusstream.kafka.bookkeeper.broker;

import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolAppendPlanV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaNwkcp1WalRunContextV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectRecoveredTailV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.ObjectKafkaProtocolCheckpointStoreV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.StorageObjectNwkcp1BackendV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaNwg1ObjectPipelineV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCompletionTrackerV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectNativeStateV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPhysicalFrontiersV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPublicationBridgeV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectStateCodecV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOffsetAssignedAppendV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalCheckpointPublisher;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunRuntime;
import com.nereusstream.storage.object.nwg1.GroupEncodingPlanV1;
import com.nereusstream.storage.object.nwg1.Nwg1CloseReasonV1;
import com.nereusstream.storage.object.nwg1.Nwg1CommitmentsV1;
import com.nereusstream.storage.object.nwg1.Nwg1ConstantsV1;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Synchronous native Kafka append for one already admitted Object WAL Owner, serialized by its partition log. */
public final class KafkaObjectNativeAppendV1 {
    private final WalRunRootRecord root;
    private final Nwg1VerificationContextV1 verification;
    private final KafkaCoherentCommitCoordinatorV1 coordinator;
    private final KafkaObjectCompletionTrackerV1 tracker;
    private final KafkaNwg1ObjectPipelineV1 pipeline;
    private final KafkaObjectPublicationBridgeV1 bridge;
    private final KafkaPartitionFenceV1 fence;
    private final BooleanSupplier current;
    private final Nwg1DirectoryV1.BindingContext binding;
    private final RunOwner runOwner;
    private volatile boolean fenced;

    /** Arms a fresh physical run only after the exact current native owner selected its authorization Head. */
    public static KafkaObjectNativeAppendV1 open(
            WalRunObjectSession fresh,
            CanonicalControlMetadataStore metadata,
            KafkaObjectAuthorizationV1 authority,
            KafkaCoherentCommitCoordinatorV1 coordinator,
            Optional<KafkaObjectRecoveredTailV1> recoveredTail,
            BooleanSupplier current) {
        if (!current.getAsBoolean() || fresh.runtimeState() != WalRunRuntime.State.ADMITTING) {
            throw new IllegalStateException("native Object WAL requires a fresh run and current assignment");
        }
        var snapshot = coordinator.captureObject();
        var fence = snapshot.root().fence();
        var head = authority
                .read(fence)
                .orElseThrow(() -> new IllegalStateException("native Object WAL authorization Head is absent"));
        long end = snapshot.root().frontiers().durableEndOffset();
        if (head.closed() || !head.fence().equals(fence) || head.endOffset() != end) {
            throw new IllegalStateException("native Object WAL authorization and recovered partition cut differ");
        }
        if (recoveredTail
                .map(value -> !value.activeTail().equals(snapshot.activeTail()))
                .orElseGet(() -> !snapshot.activeTail().locators().isEmpty())) {
            throw new IllegalStateException("native Object WAL locator admission differs from the coherent cut");
        }
        var root = fresh.rootRecord();
        var verification = KafkaObjectNativeVerificationV1.context(root, fence);
        var tracker = tracker(root, fence, recoveredTail);
        tracker.bindAuthorization(authority, fence, root.nwg1AdmissionCaps().maxCanonicalBodyBytes());
        var checkpoint = new WalCheckpointPublisher(
                metadata,
                WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch()),
                WalRunControlKeys.checkpointPagePrefix(root.shardId(), root.shardRunEpoch()),
                root,
                WalCheckpointHeadV1.empty(fresh.rootSha256(), root.shardRunEpoch(), fence.ownerEpoch()),
                fresh);
        checkpoint.initializeHead();
        var pipeline = new KafkaNwg1ObjectPipelineV1(
                root,
                fresh,
                verification,
                new KafkaObjectPhysicalFrontiersV1(fresh.rootSha256()),
                tracker,
                checkpoint,
                authority);
        var bridge = new KafkaObjectPublicationBridgeV1(
                KafkaObjectAuthorizationV1.binding(fence), end, tracker, coordinator);
        return new KafkaObjectNativeAppendV1(
                verification,
                coordinator,
                tracker,
                pipeline,
                bridge,
                current,
                new RunOwner(fresh, metadata, authority, checkpoint));
    }

    private static KafkaObjectCompletionTrackerV1 tracker(
            WalRunRootRecord root, KafkaPartitionFenceV1 fence, Optional<KafkaObjectRecoveredTailV1> recovered) {
        int slots = root.checkpointPolicy().maxUncheckpointedExtents();
        long extents = Math.multiplyExact(
                root.bounds().maxExtentCount(), root.recoveryEnvelope().maxLiveRoots());
        long locatorBytes = Math.multiplyExact(extents, KafkaObjectStateCodecV1.exactLocatorBytes());
        return recovered
                .map(value -> new KafkaObjectCompletionTrackerV1(slots, locatorBytes, fence.ownerEpoch(), 0, value))
                .orElseGet(() -> new KafkaObjectCompletionTrackerV1(slots, locatorBytes, fence.ownerEpoch()));
    }

    private KafkaObjectNativeAppendV1(
            Nwg1VerificationContextV1 verification,
            KafkaCoherentCommitCoordinatorV1 coordinator,
            KafkaObjectCompletionTrackerV1 tracker,
            KafkaNwg1ObjectPipelineV1 pipeline,
            KafkaObjectPublicationBridgeV1 bridge,
            BooleanSupplier current,
            RunOwner runOwner) {
        this.runOwner = runOwner;
        this.root = runOwner.session().rootRecord();
        this.verification = Objects.requireNonNull(verification, "verification");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
        this.bridge = Objects.requireNonNull(bridge, "bridge");
        this.current = Objects.requireNonNull(current, "current");
        fence = coordinator.captureObject().root().fence();
        binding = new Nwg1DirectoryV1.BindingContext(
                fence.bindingId().digest().bytes().toByteArray(),
                fence.storageEpochId().digest().bytes().toByteArray(),
                Nwg1CommitmentsV1.ownerFence(
                        1, 1, KafkaObjectNativeVerificationV1.witness(fence).toByteArray()),
                TopicIncarnationIdentityCodecV1.encode(fence.topicIncarnation()).toByteArray(),
                1,
                1,
                1,
                1,
                1,
                1);
    }

    public Admission admit(List<Integer> maximumFrameBytes) {
        requireCurrent();
        var limits = List.copyOf(maximumFrameBytes);
        long body = GroupEncodingPlanV1.kafkaSingleUncompressedBodyLimit(
                binding.nti1Bytes().length, limits, root.nwg1AdmissionCaps());
        var physical = pipeline.reservePhysicalBeforePosition(body);
        try {
            return new Admission(this, limits, body, tracker.reserveBeforePosition(physical));
        } catch (RuntimeException failure) {
            physical.cancelUnused();
            throw failure;
        }
    }

    public int maximumFrameBytes() {
        return root.nwg1AdmissionCaps().maxDecodedFrameBytes();
    }

    public KafkaObjectCoherentProtocolSnapshotV1 append(
            Admission admitted,
            List<KafkaNativeAssignedRecordBatchV1> assigned,
            long nowMillis,
            Consumer<KafkaObjectCoherentProtocolSnapshotV1> acceptPublished) {
        Objects.requireNonNull(acceptPublished, "acceptPublished");
        requireCurrent();
        var batches = List.copyOf(assigned);
        admitted.require(this, batches);
        var adapter = new KafkaNativeProtocolBatchAdapterV1();
        var protocol = new KafkaProtocolAppendPlanV1(
                fence, batches.stream().map(adapter::protocolDelta).toList());
        long start = batches.get(0).baseOffset();
        long end = batches.get(batches.size() - 1).endOffsetExclusive();
        var hooks = coordinator.protocolHooks(protocol);
        hooks.validateBeforeOffsetAssignment();
        var commit = KafkaSpeculativeCommitV1.assign(protocol, start, end);
        try {
            hooks.prepareAfterOffsetAssignment(new KafkaOffsetAssignedAppendV1(start, end, ignored -> {
                throw new IllegalStateException("Object WAL cannot build a BK append group");
            }));
            var ticket = admitted.assign(commit);
            var plan = plan(batches, commit, ticket);
            if (plan.requireAdmission(root.nwg1AdmissionCaps()).canonicalBodyBytes() > admitted.bodyLimit) {
                throw new IllegalStateException("assigned Object WAL body exceeds its original reservation");
            }
            var before = coordinator.captureObject();
            var transactions = before.transactionState().apply(commit);
            var nativeState = new KafkaObjectNativeStateV1(
                    before.committedProducerState().apply(commit),
                    transactions,
                    before.leaderEpochIndex().observe(fence.kafkaLeaderEpoch(), start),
                    end,
                    Math.min(end, transactions.firstUnstableOffset(end).orElse(end)));
            requireCurrent();
            pipeline.writeResolveAndInstall(plan, ticket, commit, nativeState, nowMillis);
            requireCurrent();
            return bridge.publishNext(() -> {
                        requireCurrent();
                        var selected = coordinator.captureObject();
                        if (selected.root().frontiers().durableEndOffset() != end) {
                            throw new IllegalStateException("Object WAL published another append range");
                        }
                        acceptPublished.accept(selected);
                    })
                    .orElseThrow(
                            () -> new IllegalStateException("durably authorized Object WAL append was not selected"));
        } catch (RuntimeException | Error failure) {
            fence();
            throw failure;
        }
    }

    private GroupEncodingPlanV1 plan(
            List<KafkaNativeAssignedRecordBatchV1> batches,
            KafkaSpeculativeCommitV1 commit,
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket) {
        var frames = new ArrayList<GroupEncodingPlanV1.PlannedFrame>(batches.size());
        var payload = sha256();
        long bytes = 0;
        long next = commit.startOffset();
        for (var batch : batches) {
            if (batch.baseOffset() != next || batch.partitionLeaderEpoch() != fence.kafkaLeaderEpoch()) {
                throw new IllegalArgumentException("native Object WAL frames differ from their assigned offset/epoch");
            }
            byte[] raw = batch.rawAssignedRecordBatch().toByteArray();
            bytes = Math.addExact(bytes, raw.length);
            payload.update(raw);
            frames.add(new GroupEncodingPlanV1.PlannedFrame(0, raw, raw, next, batch.endOffsetExclusive(), 0, 0));
            next = batch.endOffsetExclusive();
        }
        var unit = new Nwg1DirectoryV1.KafkaAppendUnit(
                0,
                0,
                frames.size(),
                fence.partitionId(),
                fence.kafkaLeaderEpoch(),
                commit.startOffset(),
                commit.endOffsetExclusive(),
                KafkaNwg1ObjectPipelineV1.commitSetId(commit),
                KafkaNwg1ObjectPipelineV1.storageAttemptId(ticket),
                payload.digest());
        return new GroupEncodingPlanV1(
                Nwg1ConstantsV1.PROTOCOL_KAFKA,
                root.shardId(),
                root.shardRunEpoch(),
                0,
                root.formatContract().packingPolicyCatalogVersion(),
                bytes,
                0,
                0,
                Nwg1CloseReasonV1.EXPLICIT_FLUSH.code(),
                Nwg1CommitmentsV1.protocolCell(verification.exactNpc1()),
                verification.cellProviderScopeId(),
                verification.walRunRootSha256(),
                Nwg1CommitmentsV1.wrappedEnvelope(verification.envelope()),
                List.of(binding),
                List.of(unit),
                frames);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
    }

    public void fence() {
        fenced = true;
    }

    /** Stop the original background publisher before releasing its WalRun session. */
    public void closeCheckpointAfterDrain() {
        fence();
        runOwner.checkpoint().closeAfterDrain();
    }

    private record RunOwner(
            WalRunObjectSession session,
            CanonicalControlMetadataStore metadata,
            KafkaObjectAuthorizationV1 authority,
            WalCheckpointPublisher checkpoint) {}

    public record ClosedRun(WalRunPredecessor predecessor, KafkaObjectAuthorizationV1.ClosedHistory history) {}

    /**
     * Planned handoff only: the native lifecycle has stopped and drained request admission while the assignment is
     * still current. Close of an unexpected predecessor uses durable recovery instead.
     */
    public ClosedRun sealDrained() {
        if (!current.getAsBoolean()) {
            throw new IllegalStateException("native Object WAL cannot seal under a superseded assignment");
        }
        fence();
        var session = runOwner.session();
        var authority = runOwner.authority();
        var metadata = runOwner.metadata();
        var run = runBinding(root, fence);
        authority.checkpoint(session.rootSha256(), run, coordinator);
        var snapshot = coordinator.captureObject();
        long end = snapshot.root().frontiers().durableEndOffset();
        var checkpoint = new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(run, end, end, end, end),
                snapshot.committedProducerState(),
                snapshot.transactionState(),
                snapshot.leaderEpochIndex());
        String prefix = checkpointPrefix(root);
        var store = new ObjectKafkaProtocolCheckpointStoreV1(
                prefix,
                new KafkaNwkcp1WalRunContextV1(session.rootSha256(), run),
                fence.ownerEpoch(),
                new StorageObjectNwkcp1BackendV1(session, metadata, prefix));
        store.publish(checkpoint).toCompletableFuture().join();
        session.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
        session.sealRuntime();
        var physical = pipeline.publishPhysicalSeal(new WalRunLifecycleManager(metadata));
        var terminal = store.terminalize(physical).toCompletableFuture().join();
        var history = authority.close(fence);
        if (authority.checkpoint(history).filter(checkpoint::equals).isEmpty()) {
            throw new IllegalStateException("Object WAL handoff selected a different terminal protocol cut");
        }
        return new ClosedRun(
                new WalRunPredecessor(
                        physical.root(), physical.sealKey(), physical.sealSha256(), Optional.of(terminal)),
                history);
    }

    public static String checkpointPrefix(WalRunRootRecord root) {
        return root.providerConfiguration().exclusiveNamespacePrefix() + "/runs/" + root.shardRunEpoch();
    }

    public static Nbke2RunBindingV1 runBinding(WalRunRootRecord root, KafkaPartitionFenceV1 fence) {
        return new Nbke2RunBindingV1(
                fence.bindingId(),
                fence.topicIncarnation(),
                fence.partitionId(),
                fence.storageEpochId(),
                fence.ownerEpoch(),
                fence.kafkaLeaderEpoch(),
                root.providerScopeId(),
                new StorageRunId(root.walRunSessionId()));
    }

    private void requireCurrent() {
        if (fenced || !current.getAsBoolean()) {
            throw new IllegalStateException("native Object WAL append Owner is fenced");
        }
    }

    public static final class Admission implements AutoCloseable {
        private final KafkaObjectNativeAppendV1 owner;
        private final List<Integer> limits;
        private final long bodyLimit;
        private final KafkaObjectCompletionTrackerV1.Reservation reserved;
        private boolean assigned;
        private boolean closed;

        private Admission(
                KafkaObjectNativeAppendV1 owner,
                List<Integer> limits,
                long bodyLimit,
                KafkaObjectCompletionTrackerV1.Reservation reserved) {
            this.owner = owner;
            this.limits = limits;
            this.bodyLimit = bodyLimit;
            this.reserved = reserved;
        }

        private void require(KafkaObjectNativeAppendV1 expected, List<KafkaNativeAssignedRecordBatchV1> batches) {
            if (owner != expected || assigned || closed || batches.size() != limits.size()) {
                throw new IllegalArgumentException("native Object WAL append differs from its original admission");
            }
            for (int i = 0; i < batches.size(); i++) {
                if (batches.get(i).rawAssignedRecordBatch().length() > limits.get(i)) {
                    throw new IllegalArgumentException("assigned Kafka frame exceeds its pre-position cap");
                }
            }
        }

        private KafkaObjectCompletionTrackerV1.AssignedTicket assign(KafkaSpeculativeCommitV1 commit) {
            var ticket = owner.tracker.assignPosition(reserved, commit);
            assigned = true;
            return ticket;
        }

        @Override
        public void close() {
            if (!closed && !assigned) {
                owner.tracker.cancelBeforePosition(reserved);
            }
            closed = true;
        }
    }
}
