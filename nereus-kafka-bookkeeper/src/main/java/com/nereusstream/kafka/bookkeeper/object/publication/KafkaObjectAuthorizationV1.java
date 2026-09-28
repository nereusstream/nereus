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
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1CodecV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1ObjectV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Single-key, per-Binding durable grant authority. Provider completion alone never grants a commit.
 * Immutable closed snapshots and common checkpoints are prewritten; only the Head CAS selects them.
 * The bounded source index remains selected across Owner and physical-run changes until M4 retirement.
 */
public final class KafkaObjectAuthorizationV1 {
    private static final int MAX_VALUE_BYTES = 1024 * 1024;
    private static final int MAGIC = 0x4e4f4131;

    public record Bounds(int maxSources, int maxUncoveredUnits, long maxUncoveredBytes, int maxClosedOwners) {
        public Bounds {
            if (maxSources <= 0
                    || maxSources > 2048
                    || maxUncoveredUnits <= 0
                    || maxUncoveredUnits > maxSources
                    || maxUncoveredBytes <= 0
                    || maxClosedOwners <= 0
                    || maxClosedOwners > 65536) {
                throw new IllegalArgumentException("Object authorization bounds are outside their domain");
            }
        }
    }

    /** Exact full commit-set descriptor; its Object is already independently authenticated by the pipeline. */
    public record Grant(
            KafkaPartitionFenceV1 fence,
            ObjectIdentity object,
            KafkaObjectExtentLocatorV1 locator,
            CanonicalBytes commitSetId,
            Sha256Digest payloadSha) {
        public Grant {
            Objects.requireNonNull(fence, "fence");
            Objects.requireNonNull(object, "object");
            Objects.requireNonNull(locator, "locator");
            Objects.requireNonNull(commitSetId, "commitSetId");
            Objects.requireNonNull(payloadSha, "payloadSha");
            if (!binding(fence).equals(locator.binding())
                    || commitSetId.length() != 16
                    || payloadSha.isZero()
                    || object.bodyLength() != locator.extent().bodyLength()
                    || !object.bodySha256().equals(locator.extent().bodySha())) {
                throw new IllegalArgumentException("Object authorization descriptor differs from its exact commit");
            }
        }
    }

    public record Head(
            long generation,
            KafkaPartitionFenceV1 fence,
            boolean closed,
            long startOffset,
            long endOffset,
            List<Grant> grants,
            long checkpointEnd,
            Optional<Sha256Digest> checkpoint,
            Optional<Sha256Digest> previousClosed,
            int closedOwners,
            Bounds bounds) {
        public Head {
            Objects.requireNonNull(fence, "fence");
            Objects.requireNonNull(bounds, "bounds");
            grants = List.copyOf(grants);
            checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
            previousClosed = Objects.requireNonNull(previousClosed, "previousClosed");
            if (generation <= 0
                    || startOffset < 0
                    || checkpointEnd < startOffset
                    || endOffset < checkpointEnd
                    || checkpoint.filter(Sha256Digest::isZero).isPresent()
                    || previousClosed.filter(Sha256Digest::isZero).isPresent()
                    || closedOwners < 0
                    || checkpoint.isEmpty() && checkpointEnd != startOffset) {
                throw new IllegalArgumentException("Object authorization Head is outside its domain");
            }
            long next = startOffset;
            for (Grant grant : grants) {
                if (!binding(fence).equals(grant.locator().binding())
                        || grant.locator().startOffset() != next
                        || !grant.fence().topicIncarnation().equals(fence.topicIncarnation())
                        || grant.fence().bindingGeneration() != fence.bindingGeneration()
                        || grant.fence().kafkaLeaderEpoch() > fence.kafkaLeaderEpoch()
                        || grant.fence().ownerEpoch() > fence.ownerEpoch()) {
                    throw new IllegalArgumentException("Object authorization sources have a gap or foreign Owner");
                }
                next = grant.locator().endOffsetExclusive();
            }
            if (next != endOffset
                    || grants.stream()
                            .anyMatch(g -> g.locator().startOffset() < checkpointEnd
                                    && g.locator().endOffsetExclusive() > checkpointEnd)) {
                throw new IllegalArgumentException("Object authorization prefix or checkpoint cuts a commit set");
            }
        }
    }

    public static final class ClosedHistory {
        private final Head head;
        private final CanonicalBytes bytes;

        private ClosedHistory(Head head, CanonicalBytes bytes) {
            this.head = head;
            this.bytes = bytes;
        }

        public Head head() {
            return head;
        }

        public Sha256Digest digest() {
            return Sha256Digest.hash(bytes);
        }
    }

    /** An unforgeable exact grant receipt, required by both tracker and coherent publication. */
    public static final class Proof {
        private final Grant grant;
        private final com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1 commit;

        private Proof(Grant grant, com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1 commit) {
            this.grant = grant;
            this.commit = commit;
        }

        public void require(
                com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1 expected,
                KafkaObjectExtentLocatorV1 locator) {
            if (!commit.equals(expected) || !grant.locator().equals(locator)) {
                throw new IllegalArgumentException("Object grant receipt differs from the complete publication cut");
            }
        }
    }

    public static final class Fenced extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Fenced(String message) {
            super(message);
        }
    }

    public static final class Unknown extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Unknown(String message) {
            super(message);
        }
    }

    static final class Reservation implements AutoCloseable {
        private final KafkaObjectAuthorizationV1 authority;
        private final String key;
        private final long bytes;
        private final long ownerEpoch;
        private boolean released;

        private Reservation(KafkaObjectAuthorizationV1 authority, String key, long bytes, long ownerEpoch) {
            this.authority = authority;
            this.key = key;
            this.bytes = bytes;
            this.ownerEpoch = ownerEpoch;
        }

        @Override
        public void close() {
            authority.release(this);
        }
    }

    synchronized Reservation reserve(KafkaPartitionFenceV1 fence, long bodyBytes) {
        Head head = read(fence).orElseThrow(() -> new Fenced("Object Owner absent"));
        requireOwner(head, fence);
        String key = headKey(fence);
        long[] reserved = reservations.getOrDefault(key, new long[2]);
        requireCapacity(head, Math.addExact(bodyBytes, reserved[1]));
        long uncovered = head.grants().stream()
                .filter(g -> g.locator().endOffsetExclusive() > head.checkpointEnd())
                .count();
        if (reserved[0] + uncovered >= bounds.maxUncoveredUnits()
                || reserved[0] + head.grants().size() >= bounds.maxSources()) {
            throw new IllegalStateException("Object pre-position authorization capacity is exhausted");
        }
        reservations.putIfAbsent(key, reserved);
        reserved[0]++;
        reserved[1] = Math.addExact(reserved[1], bodyBytes);
        var reservation = new Reservation(this, key, bodyBytes, fence.ownerEpoch());
        activeReservations.add(reservation);
        return reservation;
    }

    private synchronized void release(Reservation reservation) {
        if (!reservation.released) {
            long[] reserved = reservations.get(reservation.key);
            reserved[0]--;
            reserved[1] -= reservation.bytes;
            reservation.released = true;
            if (reserved[0] == 0) {
                reservations.remove(reservation.key);
            }
            activeReservations.remove(reservation);
        }
    }

    private record Attempt(Grant grant, CanonicalBytes expected, CanonicalBytes candidate) {}

    private final CanonicalControlMetadataStore metadata;
    private final String prefix;
    private final Bounds bounds;
    // Retain the same exact candidate through UNKNOWN. Capacity is bounded by the selected source index.
    private final Map<Grant, Attempt> attempts = new HashMap<>();
    private final Map<String, long[]> reservations = new HashMap<>();
    private final java.util.Set<Reservation> activeReservations = new java.util.HashSet<>();

    public KafkaObjectAuthorizationV1(CanonicalControlMetadataStore metadata, int shardId, Bounds bounds) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        if (shardId < 0) {
            throw new IllegalArgumentException("negative authorization shard");
        }
        this.prefix = String.format(java.util.Locale.ROOT, "v2/object-wal/shards/%010d/authorize/", shardId);
        this.bounds = Objects.requireNonNull(bounds, "bounds");
    }

    public Optional<Head> read(KafkaPartitionFenceV1 fence) {
        return metadata.get(headKey(fence)).map(this::decodeChecked);
    }

    /** Caller supplies the current native Controller fence, never a node-local lease or a physical-run epoch. */
    public Head open(KafkaPartitionFenceV1 fence, long startOffset, Optional<ClosedHistory> predecessor) {
        Objects.requireNonNull(predecessor, "predecessor");
        Head next;
        Optional<CanonicalBytes> expected;
        if (predecessor.isEmpty()) {
            expected = Optional.empty();
            next = new Head(
                    1,
                    fence,
                    false,
                    startOffset,
                    startOffset,
                    List.of(),
                    startOffset,
                    Optional.empty(),
                    Optional.empty(),
                    0,
                    bounds);
        } else {
            ClosedHistory closed = predecessor.orElseThrow();
            Head old = closed.head;
            if (!old.closed()
                    || !binding(old.fence()).equals(binding(fence))
                    || startOffset != old.endOffset()
                    || !old.fence().topicIncarnation().equals(fence.topicIncarnation())
                    || old.fence().bindingGeneration() != fence.bindingGeneration()
                    || fence.ownerEpoch() <= old.fence().ownerEpoch()
                    || fence.kafkaLeaderEpoch() < old.fence().kafkaLeaderEpoch()) {
                throw new Fenced("Object Owner successor differs from the exact closed predecessor");
            }
            expected = Optional.of(closed.bytes);
            next = new Head(
                    Math.addExact(old.generation(), 1),
                    fence,
                    false,
                    old.startOffset(),
                    old.endOffset(),
                    old.grants(),
                    old.checkpointEnd(),
                    old.checkpoint(),
                    Optional.of(closed.digest()),
                    old.closedOwners(),
                    old.bounds());
        }
        CanonicalBytes candidate = encode(next);
        if (metadata.compareAndSet(headKey(fence), expected, candidate) != ControlMutationOutcome.APPLIED
                && !metadata.get(headKey(fence)).equals(Optional.of(candidate))) {
            throw new Unknown("Object Owner open did not resolve to the exact candidate");
        }
        return next;
    }

    /** Close and authorize race on this same key. Closing selects a finite immutable complete legal prefix. */
    public ClosedHistory close(KafkaPartitionFenceV1 fence) {
        // Bounded retry only for real preceding grants/checkpoint updates from this Owner.
        for (int retry = 0; retry <= bounds.maxUncoveredUnits(); retry++) {
            CanonicalBytes before = metadata.get(headKey(fence)).orElseThrow(() -> new Fenced("Object Owner absent"));
            Head old = decodeChecked(before);
            if (!old.fence().equals(fence)) {
                return readClosed(fence);
            }
            if (old.closed()) {
                closedLocal(old);
                return new ClosedHistory(old, before);
            }
            if (old.closedOwners() >= bounds.maxClosedOwners()) {
                throw new IllegalStateException("Object closed-history bound requires protected retirement");
            }
            Head closed = new Head(
                    Math.addExact(old.generation(), 1),
                    fence,
                    true,
                    old.startOffset(),
                    old.endOffset(),
                    old.grants(),
                    old.checkpointEnd(),
                    old.checkpoint(),
                    old.previousClosed(),
                    old.closedOwners() + 1,
                    old.bounds());
            CanonicalBytes candidate = encode(closed);
            putExact(recordKey(fence, Sha256Digest.hash(candidate)), candidate);
            ControlMutationOutcome result = metadata.compareAndSet(headKey(fence), Optional.of(before), candidate);
            if (result == ControlMutationOutcome.APPLIED
                    || metadata.get(headKey(fence)).equals(Optional.of(candidate))) {
                closedLocal(closed);
                return new ClosedHistory(closed, candidate);
            }
            if (result == ControlMutationOutcome.RESPONSE_UNKNOWN) {
                throw new Unknown("Object close response is unknown; retry the same Owner close");
            }
        }
        throw new Unknown("Object close could not fix a bounded prefix");
    }

    public ClosedHistory readClosed(KafkaPartitionFenceV1 fence) {
        CanonicalBytes bytes = metadata.get(headKey(fence)).orElseThrow();
        Head head = decodeChecked(bytes);
        for (int index = 0; index <= bounds.maxClosedOwners(); index++) {
            if (head.closed() && head.fence().equals(fence)) {
                if (!metadata.get(recordKey(fence, Sha256Digest.hash(bytes))).equals(Optional.of(bytes))) {
                    throw new IllegalStateException("selected Object closed history is missing");
                }
                closedLocal(head);
                return new ClosedHistory(head, bytes);
            }
            if (head.previousClosed().isEmpty() || head.fence().ownerEpoch() < fence.ownerEpoch()) {
                throw new Fenced("Object Owner has no selected closed snapshot");
            }
            var sha = head.previousClosed().orElseThrow();
            bytes = metadata.get(recordKey(fence, sha)).orElseThrow();
            if (!Sha256Digest.hash(bytes).equals(sha)) {
                throw new IllegalStateException("Object closed lineage SHA differs");
            }
            head = decodeChecked(bytes);
            if (!head.closed() || !binding(head.fence()).equals(binding(fence))) {
                throw new IllegalStateException("Object closed lineage substituted its scope");
            }
        }
        throw new IllegalStateException("Object closed lineage exceeds its persisted bound");
    }

    private synchronized void closedLocal(Head head) {
        String key = headKey(head.fence());
        attempts.keySet()
                .removeIf(g -> headKey(g.fence()).equals(key)
                        && g.fence().ownerEpoch() <= head.fence().ownerEpoch());
        for (var reservation : List.copyOf(activeReservations)) {
            if (reservation.key.equals(key)
                    && reservation.ownerEpoch <= head.fence().ownerEpoch()) {
                release(reservation);
            }
        }
    }

    /** Capacity check before allocating Kafka offsets; rollover/open never clears this cumulative debt. */
    public void requireCapacityBeforePosition(KafkaPartitionFenceV1 fence, long candidateBodyBytes) {
        Head head = read(fence).orElseThrow(() -> new Fenced("Object Owner absent"));
        requireOwner(head, fence);
        requireCapacity(head, candidateBodyBytes);
    }

    synchronized Proof authorize(
            ObjectIdentity object,
            KafkaVerifiedNwg1CommitV1 verified,
            com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1 commit) {
        Grant grant = new Grant(
                commit.expectedFence(),
                object,
                verified.locator(),
                CanonicalBytes.copyOf(KafkaNwg1ObjectPipelineV1.commitSetId(commit)),
                verified.assignedPayloadSha());
        Attempt attempt = attempts.get(grant);
        Head selected = read(grant.fence()).orElseThrow(() -> new Fenced("Object Owner absent"));
        if (selected.grants().contains(grant)) {
            attempts.remove(grant);
            return new Proof(grant, commit);
        }
        if (attempt == null) {
            requireOwner(selected, grant.fence());
            if (selected.endOffset() != grant.locator().startOffset()) {
                throw new IllegalStateException(
                        "Object authorization does not extend the exact continuous predecessor");
            }
            requireCapacity(selected, object.bodyLength());
            if (attempts.size() >= bounds.maxSources()) {
                throw new IllegalStateException("Object UNKNOWN ring is full");
            }
            var grants = new ArrayList<>(selected.grants());
            grants.add(grant);
            Head next = new Head(
                    Math.addExact(selected.generation(), 1),
                    selected.fence(),
                    false,
                    selected.startOffset(),
                    grant.locator().endOffsetExclusive(),
                    grants,
                    selected.checkpointEnd(),
                    selected.checkpoint(),
                    selected.previousClosed(),
                    selected.closedOwners(),
                    selected.bounds());
            attempt = new Attempt(grant, encode(selected), encode(next));
            attempts.put(grant, attempt);
        }
        ControlMutationOutcome result =
                metadata.compareAndSet(headKey(grant.fence()), Optional.of(attempt.expected()), attempt.candidate());
        if (result == ControlMutationOutcome.APPLIED) {
            attempts.remove(grant);
            return new Proof(grant, commit);
        }
        Head after = read(grant.fence()).orElseThrow(() -> new Unknown("Object grant reconciliation has no Head"));
        if (after.grants().contains(grant)) {
            attempts.remove(grant);
            return new Proof(grant, commit);
        }
        if (result == ControlMutationOutcome.DEFINITIVE_CONFLICT
                || !encode(after).equals(attempt.expected())) {
            attempts.remove(grant);
            throw new Fenced("Object grant was not selected before Owner close or another predecessor");
        }
        throw new Unknown("Object grant response remained unknown; retain the exact candidate");
    }

    /** Select a complete common state at the exact authorization end; never release debt on a partial vector. */
    public Head checkpoint(
            Sha256Digest walRoot,
            com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1 run,
            com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1 coordinator) {
        var snapshot = coordinator.captureObject();
        var fence = snapshot.root().fence();
        Head selected = read(fence).orElseThrow();
        if (!snapshot.activeTail()
                        .locators()
                        .equals(selected.grants().stream().map(Grant::locator).toList())
                || snapshot.root().frontiers().durableEndOffset() != selected.endOffset()
                || snapshot.root().frontiers().highWatermark() != selected.endOffset()) {
            throw new IllegalArgumentException("Object checkpoint snapshot lacks the complete selected source prefix");
        }
        long end = selected.endOffset();
        var state = new KafkaProtocolCheckpointStateV1(
                new com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1(
                        run, end, end, end, end),
                snapshot.committedProducerState(),
                snapshot.transactionState(),
                snapshot.leaderEpochIndex());
        return selectCheckpoint(fence, walRoot, state);
    }

    private Head selectCheckpoint(
            KafkaPartitionFenceV1 fence, Sha256Digest walRoot, KafkaProtocolCheckpointStateV1 state) {
        Head old = read(fence).orElseThrow();
        requireOwner(old, fence);
        var run = state.vector().runBinding();
        if (!state.vector().isAlignedCompoundCheckpoint()
                || state.vector().recoveryCoveredThrough() != old.endOffset()
                || !run.bindingId().equals(fence.bindingId())
                || !run.topicIncarnation().equals(fence.topicIncarnation())
                || run.partitionId() != fence.partitionId()
                || !run.storageEpochId().equals(fence.storageEpochId())
                || run.creatorOwnerEpoch() != fence.ownerEpoch()
                || run.kafkaLeaderEpoch() != fence.kafkaLeaderEpoch()) {
            throw new IllegalArgumentException("Object checkpoint does not cover the exact common authorized cut");
        }
        CanonicalBytes body = Nwkcp1CodecV1.encode("authorization", new Nwkcp1ObjectV1(walRoot, List.of(state)))
                .body();
        Sha256Digest sha = Sha256Digest.hash(body);
        putExact(recordKey(fence, sha), body);
        Head next = new Head(
                Math.addExact(old.generation(), 1),
                fence,
                false,
                old.startOffset(),
                old.endOffset(),
                old.grants(),
                old.endOffset(),
                Optional.of(sha),
                old.previousClosed(),
                old.closedOwners(),
                old.bounds());
        CanonicalBytes candidate = encode(next);
        ControlMutationOutcome result = metadata.compareAndSet(headKey(fence), Optional.of(encode(old)), candidate);
        if (result != ControlMutationOutcome.APPLIED
                && !metadata.get(headKey(fence)).equals(Optional.of(candidate))) {
            throw new Unknown("Object common checkpoint selection did not resolve; debt stays charged");
        }
        return next;
    }

    public Optional<KafkaProtocolCheckpointStateV1> checkpoint(ClosedHistory closed) {
        if (closed.head.checkpoint().isEmpty()) {
            return Optional.empty();
        }
        Sha256Digest sha = closed.head.checkpoint().orElseThrow();
        CanonicalBytes body = metadata.get(recordKey(closed.head.fence(), sha)).orElseThrow();
        if (!Sha256Digest.hash(body).equals(sha)) {
            throw new IllegalStateException("Object checkpoint identity differs");
        }
        var object = Nwkcp1CodecV1.decode(body);
        if (object.rows().size() != 1
                || object.rows().get(0).vector().recoveryCoveredThrough() != closed.head.checkpointEnd()) {
            throw new IllegalStateException("Object checkpoint differs from selected common coverage");
        }
        return Optional.of(object.rows().get(0));
    }

    public String headKey(KafkaPartitionFenceV1 fence) {
        return prefix + scope(fence) + "/head";
    }

    private static String scope(KafkaPartitionFenceV1 fence) {
        return Sha256Digest.hash(CanonicalBytes.copyOf((fence.bindingId()
                                        .digest()
                                        .toHex() + ":"
                                + fence.topicIncarnation().topicId().value().toHex() + ":" + fence.partitionId() + ":"
                                + fence.storageEpochId().digest().toHex())
                        .getBytes(StandardCharsets.UTF_8)))
                .toHex();
    }

    private String recordKey(KafkaPartitionFenceV1 fence, Sha256Digest digest) {
        return prefix + scope(fence) + "/records/" + digest.toHex();
    }

    private void putExact(String key, CanonicalBytes bytes) {
        if (bytes.length() > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("Object authority value exceeds cap");
        }
        ControlMutationOutcome result = metadata.putIfAbsent(key, bytes);
        if (result != ControlMutationOutcome.APPLIED && !metadata.get(key).equals(Optional.of(bytes))) {
            throw new Unknown("Object immutable authority prewrite did not resolve exactly");
        }
    }

    private static void requireOwner(Head head, KafkaPartitionFenceV1 fence) {
        if (head.closed() || !head.fence().equals(fence)) {
            throw new Fenced("Object Owner no longer admits grants");
        }
    }

    private void requireCapacity(Head head, long candidateBytes) {
        if (candidateBytes <= 0) {
            throw new IllegalArgumentException("Object candidate byte charge must be positive");
        }
        int units = 0;
        long bytes = candidateBytes;
        for (Grant grant : head.grants()) {
            if (grant.locator().endOffsetExclusive() > head.checkpointEnd()) {
                units++;
                bytes = Math.addExact(bytes, grant.object().bodyLength());
            }
        }
        if (encode(head).length() + 9000L > MAX_VALUE_BYTES
                || head.grants().size() >= bounds.maxSources()
                || units >= bounds.maxUncoveredUnits()
                || bytes > bounds.maxUncoveredBytes()) {
            throw new IllegalStateException(
                    "Object protected source or cumulative recovery debt capacity is exhausted");
        }
    }

    private Head decodeChecked(CanonicalBytes bytes) {
        Head head = decode(bytes);
        if (!head.bounds().equals(bounds)
                || head.grants().size() > bounds.maxSources()
                || head.closedOwners() > bounds.maxClosedOwners()) {
            throw new IllegalStateException("Object authorization history exceeds configured recovery bounds");
        }
        return head;
    }

    public static KafkaObjectBindingKeyV1 binding(KafkaPartitionFenceV1 fence) {
        return new KafkaObjectBindingKeyV1(
                fence.bindingId(), fence.topicIncarnation().topicId(), fence.partitionId(), fence.storageEpochId());
    }

    private static CanonicalBytes encode(Head head) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(head.bounds().maxSources());
                out.writeInt(head.bounds().maxUncoveredUnits());
                out.writeLong(head.bounds().maxUncoveredBytes());
                out.writeInt(head.bounds().maxClosedOwners());
                out.writeLong(head.generation());
                fence(out, head.fence());
                out.writeBoolean(head.closed());
                out.writeLong(head.startOffset());
                out.writeLong(head.endOffset());
                out.writeLong(head.checkpointEnd());
                optionalSha(out, head.checkpoint());
                optionalSha(out, head.previousClosed());
                out.writeInt(head.closedOwners());
                out.writeInt(head.grants().size());
                for (Grant grant : head.grants()) {
                    fence(out, grant.fence());
                    write(out, grant.object().key().getBytes(StandardCharsets.UTF_8));
                    out.writeLong(grant.object().bodyLength());
                    out.write(grant.object().bodySha256().bytes().toByteArray());
                    write(out, KafkaObjectStateCodecV1.locator(grant.locator()).toByteArray());
                    out.write(grant.commitSetId().toByteArray());
                    out.write(grant.payloadSha().bytes().toByteArray());
                }
            }
            if (bytes.size() > MAX_VALUE_BYTES) {
                throw new IllegalArgumentException("Object Head exceeds canonical cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory Object authority encoding failed", failure);
        }
    }

    private static Head decode(CanonicalBytes bytes) {
        if (bytes.length() > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("Object Head exceeds canonical cap");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC) {
                throw new IllegalArgumentException("Object authorization wire mismatch");
            }
            var bounds = new Bounds(in.readInt(), in.readInt(), in.readLong(), in.readInt());
            long generation = in.readLong();
            var fence = fence(in);
            boolean closed = in.readBoolean();
            long start = in.readLong();
            long end = in.readLong();
            long cpEnd = in.readLong();
            var cp = optionalSha(in);
            var previous = optionalSha(in);
            int closedOwners = in.readInt();
            int count = in.readInt();
            if (count < 0 || count > 2048) {
                throw new IllegalArgumentException("Object source count exceeds cap");
            }
            var grants = new ArrayList<Grant>(count);
            for (int index = 0; index < count; index++) {
                var owner = fence(in);
                String key = new String(read(in, 4096), StandardCharsets.UTF_8);
                var object = new ObjectIdentity(key, in.readLong(), sha(in));
                var locator = KafkaObjectStateCodecV1.decodeLocator(CanonicalBytes.copyOf(read(in, 512)));
                grants.add(new Grant(owner, object, locator, CanonicalBytes.copyOf(in.readNBytes(16)), sha(in)));
            }
            Head head =
                    new Head(generation, fence, closed, start, end, grants, cpEnd, cp, previous, closedOwners, bounds);
            if (in.read() != -1 || !encode(head).equals(bytes)) {
                throw new IllegalArgumentException("Object Head is not canonical");
            }
            return head;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Object authorization Head is truncated", failure);
        }
    }

    private static void fence(DataOutputStream out, KafkaPartitionFenceV1 fence) throws IOException {
        out.write(fence.bindingId().digest().bytes().toByteArray());
        write(
                out,
                TopicIncarnationIdentityCodecV1.encode(fence.topicIncarnation()).toByteArray());
        out.writeInt(fence.partitionId());
        out.writeLong(fence.bindingGeneration());
        out.write(fence.storageEpochId().digest().bytes().toByteArray());
        out.writeLong(fence.ownerEpoch());
        out.writeInt(fence.kafkaLeaderEpoch());
    }

    private static KafkaPartitionFenceV1 fence(DataInputStream in) throws IOException {
        return new KafkaPartitionFenceV1(
                new TopicBindingId(sha(in)),
                (KafkaTopicIncarnationIdentity) TopicIncarnationIdentityCodecV1.decode(read(in, 4096)),
                in.readInt(),
                in.readLong(),
                new StorageEpochId(sha(in)),
                in.readLong(),
                in.readInt());
    }

    private static void write(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] read(DataInputStream in, int cap) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > cap) {
            throw new IllegalArgumentException("Object authority field exceeds cap");
        }
        byte[] bytes = in.readNBytes(size);
        if (bytes.length != size) {
            throw new IllegalArgumentException("Object authority field is truncated");
        }
        return bytes;
    }

    private static Sha256Digest sha(DataInputStream in) throws IOException {
        return Sha256Digest.copyOf(in.readNBytes(32));
    }

    private static void optionalSha(DataOutputStream out, Optional<Sha256Digest> sha) throws IOException {
        out.writeBoolean(sha.isPresent());
        if (sha.isPresent()) {
            out.write(sha.orElseThrow().bytes().toByteArray());
        }
    }

    private static Optional<Sha256Digest> optionalSha(DataInputStream in) throws IOException {
        return in.readBoolean() ? Optional.of(sha(in)) : Optional.empty();
    }
}
