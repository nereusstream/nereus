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

package com.nereusstream.storage.api.kafka;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Bounded run record: unpublished prewrite, admitted root, and one irreversible successor selection. */
public record KafkaRunRootRecordV2(
        PhysicalResourceIdV2.BookKeeperLedger resource,
        KafkaRunRootSnapshotV1 root,
        boolean admitted,
        Optional<Link> successor) {
    public static final int MAX_BYTES = 32768;
    private static final int MAGIC = 0x4d354b52;

    /** Protocol routing identity; physical namespace belongs to the backend binding, not a caller-supplied path. */
    public record Scope(
            TopicBindingId bindingId,
            KafkaTopicIncarnationIdentity topic,
            int partition,
            StorageEpochId storageEpoch,
            CellProviderScopeId providerScope) {
        public Scope {
            Objects.requireNonNull(bindingId, "bindingId");
            Objects.requireNonNull(topic, "topic");
            Objects.requireNonNull(storageEpoch, "storageEpoch");
            Objects.requireNonNull(providerScope, "providerScope");
            if (partition < 0
                    || bindingId.digest().isZero()
                    || storageEpoch.digest().isZero()) {
                throw new IllegalArgumentException("run-root scope has an invalid identity");
            }
        }

        public static Scope of(KafkaRunRootSnapshotV1 root) {
            return new Scope(
                    root.bindingId(),
                    root.topicIncarnation(),
                    root.partitionId(),
                    root.storageEpochId(),
                    root.providerScopeId());
        }

        public CanonicalBytes encode() {
            var name = topic.topicName().bytes().toByteArray();
            return CanonicalBytes.copyOf(ByteBuffer.allocate(126 + name.length)
                    .putInt(0x4d354b53)
                    .putShort((short) 2)
                    .put(bindingId.digest().bytes().toByteArray())
                    .put(topic.topicId().value().bytes().toByteArray())
                    .putInt(name.length)
                    .put(name)
                    .putInt(partition)
                    .put(storageEpoch.digest().bytes().toByteArray())
                    .put(providerScope.digest().bytes().toByteArray())
                    .array());
        }
    }

    /** Exact immutable choice; a child cannot substitute a different ledger or creator under the same run ID. */
    public record Link(StorageRunId runId, Sha256Digest initialRootSha256) {
        public Link {
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(initialRootSha256, "initialRootSha256");
            if (initialRootSha256.isZero()) {
                throw new IllegalArgumentException("run selection digest is zero");
            }
        }

        public CanonicalBytes encode() {
            var body = ByteBuffer.allocate(54)
                    .putInt(0x4d354b4c)
                    .putShort((short) 2)
                    .put(runId.value().bytes().toByteArray())
                    .put(initialRootSha256.bytes().toByteArray())
                    .array();
            return signed(body);
        }

        public static Link decode(CanonicalBytes bytes) {
            var in = verified(bytes);
            if (in.getInt() != 0x4d354b4c || in.getShort() != 2) {
                throw new IllegalArgumentException("unknown run selection wire");
            }
            var link = new Link(new StorageRunId(Id128.fromBytes(take(in, 16))), Sha256Digest.copyOf(take(in, 32)));
            if (in.hasRemaining() || !link.encode().equals(bytes)) {
                throw new IllegalArgumentException("noncanonical run selection");
            }
            return link;
        }
    }

    public KafkaRunRootRecordV2 {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(root, "root");
        successor = Objects.requireNonNull(successor, "successor");
        Scope.of(root);
        if (resource.ledgerId() != root.ledgerIdentity().ledgerId()
                || !admitted && root.state() != KafkaRunRootStateV1.ACTIVE
                || successor.isPresent() && (!admitted || root.state() != KafkaRunRootStateV1.SEALED)
                || successor.filter(link -> link.runId().equals(root.runId())).isPresent()) {
            throw new IllegalArgumentException("run record identity, admission, or successor is invalid");
        }
    }

    public static KafkaRunRootRecordV2 pending(
            PhysicalResourceIdV2.Namespace namespace, KafkaRunRootSnapshotV1 active) {
        if (active.state() != KafkaRunRootStateV1.ACTIVE) {
            throw new IllegalArgumentException("run prewrite must be ACTIVE");
        }
        return new KafkaRunRootRecordV2(
                new PhysicalResourceIdV2.BookKeeperLedger(
                        namespace, active.ledgerIdentity().ledgerId()),
                active,
                false,
                Optional.empty());
    }

    public KafkaRunRootRecordV2 admit() {
        return new KafkaRunRootRecordV2(resource, root, true, successor);
    }

    public KafkaRunRootRecordV2 seal(KafkaRunRootSnapshotV1 sealed) {
        if (!admitted
                || root.state() != KafkaRunRootStateV1.ACTIVE
                || sealed.state() != KafkaRunRootStateV1.SEALED
                || !active(sealed).equals(root)) {
            throw new IllegalArgumentException("seal must preserve the exact admitted active root");
        }
        return new KafkaRunRootRecordV2(resource, sealed, true, Optional.empty());
    }

    public KafkaRunRootRecordV2 select(KafkaRunRootRecordV2 child) {
        if (!admitted
                || root.state() != KafkaRunRootStateV1.SEALED
                || !resource.namespace().equals(child.resource().namespace())
                || !Scope.of(root).equals(Scope.of(child.root()))
                || child.root().state() != KafkaRunRootStateV1.ACTIVE
                || !child.root().predecessorRunId().equals(Optional.of(root.runId()))
                || child.root().kafkaStartOffset()
                        != root.kafkaEndOffsetExclusive().orElseThrow()
                || child.root().creatorOwnerEpoch() < root.creatorOwnerEpoch()
                || child.root().kafkaLeaderEpoch() < root.kafkaLeaderEpoch()
                || child.resource().equals(resource)
                || successor.filter(value -> !value.equals(child.initialLink())).isPresent()) {
            throw new IllegalArgumentException("successor changes the run chain or its permanent choice");
        }
        return new KafkaRunRootRecordV2(resource, root, true, Optional.of(child.initialLink()));
    }

    public Link initialLink() {
        return new Link(
                root.runId(),
                Sha256Digest.hash(pending(resource.namespace(), active(root)).encode()));
    }

    public CanonicalBytes encode() {
        var physical = resource.canonicalBytes().toByteArray();
        var scope = Scope.of(root).encode().toByteArray();
        var out = ByteBuffer.allocate(MAX_BYTES);
        out.putInt(MAGIC).putShort((short) 2).put((byte) (admitted ? 1 : 0));
        out.putInt(physical.length).put(physical).putInt(scope.length).put(scope);
        out.putLong(root.creatorOwnerEpoch())
                .putInt(root.kafkaLeaderEpoch())
                .put(root.runId().value().bytes().toByteArray())
                .putLong(root.kafkaStartOffset());
        out.put((byte) (root.state() == KafkaRunRootStateV1.ACTIVE ? 1 : 2));
        out.putLong(root.kafkaEndOffsetExclusive().orElse(-1));
        out.put((byte) (root.predecessorRunId().isPresent() ? 1 : 0));
        root.predecessorRunId().ifPresent(value -> out.put(value.value().bytes().toByteArray()));
        out.put((byte) (successor.isPresent() ? 1 : 0));
        successor.ifPresent(value -> out.put(value.runId().value().bytes().toByteArray())
                .put(value.initialRootSha256().bytes().toByteArray()));
        return signed(Arrays.copyOf(out.array(), out.position()));
    }

    public static KafkaRunRootRecordV2 decode(CanonicalBytes bytes) {
        try {
            var in = verified(bytes);
            if (in.getInt() != MAGIC || in.getShort() != 2) {
                throw new IllegalArgumentException("unknown run-root record wire");
            }
            boolean admitted = flag(in);
            var physical = PhysicalResourceIdCodecV2.decode(CanonicalBytes.copyOf(take(in, in.getInt())));
            if (!(physical instanceof PhysicalResourceIdV2.BookKeeperLedger resource)) {
                throw new IllegalArgumentException("run root has a non-BookKeeper resource");
            }
            var scopeIn = ByteBuffer.wrap(take(in, in.getInt()));
            if (scopeIn.getInt() != 0x4d354b53 || scopeIn.getShort() != 2) {
                throw new IllegalArgumentException("unknown run scope wire");
            }
            var binding = new TopicBindingId(Sha256Digest.copyOf(take(scopeIn, 32)));
            var topicId = new KafkaTopicId(Id128.fromBytes(take(scopeIn, 16)));
            var topic = new KafkaTopicIncarnationIdentity(
                    topicId, KafkaTopicName.fromBytes(take(scopeIn, scopeIn.getInt())));
            var scope = new Scope(
                    binding,
                    topic,
                    scopeIn.getInt(),
                    new StorageEpochId(Sha256Digest.copyOf(take(scopeIn, 32))),
                    new CellProviderScopeId(Sha256Digest.copyOf(take(scopeIn, 32))));
            if (scopeIn.hasRemaining()) {
                throw new IllegalArgumentException("trailing run scope bytes");
            }
            long owner = in.getLong();
            int leader = in.getInt();
            var run = new StorageRunId(Id128.fromBytes(take(in, 16)));
            long start = in.getLong();
            int state = Byte.toUnsignedInt(in.get());
            long end = in.getLong();
            if (state != 1 && state != 2 || state == 1 && end != -1) {
                throw new IllegalArgumentException("invalid run root state or end");
            }
            Optional<StorageRunId> predecessor =
                    flag(in) ? Optional.of(new StorageRunId(Id128.fromBytes(take(in, 16)))) : Optional.empty();
            Optional<Link> successor = flag(in)
                    ? Optional.of(new Link(
                            new StorageRunId(Id128.fromBytes(take(in, 16))), Sha256Digest.copyOf(take(in, 32))))
                    : Optional.empty();
            var root = new KafkaRunRootSnapshotV1(
                    scope.bindingId(),
                    scope.topic(),
                    scope.partition(),
                    scope.storageEpoch(),
                    owner,
                    leader,
                    scope.providerScope(),
                    run,
                    new com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity(resource.ledgerId()),
                    start,
                    state == 1 ? OptionalLong.empty() : OptionalLong.of(end),
                    state == 1 ? KafkaRunRootStateV1.ACTIVE : KafkaRunRootStateV1.SEALED,
                    predecessor);
            var value = new KafkaRunRootRecordV2(resource, root, admitted, successor);
            if (in.hasRemaining() || !value.encode().equals(bytes)) {
                throw new IllegalArgumentException("noncanonical run-root record");
            }
            return value;
        } catch (java.nio.BufferUnderflowException failure) {
            throw new IllegalArgumentException("truncated run-root record", failure);
        }
    }

    private static KafkaRunRootSnapshotV1 active(KafkaRunRootSnapshotV1 root) {
        return new KafkaRunRootSnapshotV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId(),
                root.ledgerIdentity(),
                root.kafkaStartOffset(),
                OptionalLong.empty(),
                KafkaRunRootStateV1.ACTIVE,
                root.predecessorRunId());
    }

    private static CanonicalBytes signed(byte[] body) {
        if (body.length + 32 > MAX_BYTES) {
            throw new IllegalArgumentException("run-root record exceeds its bound");
        }
        return CanonicalBytes.copyOf(ByteBuffer.allocate(body.length + 32)
                .put(body)
                .put(Sha256Digest.hash(CanonicalBytes.copyOf(body)).bytes().toByteArray())
                .array());
    }

    private static ByteBuffer verified(CanonicalBytes value) {
        Objects.requireNonNull(value, "value");
        if (value.length() < 38 || value.length() > MAX_BYTES) {
            throw new IllegalArgumentException("run-root envelope size is invalid");
        }
        byte[] bytes = value.toByteArray();
        byte[] body = Arrays.copyOf(bytes, bytes.length - 32);
        if (!Sha256Digest.hash(CanonicalBytes.copyOf(body))
                .equals(Sha256Digest.copyOf(Arrays.copyOfRange(bytes, body.length, bytes.length)))) {
            throw new IllegalArgumentException("run-root envelope digest differs");
        }
        return ByteBuffer.wrap(body);
    }

    private static byte[] take(ByteBuffer in, int count) {
        if (count < 0 || count > in.remaining()) {
            throw new IllegalArgumentException("run-root field length is outside its bound");
        }
        byte[] bytes = new byte[count];
        in.get(bytes);
        return bytes;
    }

    private static boolean flag(ByteBuffer in) {
        byte value = in.get();
        if (value != 0 && value != 1) {
            throw new IllegalArgumentException("noncanonical run-root flag");
        }
        return value == 1;
    }
}
