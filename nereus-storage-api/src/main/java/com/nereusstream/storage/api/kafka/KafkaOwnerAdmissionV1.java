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
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2.Link;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One bounded native-owner admission cut. Run attachment and closure use the same head CAS.
 * Closed bytes are immutable and are archived before a successor owner can replace the head.
 */
public record KafkaOwnerAdmissionV1(
        Sha256Digest scopeSha256,
        KafkaOwnerIdentityV1 owner,
        boolean closed,
        Optional<Previous> previous,
        List<Link> runs) {
    public static final int MAX_RUNS = 1024;
    public static final int MAX_BYTES = 128 + 48 * MAX_RUNS + 128;
    private static final int MAGIC = 0x4e4b4f41;

    public record Previous(long ownerEpoch, Sha256Digest closureSha256, Optional<Link> tail) {
        public Previous {
            Objects.requireNonNull(closureSha256, "closureSha256");
            Objects.requireNonNull(tail, "tail");
            if (ownerEpoch <= 0 || closureSha256.isZero()) {
                throw new IllegalArgumentException("previous owner closure identity is invalid");
            }
        }
    }

    public KafkaOwnerAdmissionV1 {
        Objects.requireNonNull(scopeSha256, "scopeSha256");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(previous, "previous");
        runs = List.copyOf(runs);
        var ids = new HashSet<com.nereusstream.storage.api.bookkeeper.StorageRunId>();
        if (scopeSha256.isZero()
                || runs.size() > MAX_RUNS
                || previous.filter(value -> value.ownerEpoch() >= owner.ownerEpoch())
                        .isPresent()
                || runs.stream().anyMatch(link -> !ids.add(link.runId()))
                || previous.flatMap(Previous::tail)
                        .filter(link -> ids.contains(link.runId()))
                        .isPresent()) {
            throw new IllegalArgumentException("owner admission scope, history, or bounded run set is invalid");
        }
    }

    public Optional<Link> tail() {
        return runs.isEmpty() ? previous.flatMap(Previous::tail) : Optional.of(runs.get(runs.size() - 1));
    }

    public KafkaOwnerAdmissionV1 close() {
        return new KafkaOwnerAdmissionV1(scopeSha256, owner, true, previous, runs);
    }

    public KafkaOwnerAdmissionV1 append(KafkaRunRootRecordV2 candidate) {
        var root = candidate.root();
        if (closed
                || root.creatorOwnerEpoch() != owner.ownerEpoch()
                || root.kafkaLeaderEpoch() != owner.kafkaLeaderEpoch()
                || !Sha256Digest.hash(KafkaRunRootRecordV2.Scope.of(root).encode())
                        .equals(scopeSha256)
                || !root.predecessorRunId().equals(tail().map(Link::runId))) {
            throw new IllegalArgumentException("run attachment is outside the open owner's exact tail");
        }
        var replacement = new ArrayList<>(runs);
        replacement.add(candidate.initialLink());
        return new KafkaOwnerAdmissionV1(scopeSha256, owner, false, previous, replacement);
    }

    public CanonicalBytes encode() {
        var out = ByteBuffer.allocate(MAX_BYTES);
        out.putInt(MAGIC).putShort((short) 1).put(scopeSha256.bytes().toByteArray());
        out.putLong(owner.ownerEpoch())
                .putInt(owner.kafkaLeaderEpoch())
                .putInt(owner.brokerId())
                .putLong(owner.brokerEpoch())
                .putLong(owner.metadataOffset());
        out.put((byte) (closed ? 1 : 0)).put((byte) (previous.isPresent() ? 1 : 0));
        previous.ifPresent(value -> {
            out.putLong(value.ownerEpoch()).put(value.closureSha256().bytes().toByteArray());
            out.put((byte) (value.tail().isPresent() ? 1 : 0));
            value.tail().ifPresent(link -> putLink(out, link));
        });
        out.putInt(runs.size());
        runs.forEach(link -> putLink(out, link));
        var body = CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position()));
        out.put(Sha256Digest.hash(body).bytes().toByteArray());
        return CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position()));
    }

    public static KafkaOwnerAdmissionV1 decode(CanonicalBytes bytes) {
        try {
            var raw = bytes.toByteArray();
            if (raw.length < 108 || raw.length > MAX_BYTES) {
                throw new IllegalArgumentException("owner admission wire size is invalid");
            }
            var body = Arrays.copyOf(raw, raw.length - 32);
            if (!Sha256Digest.hash(CanonicalBytes.copyOf(body))
                    .equals(Sha256Digest.copyOf(Arrays.copyOfRange(raw, body.length, raw.length)))) {
                throw new IllegalArgumentException("owner admission digest differs");
            }
            var in = ByteBuffer.wrap(body);
            if (in.getInt() != MAGIC || in.getShort() != 1) {
                throw new IllegalArgumentException("unknown owner admission wire");
            }
            var scope = digest(in);
            var owner = new KafkaOwnerIdentityV1(in.getLong(), in.getInt(), in.getInt(), in.getLong(), in.getLong());
            boolean closed = flag(in);
            Optional<Previous> previous = Optional.empty();
            if (flag(in)) {
                long epoch = in.getLong();
                var closure = digest(in);
                previous =
                        Optional.of(new Previous(epoch, closure, flag(in) ? Optional.of(link(in)) : Optional.empty()));
            }
            int count = in.getInt();
            if (count < 0 || count > MAX_RUNS || in.remaining() != count * 48) {
                throw new IllegalArgumentException("owner admission run count differs");
            }
            var runs = new ArrayList<Link>(count);
            for (int index = 0; index < count; index++) {
                runs.add(link(in));
            }
            var value = new KafkaOwnerAdmissionV1(scope, owner, closed, previous, runs);
            if (!value.encode().equals(bytes)) {
                throw new IllegalArgumentException("noncanonical owner admission");
            }
            return value;
        } catch (java.nio.BufferUnderflowException failure) {
            throw new IllegalArgumentException("truncated owner admission", failure);
        }
    }

    private static void putLink(ByteBuffer out, Link link) {
        out.put(link.runId().value().bytes().toByteArray())
                .put(link.initialRootSha256().bytes().toByteArray());
    }

    private static Link link(ByteBuffer in) {
        byte[] id = new byte[16];
        in.get(id);
        return new Link(
                new com.nereusstream.storage.api.bookkeeper.StorageRunId(
                        com.nereusstream.domain.identity.Id128.fromBytes(id)),
                digest(in));
    }

    private static Sha256Digest digest(ByteBuffer in) {
        byte[] bytes = new byte[32];
        in.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }

    private static boolean flag(ByteBuffer in) {
        byte value = in.get();
        if (value != 0 && value != 1) {
            throw new IllegalArgumentException("noncanonical owner admission flag");
        }
        return value == 1;
    }
}
