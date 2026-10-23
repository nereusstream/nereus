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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

/**
 * A distinct terminal value at the existing Root-scoped Kafka checkpoint Head key. It closes only a domain that
 * never selected a checkpoint, never fabricates a checkpoint/Binding/offset, and grants no whole-Run retirement.
 */
public final class WalRunKafkaProtocolEmptyClosureV1 {
    public static final int MAX_BYTES = 1024 * 1024;
    private static final int MAGIC = 0x4e4b4330;
    private final WalRunRootRecord root;
    private final WalRunSealRecord seal;

    private WalRunKafkaProtocolEmptyClosureV1(WalRunRootRecord root, WalRunSealRecord seal) {
        this.root = Objects.requireNonNull(root, "root");
        this.seal = Objects.requireNonNull(seal, "seal");
        if (root.protocolCellIdentity().protocolKind() != ProtocolKindV1.KAFKA
                || !seal.root().rootSha256().equals(WalRunControlCodec.rootSha256(root))
                || root.shardId() != seal.root().shardId()
                || root.shardRunEpoch() != seal.root().shardRunEpoch()) {
            throw new IllegalArgumentException("empty protocol closure substituted the actual Kafka Root/Seal");
        }
        WalRunControlKeys.requireRootKey(seal.root().rootKey(), root.shardId(), root.shardRunEpoch());
    }

    public static boolean hasMagic(CanonicalBytes value) {
        if (value.length() > MAX_BYTES) {
            throw new IllegalArgumentException("protocol-domain Head exceeds its control bound");
        }
        return value.length() >= Integer.BYTES
                && ByteBuffer.wrap(value.toByteArray()).getInt() == MAGIC;
    }

    public WalRunRootRecord root() {
        return root;
    }

    public WalRunSealRecord seal() {
        return seal;
    }

    public String headKey() {
        return WalRunControlKeys.kafkaProtocolCheckpointHeadKey(root.shardId(), root.shardRunEpoch());
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(1);
                field(out, WalRunControlCodec.encodeRoot(root));
                field(out, WalRunControlCodec.encodeSeal(seal));
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("empty protocol closure exceeds its control bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory protocol closure encoding failed", failure);
        }
    }

    public static WalRunKafkaProtocolEmptyClosureV1 decode(CanonicalBytes raw) {
        if (raw.length() > MAX_BYTES) {
            throw new IllegalArgumentException("empty protocol closure exceeds its control bound");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(raw.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != 1) {
                throw new IllegalArgumentException("unsupported empty protocol closure wire version");
            }
            var value = new WalRunKafkaProtocolEmptyClosureV1(
                    WalRunControlCodec.decodeRoot(field(in)), WalRunControlCodec.decodeSeal(field(in)));
            if (in.read() != -1 || !value.encode().equals(raw)) {
                throw new IllegalArgumentException("noncanonical empty protocol closure");
            }
            return value;
        } catch (IOException failure) {
            throw new IllegalArgumentException("truncated empty protocol closure", failure);
        }
    }

    /** Wire value only: actual domain closure additionally requires confirmed selection at its existing Head key. */
    public static WalRunKafkaProtocolEmptyClosureV1 forClosure(
            WalRunRootRecord root, WalRunTerminalClosureProofV1 proof) {
        Objects.requireNonNull(proof, "proof");
        var seal = new WalRunSealRecord(
                proof.root(),
                proof.terminalSequence(),
                proof.finalPhysicalCheckpointHeadKey(),
                proof.finalPhysicalCheckpointHeadSha256(),
                proof.aggregateExtentCount(),
                proof.aggregateCanonicalBodyBytes());
        return new WalRunKafkaProtocolEmptyClosureV1(root, seal);
    }

    /** Acquires the actual sealed session and retains its exact candidate across UNKNOWN; no raw-value factory. */
    public static Attempt begin(WalRunObjectSession session, WalRunTerminalClosureProofV1 proof) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(proof, "proof");
        var io = session.acquireTerminalIo();
        try {
            var value = forClosure(session.rootRecord(), proof);
            CanonicalBytes raw = value.encode();
            var memory = session.acquireRecoveryWorkingSet(Math.multiplyExact(3L, raw.length()));
            return new Attempt(session, value, raw, io, memory);
        } catch (RuntimeException | Error failure) {
            io.close();
            throw failure;
        }
    }

    /** Only a confirmed same-key decision is issued; a decoder/value/hash is never this authority. */
    public static final class Confirmed {
        private final WalRunKafkaProtocolEmptyClosureV1 value;
        private final CanonicalBytes raw;

        private Confirmed(WalRunKafkaProtocolEmptyClosureV1 value, CanonicalBytes raw) {
            this.value = value;
            this.raw = raw;
        }

        public WalRunKafkaProtocolEmptyClosureV1 value() {
            return value;
        }

        public CanonicalBytes exactValue() {
            return raw;
        }

        public Sha256Digest valueSha256() {
            return Sha256Digest.hash(raw);
        }
    }

    /** No abandon/timeout release: unresolved attempts retain the original session and candidate workspace. */
    public static final class Attempt {
        private final WalRunObjectSession session;
        private final WalRunKafkaProtocolEmptyClosureV1 value;
        private final CanonicalBytes raw;
        private final WalRunObjectSession.IoLease io;
        private final WalRunObjectSession.RecoveryWorkingSetLease memory;
        private boolean settled;

        private Attempt(
                WalRunObjectSession session,
                WalRunKafkaProtocolEmptyClosureV1 value,
                CanonicalBytes raw,
                WalRunObjectSession.IoLease io,
                WalRunObjectSession.RecoveryWorkingSetLease memory) {
            this.session = session;
            this.value = value;
            this.raw = raw;
            this.io = io;
            this.memory = memory;
        }

        /** Empty means an existing Head won and must be independently verified/terminalized, never “no obligation”. */
        public synchronized Optional<Confirmed> closeIfAbsent(CanonicalControlMetadataStore metadata) {
            if (settled) {
                throw new IllegalStateException("protocol-domain close attempt is already settled");
            }
            requireStored(metadata, value.seal().root().rootKey(), WalRunControlCodec.encodeRoot(value.root()));
            requireStored(
                    metadata,
                    WalRunControlKeys.sealKey(
                            value.root().shardId(), value.root().shardRunEpoch()),
                    WalRunControlCodec.encodeSeal(value.seal()));
            session.chargeRecoveryControlMetadata(raw.length());
            metadata.compareAndSet(value.headKey(), Optional.empty(), raw);
            session.chargeRecoveryControlMetadata(MAX_BYTES);
            var observed = metadata.get(value.headKey());
            if (observed.isEmpty()) {
                throw new IllegalStateException("protocol-domain close remains unknown; retain the original attempt");
            }
            CanonicalBytes found = observed.orElseThrow();
            if (found.length() > MAX_BYTES) {
                throw new IllegalStateException("protocol-domain Head exceeds its admitted control bound");
            }
            Optional<Confirmed> result;
            try (var read = session.acquireRecoveryWorkingSet(Math.multiplyExact(3L, found.length()))) {
                if (found.equals(raw)) {
                    result = Optional.of(new Confirmed(value, raw));
                } else {
                    // Head keys are never deleted. Once nonempty, every late expected-absent CAS is fenced out.
                    result = Optional.empty();
                }
            }
            memory.close();
            io.close();
            settled = true;
            return result;
        }

        private void requireStored(CanonicalControlMetadataStore metadata, String key, CanonicalBytes expected) {
            session.chargeRecoveryControlMetadata(MAX_BYTES);
            var observed = metadata.get(key)
                    .orElseThrow(() -> new IllegalStateException("protocol-domain closure lost its actual Root/Seal"));
            if (observed.length() > MAX_BYTES) {
                throw new IllegalStateException("protocol-domain closure control identity exceeds its bound");
            }
            try (var read = session.acquireRecoveryWorkingSet(Math.multiplyExact(3L, observed.length()))) {
                if (!observed.equals(expected)) {
                    throw new IllegalStateException("protocol-domain closure substituted its actual Root/Seal");
                }
            }
        }
    }

    private static void field(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        if (bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("protocol closure field exceeds its bound");
        }
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static CanonicalBytes field(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > MAX_BYTES || size > in.available()) {
            throw new IllegalArgumentException("protocol closure field length is invalid");
        }
        return CanonicalBytes.copyOf(in.readNBytes(size));
    }
}
