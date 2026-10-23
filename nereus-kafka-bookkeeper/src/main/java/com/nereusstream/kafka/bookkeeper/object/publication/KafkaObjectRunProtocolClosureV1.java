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
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaCheckpointCoverageV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadCodecV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadStateV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1CodecV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.StorageObjectNwkcp1BackendV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunKafkaProtocolEmptyClosureV1;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunTerminalClosureProofV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.io.IOException;
import java.util.Optional;

/** The one existing protocol Head key is the persistent fence, including a never-selected domain. */
final class KafkaObjectRunProtocolClosureV1 {
    private final CanonicalControlMetadataStore metadata;
    private Attempt pending;

    KafkaObjectRunProtocolClosureV1(CanonicalControlMetadataStore metadata) {
        this.metadata = metadata;
    }

    boolean hasPending() {
        return pending != null;
    }

    Closed close(WalRunObjectSession session, WalRunTerminalClosureProofV1 proof) throws IOException {
        if (pending != null
                && (pending.session != session
                        || !pending.proof.root().equals(proof.root())
                        || !pending.proof.sealSha256().equals(proof.sealSha256()))) {
            throw new IllegalStateException("retain the original unresolved protocol-domain closure");
        }
        for (int conflicts = 0;
                conflicts <= session.rootRecord().recoveryEnvelope().maxRetryAttempts();
                conflicts++) {
            if (pending == null) {
                pending = qualify(session, proof);
            }
            var attempt = pending;
            if (attempt.expected.equals(Optional.of(attempt.terminal))) {
                pending = null;
                return new Closed(attempt);
            }
            if (attempt.dispatched) {
                session.chargeRecoveryControlMetadata(attempt.terminal.length());
            }
            attempt.dispatched = true;
            var outcome = metadata.compareAndSet(attempt.key, attempt.expected, attempt.terminal);
            var selected = attempt.read(attempt.key);
            if (selected.equals(Optional.of(attempt.terminal))) {
                pending = null;
                return new Closed(attempt);
            }
            if (outcome != ControlMutationOutcome.DEFINITIVE_CONFLICT || selected.isEmpty()) {
                throw new KafkaObjectAuthorizationV1.Unknown(
                        "protocol closure is unknown; retain the original candidate and session leases");
            }
            // An actual same-key CAS conflict fences this original predecessor; requalify the real winner.
            attempt.close();
            pending = null;
        }
        throw new IllegalStateException("protocol Head kept advancing beyond the original Root retry bound");
    }

    private Attempt qualify(WalRunObjectSession session, WalRunTerminalClosureProofV1 proof) throws IOException {
        var attempt = new Attempt(session, proof, metadata);
        try {
            var root = session.rootRecord();
            var empty = WalRunKafkaProtocolEmptyClosureV1.forClosure(root, proof);
            if (!attempt.read(proof.root().rootKey()).equals(Optional.of(WalRunControlCodec.encodeRoot(root)))
                    || !attempt.read(proof.sealKey())
                            .equals(Optional.of(WalRunControlCodec.encodeSeal(empty.seal())))) {
                throw new IllegalStateException("protocol closure lost the actual immutable Root/Seal");
            }
            attempt.expected = attempt.read(attempt.key);
            if (attempt.expected.isEmpty()) {
                attempt.terminal = empty.encode();
            } else if (WalRunKafkaProtocolEmptyClosureV1.hasMagic(attempt.expected.orElseThrow())) {
                if (!WalRunKafkaProtocolEmptyClosureV1.decode(attempt.expected.orElseThrow())
                        .encode()
                        .equals(empty.encode())) {
                    throw new IllegalStateException("closed protocol domain substituted Root/Seal");
                }
                attempt.terminal = attempt.expected.orElseThrow();
            } else {
                var head = KafkaProtocolCheckpointHeadCodecV1.decodeForRoot(root, attempt.expected.orElseThrow());
                attempt.reserve(Math.multiplyExact(3L, head.checkpointObjectLength()));
                var raw = session.readVerifiedProtocolObject(new ObjectIdentity(
                        head.checkpointObjectKey(), head.checkpointObjectLength(), head.checkpointObjectDigest()));
                var prefix = KafkaProtocolCheckpointHeadCodecV1.objectPrefix(head);
                var object = Nwkcp1CodecV1.decodeVerified(
                        prefix,
                        head.checkpointObjectKey(),
                        head.checkpointObjectLength(),
                        head.checkpointObjectDigest(),
                        raw);
                if (!object.walRunRootSha().equals(session.rootSha256())
                        || object.rows().size() != 1
                        || !object.rows().stream()
                                .map(row -> KafkaCheckpointCoverageV1.from(row.vector()))
                                .toList()
                                .equals(head.coveredThroughVector())) {
                    throw new IllegalStateException(
                            "protocol closure requires the complete supported single-Binding vector");
                }
                var state = object.rows().get(0);
                new StorageObjectNwkcp1BackendV1(session, metadata, prefix)
                        .verifyPhysicalClosure(proof, state.vector().runBinding(), state);
                attempt.checkpoint = Optional.of(state);
                attempt.terminal = KafkaProtocolCheckpointHeadCodecV1.encode(
                        prefix,
                        session.rootSha256(),
                        head.state() == KafkaProtocolCheckpointHeadStateV1.TERMINAL ? head : head.terminal());
            }
            attempt.reserve(Math.multiplyExact(3L, attempt.terminal.length()));
            if (!attempt.expected.equals(Optional.of(attempt.terminal))) {
                session.chargeRecoveryControlMetadata(attempt.terminal.length());
            }
            return attempt;
        } catch (IOException | RuntimeException | Error failure) {
            attempt.close();
            throw failure;
        }
    }

    static final class Closed implements AutoCloseable {
        private final Attempt attempt;
        private boolean transferred;

        private Closed(Attempt attempt) {
            this.attempt = attempt;
        }

        CanonicalBytes exactHead() {
            return attempt.terminal;
        }

        Optional<KafkaProtocolCheckpointStateV1> checkpoint() {
            return attempt.checkpoint;
        }

        WalRunObjectSession.RecoveryWorkingSetLease transferMemory() {
            var memory = attempt.memory;
            if (memory == null) {
                throw new IllegalStateException("protocol closure memory was already transferred");
            }
            attempt.memory = null;
            return memory;
        }

        WalRunObjectSession.IoLease transferIo() {
            if (transferred) {
                throw new IllegalStateException("protocol closure I/O was already transferred");
            }
            attempt.releaseMemory();
            transferred = true;
            return attempt.io;
        }

        @Override
        public void close() {
            if (!transferred) {
                attempt.close();
            }
        }
    }

    private static final class Attempt implements AutoCloseable {
        private final WalRunObjectSession session;
        private final CanonicalControlMetadataStore metadata;
        private final WalRunTerminalClosureProofV1 proof;
        private final String key;
        private final WalRunObjectSession.IoLease io;
        private WalRunObjectSession.RecoveryWorkingSetLease memory;
        private Optional<CanonicalBytes> expected;
        private CanonicalBytes terminal;
        private boolean dispatched;
        private Optional<KafkaProtocolCheckpointStateV1> checkpoint = Optional.empty();

        private Attempt(
                WalRunObjectSession session,
                WalRunTerminalClosureProofV1 proof,
                CanonicalControlMetadataStore metadata) {
            this.session = session;
            this.metadata = metadata;
            this.proof = proof;
            key = WalRunControlKeys.kafkaProtocolCheckpointHeadKey(
                    session.rootRecord().shardId(), session.rootRecord().shardRunEpoch());
            io = session.acquireTerminalIo();
        }

        private Optional<CanonicalBytes> read(String key) {
            var reservation = session.reserveRecoveryControlMetadata(WalRunKafkaProtocolEmptyClosureV1.MAX_BYTES);
            var value = metadata.get(key);
            value.ifPresent(raw -> {
                if (raw.length() > WalRunKafkaProtocolEmptyClosureV1.MAX_BYTES) {
                    throw new IllegalStateException("protocol closure metadata exceeds its control bound");
                }
                reserve(Math.multiplyExact(3L, raw.length()));
            });
            reservation.settle(value.map(CanonicalBytes::length).orElse(0));
            return value;
        }

        private void reserve(long bytes) {
            if (bytes <= 0) {
                return;
            }
            if (memory == null) {
                memory = session.acquireRecoveryWorkingSet(bytes);
            } else {
                memory.grow(bytes);
            }
        }

        private void releaseMemory() {
            if (memory != null) {
                memory.close();
                memory = null;
            }
        }

        @Override
        public void close() {
            releaseMemory();
            io.close();
        }
    }
}
