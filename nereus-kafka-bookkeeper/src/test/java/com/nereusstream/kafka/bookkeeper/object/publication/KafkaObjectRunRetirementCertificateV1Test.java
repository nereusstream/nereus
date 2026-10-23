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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.kafka.bookkeeper.object.ObjectKafkaTestFixtures;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.StorageObjectNwkcp1BackendV1Test;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.LaneSequenceVector;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointBindingV1;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import com.nereusstream.storage.object.recovery.CumulativeRecoveryBudget;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeExceededException;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaObjectRunRetirementCertificateV1Test {
    @Test
    void privateWireValueRoundTripsAndRejectsVersionTruncationExpansionAndMemberBounds() {
        var root = StorageObjectNwkcp1BackendV1Test.root();
        var seal = seal(root);
        var certificate = new KafkaObjectRunRetirementCertificateV1(
                root, seal, seal.root(), 1, Optional.empty(), protocolHead(root, seal), List.of());
        var raw = certificate.encode();
        assertThat(ByteBuffer.wrap(raw.toByteArray()).getInt()).isEqualTo(0x4b525231);
        assertThat(KafkaObjectRunRetirementCertificateV1.decode(raw)).isEqualTo(certificate);
        byte[] version = raw.toByteArray();
        ByteBuffer.wrap(version).putInt(4, 1);
        byte[] members = raw.toByteArray();
        ByteBuffer.wrap(members)
                .putInt(members.length - Integer.BYTES, KafkaObjectRunRetirementCertificateV1.MAX_MEMBERS + 1);
        for (byte[] mutated : List.of(
                version,
                members,
                Arrays.copyOf(raw.toByteArray(), raw.length() - 1),
                Arrays.copyOf(raw.toByteArray(), raw.length() + 1),
                new byte[KafkaObjectRunRetirementCertificateV1.MAX_BYTES + 1])) {
            assertThatThrownBy(() -> KafkaObjectRunRetirementCertificateV1.decode(CanonicalBytes.copyOf(mutated)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new KafkaObjectRunRetirementCertificateV1(
                        root,
                        seal,
                        seal.root(),
                        1,
                        Optional.empty(),
                        protocolHead(root, seal),
                        Collections.nCopies(KafkaObjectRunRetirementCertificateV1.MAX_MEMBERS + 1, raw)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaObjectRunRetirementCertificateV1(
                        root, seal, seal.root(), 1, Optional.empty(), protocolHead(root, seal), List.of(raw, raw)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void prefixSummaryRequiresActualGenesisAndDirectLineageInsteadOfNumericEpochSelection() {
        var first = StorageObjectNwkcp1BackendV1Test.root();
        var firstSeal = seal(first);
        var boundary = new WalRunPredecessor(
                firstSeal.root(),
                WalRunControlKeys.sealKey(first.shardId(), 1),
                WalRunControlCodec.sealSha256(firstSeal),
                Optional.of(new TerminalProtocolCheckpointBindingV1(
                        ProtocolKindV1.KAFKA, "actual-aggregate-transfer", ObjectKafkaTestFixtures.digest(24))));
        var next = successor(first, boundary);
        var nextSeal = seal(next);
        var valid = new KafkaObjectRunRetirementCertificateV1(
                next,
                nextSeal,
                firstSeal.root(),
                2,
                Optional.of(ObjectKafkaTestFixtures.digest(25)),
                protocolHead(next, nextSeal),
                List.of());
        assertThat(KafkaObjectRunRetirementCertificateV1.decode(valid.encode())).isEqualTo(valid);
        assertThatThrownBy(() -> new KafkaObjectRunRetirementCertificateV1(
                        next,
                        nextSeal,
                        firstSeal.root(),
                        3,
                        Optional.of(ObjectKafkaTestFixtures.digest(25)),
                        protocolHead(next, nextSeal),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaObjectRunRetirementCertificateV1(
                        next,
                        nextSeal,
                        nextSeal.root(),
                        2,
                        Optional.of(ObjectKafkaTestFixtures.digest(25)),
                        protocolHead(next, nextSeal),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaObjectRunRetirementCertificateV1(
                        next, nextSeal, firstSeal.root(), 2, Optional.empty(), protocolHead(next, nextSeal), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaObjectRunRetirementCertificateV1(
                        first,
                        firstSeal,
                        firstSeal.root(),
                        2,
                        Optional.empty(),
                        protocolHead(first, firstSeal),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void exhaustedRootControlBudgetRejectsBeforeAnyCertificateIo() {
        var root = StorageObjectNwkcp1BackendV1Test.root();
        var sealed = seal(root);
        var boundary = new WalRunPredecessor(
                sealed.root(),
                WalRunControlKeys.sealKey(root.shardId(), 1),
                WalRunControlCodec.sealSha256(sealed),
                Optional.of(new TerminalProtocolCheckpointBindingV1(
                        ProtocolKindV1.KAFKA, "actual-aggregate-transfer", ObjectKafkaTestFixtures.digest(24))));
        var reads = new AtomicInteger();
        var store = new CanonicalControlMetadataStore() {
            @Override
            public Optional<CanonicalBytes> get(String key) {
                reads.incrementAndGet();
                throw new AssertionError("exhausted Root budget dispatched metadata I/O");
            }

            @Override
            public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
                throw new AssertionError("read verification mutated metadata");
            }

            @Override
            public ControlMutationOutcome compareAndSet(
                    String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
                throw new AssertionError("read verification mutated metadata");
            }
        };
        var authority = new KafkaObjectAuthorizationV1(
                store, root.shardId(), new KafkaObjectAuthorizationV1.Bounds(128, 8, 1024 * 1024, 1024));
        var limits = root.recoveryEnvelope();
        // A negative budget fixture only; no installed production Root or admission limit is altered.
        var budget = new CumulativeRecoveryBudget(
                new RecoveryEnvelopeLimits(
                        limits.maxLiveRoots(),
                        limits.maxPredecessorRuns(),
                        limits.maxListPages(),
                        limits.maxListedKeys(),
                        limits.maxListedKeyBytes(),
                        limits.maxHeadRequests(),
                        limits.maxRangeGetRequests(),
                        limits.maxFullGetRequests(),
                        1,
                        limits.maxDecodedContexts(),
                        limits.maxDecodedFrames(),
                        limits.maxDecodedCommitSets(),
                        limits.maxWorkingMemoryBytes(),
                        limits.maxConcurrency(),
                        limits.maxRetryAttempts(),
                        limits.maxWallTimeNanos()),
                () -> 0);
        assertThatThrownBy(() -> new KafkaObjectRunRecoveryRetirementV1(authority)
                        .isRetiredPrefix(boundary, successor(root, boundary), budget))
                .isInstanceOf(RecoveryEnvelopeExceededException.class);
        assertThat(reads).hasValue(0);
        assertThat(budget.snapshot().canonicalBodyBytes()).isZero();
    }

    // A wire-only value, deliberately not a confirmed protocol-domain closure capability.
    private static CanonicalBytes protocolHead(WalRunRootRecord root, WalRunSealRecord seal) {
        try {
            var bytes = new java.io.ByteArrayOutputStream();
            try (var out = new java.io.DataOutputStream(bytes)) {
                out.writeInt(0x4e4b4330);
                out.writeInt(1);
                var rootRaw = WalRunControlCodec.encodeRoot(root);
                var sealRaw = WalRunControlCodec.encodeSeal(seal);
                out.writeInt(rootRaw.length());
                out.write(rootRaw.toByteArray());
                out.writeInt(sealRaw.length());
                out.write(sealRaw.toByteArray());
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (java.io.IOException failure) {
            throw new AssertionError(failure);
        }
    }

    private static WalRunRootRecord successor(WalRunRootRecord root, WalRunPredecessor boundary) {
        return new WalRunRootRecord(
                root.shardId(),
                2,
                new Id128(27, 2),
                root.openedAtMillis(),
                root.protocolCellIdentity(),
                root.providerScopeId(),
                root.formatContract(),
                root.nwg1AdmissionCaps(),
                root.bounds(),
                root.checkpointPolicy(),
                root.providerConfiguration(),
                root.recoveryEnvelope(),
                root.wrappedRunKey(),
                Optional.of(boundary));
    }

    private static WalRunSealRecord seal(WalRunRootRecord root) {
        var reference = new WalRunReference(
                WalRunControlKeys.rootKey(root.shardId(), root.shardRunEpoch()),
                WalRunControlCodec.rootSha256(root),
                root.shardId(),
                root.shardRunEpoch());
        var head = WalCheckpointHeadV1.empty(reference.rootSha256(), root.shardRunEpoch(), 1);
        return new WalRunSealRecord(
                reference,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch()),
                Sha256Digest.hash(WalRunControlCodec.encodeCheckpointHead(head)),
                0,
                0);
    }
}
