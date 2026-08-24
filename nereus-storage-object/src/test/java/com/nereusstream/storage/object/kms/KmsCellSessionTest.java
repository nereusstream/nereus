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

package com.nereusstream.storage.object.kms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.ObjectWalControlTestFixtures;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KmsCellSessionTest {
    @Test
    void oneWrapPerRunAndNoPerObjectKmsCall() {
        FakeKmsTransport transport = new FakeKmsTransport();
        KmsCellSession session = session(transport, 1, "kms/cell-a", 2);
        RunKeyCacheIdentity run = new RunKeyCacheIdentity(7, 1);
        WrappedRunKeyEnvelope envelope = session.createRunKey(run);

        CanonicalBytes first = session.deriveObjectKey(run, envelope, digest(3), WalLaneId.OBJECT_LATENCY, 0);
        CanonicalBytes second = session.deriveObjectKey(run, envelope, digest(3), WalLaneId.OBJECT_LATENCY, 1);

        assertThat(first.length()).isEqualTo(32);
        assertThat(second).isNotEqualTo(first);
        assertThat(session.wrapCalls()).isEqualTo(1);
        assertThat(session.unwrapCalls()).isZero();
        assertThat(transport.wrapCalls).isEqualTo(1);
        assertThat(transport.unwrapCalls).isZero();
    }

    @Test
    void recoveryUnwrapsOnceCachesByRunAndErasesOnClose() {
        FakeKmsTransport transport = new FakeKmsTransport();
        RunKeyCacheIdentity run = new RunKeyCacheIdentity(7, 1);
        KmsCellSession writer = session(transport, 1, "kms/cell-a", 2);
        WrappedRunKeyEnvelope envelope = writer.createRunKey(run);
        writer.close();

        KmsCellSession recovery = session(transport, 1, "kms/cell-a", 2);
        CanonicalBytes key = recovery.deriveObjectKey(run, envelope, digest(3), WalLaneId.OBJECT_COST, 9);
        assertThat(recovery.deriveObjectKey(run, envelope, digest(3), WalLaneId.OBJECT_COST, 9))
                .isEqualTo(key);
        assertThat(recovery.unwrapCalls()).isEqualTo(1);
        recovery.close();
        assertThat(recovery.cachedRunKeyCount()).isZero();
        assertThat(recovery.state()).isEqualTo(KmsCellSession.State.CLOSED);
        assertThatThrownBy(() -> recovery.deriveObjectKey(run, envelope, digest(3), WalLaneId.OBJECT_COST, 10))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void cellScopeAndKmsIdentityCannotBeSubstituted() {
        FakeKmsTransport transport = new FakeKmsTransport();
        RunKeyCacheIdentity run = new RunKeyCacheIdentity(7, 1);
        KmsCellSession cellA = session(transport, 1, "kms/cell-a", 1);
        WrappedRunKeyEnvelope envelope = cellA.createRunKey(run);
        KmsCellSession cellB = session(transport, 2, "kms/cell-b", 1);

        assertThatThrownBy(() -> cellB.deriveObjectKey(run, envelope, digest(3), WalLaneId.OBJECT_BALANCED, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside this Cell");
        assertThatThrownBy(() -> cellA.createRunKey(new RunKeyCacheIdentity(7, 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("capacity");
    }

    @Test
    void hkdfBindsRootShardEpochLaneAndSequence() {
        byte[] runKey = new byte[32];
        Arrays.fill(runKey, (byte) 7);
        CanonicalBytes baseline = ObjectKeyDerivationV1.derive(runKey, digest(1), 1, 2, WalLaneId.OBJECT_LATENCY, 3);

        assertThat(ObjectKeyDerivationV1.INFO_BYTES).isEqualTo(37);
        assertThat(ObjectKeyDerivationV1.derive(runKey, digest(2), 1, 2, WalLaneId.OBJECT_LATENCY, 3))
                .isNotEqualTo(baseline);
        assertThat(ObjectKeyDerivationV1.derive(runKey, digest(1), 1, 2, WalLaneId.OBJECT_COST, 3))
                .isNotEqualTo(baseline);
        assertThat(ObjectKeyDerivationV1.derive(runKey, digest(1), 1, 2, WalLaneId.OBJECT_LATENCY, 4))
                .isNotEqualTo(baseline);
    }

    @Test
    void wrappedEnvelopeIsExactFiveFieldCanonicalAndRejectsMutableAliasOrCodeMutation() {
        WrappedRunKeyEnvelope envelope = new WrappedRunKeyEnvelope(
                "fake-kms", "aes-kw-v1", "kms/cell-a", "version-7", CanonicalBytes.copyOf(new byte[] {1, 2, 3, 4}));

        assertThat(WrappedRunKeyEnvelope.decodeFramed(envelope.framedBytes())).isEqualTo(envelope);
        assertThat(envelope.canonicalBytes().length()).isEqualTo(20 + 8 + 9 + 10 + 9 + 4);
        assertThatThrownBy(() -> new WrappedRunKeyEnvelope(
                        "fake-kms", "aes-kw-v1", "kms/cell-a", "current", CanonicalBytes.copyOf(new byte[] {1})))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutable key-version alias");
        byte[] mutated = envelope.framedBytes().toByteArray();
        mutated[1] = 2;
        assertThatThrownBy(() -> WrappedRunKeyEnvelope.decodeFramed(CanonicalBytes.copyOf(mutated)))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void perRunWalLeasesFenceOnlyTheirRunAndCloseCannotEraseOrCloseSibling() {
        FakeKmsTransport transport = new FakeKmsTransport();
        KmsCellSession cell = session(transport, 1, "kms/cell-a", 2);
        WalRunRootRecord fixtureA = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunRootRecord fixtureB = ObjectWalControlTestFixtures.root(2, Optional.empty());
        RunKeyCacheIdentity runA = new RunKeyCacheIdentity(fixtureA.shardId(), fixtureA.shardRunEpoch());
        RunKeyCacheIdentity runB = new RunKeyCacheIdentity(fixtureB.shardId(), fixtureB.shardRunEpoch());
        WrappedRunKeyEnvelope envelopeA = fixtureA.wrappedRunKey();
        WrappedRunKeyEnvelope envelopeB = fixtureB.wrappedRunKey();
        transport.register(envelopeA, 11);
        transport.register(envelopeB, 22);
        cell.deriveObjectKey(runA, envelopeA, digest(3), WalLaneId.OBJECT_COST, 0);
        cell.deriveObjectKey(runB, envelopeB, digest(3), WalLaneId.OBJECT_COST, 0);
        WalRunObjectSession ownerA =
                ObjectWalControlTestFixtures.openIsolatedSession(fixtureA, provider(fixtureA), cell, () -> 0);
        assertThat(cell.deriveObjectKey(runB, envelopeB, digest(3), WalLaneId.OBJECT_COST, 1)
                        .length())
                .isEqualTo(32);
        WalRunObjectSession ownerB =
                ObjectWalControlTestFixtures.openIsolatedSession(fixtureB, provider(fixtureB), cell, () -> 0);

        assertThatThrownBy(() -> cell.deriveObjectKey(runA, envelopeA, digest(3), WalLaneId.OBJECT_COST, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(() -> cell.deriveObjectKey(runB, envelopeB, digest(3), WalLaneId.OBJECT_COST, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(cell::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lease is live");
        assertThat(cell.cachedRunKeyCount()).isEqualTo(2);

        ownerA.close();

        assertThat(cell.state()).isEqualTo(KmsCellSession.State.OPEN);
        assertThat(cell.cachedRunKeyCount()).isEqualTo(1);
        assertThatThrownBy(cell::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lease is live");

        ownerB.close();
        assertThat(cell.cachedRunKeyCount()).isZero();
        cell.close();
        assertThat(cell.state()).isEqualTo(KmsCellSession.State.CLOSED);
    }

    @Test
    void emptyLeaseCacheSlotIsReservedAgainstUnrelatedRawRunAdmission() {
        FakeKmsTransport transport = new FakeKmsTransport();
        KmsCellSession cell = session(transport, 1, "kms/cell-a", 2);
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunObjectSession owner =
                ObjectWalControlTestFixtures.openIsolatedSession(root, provider(root), cell, () -> 0);
        RunKeyCacheIdentity rawB = new RunKeyCacheIdentity(root.shardId(), 20);
        RunKeyCacheIdentity rawC = new RunKeyCacheIdentity(root.shardId(), 21);

        cell.createRunKey(rawB);
        assertThatThrownBy(() -> cell.createRunKey(rawC))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("capacity");

        owner.close();
        assertThat(cell.createRunKey(rawC)).isNotNull();
        cell.evict(rawB);
        cell.evict(rawC);
        cell.close();
    }

    @Test
    void closedRunTombstoneBoundsTransferHistoryUntilQuiescentCellIsRecreated() {
        FakeKmsTransport transport = new FakeKmsTransport();
        WalRunRootRecord rootA = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunRootRecord rootB = ObjectWalControlTestFixtures.root(2, Optional.empty());
        KmsCellSession exhausted = session(transport, 1, "kms/cell-a", 1);
        WalRunObjectSession ownerA =
                ObjectWalControlTestFixtures.openIsolatedSession(rootA, provider(rootA), exhausted, () -> 0);
        ownerA.close();
        C1ObjectProviderSession providerB = provider(rootB);

        assertThatThrownBy(() -> ObjectWalControlTestFixtures.openIsolatedSession(rootB, providerB, exhausted, () -> 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("history capacity");
        assertThat(providerB.state()).isEqualTo(C1ObjectProviderSession.State.OPEN);
        assertThat(exhausted.state()).isEqualTo(KmsCellSession.State.OPEN);
        assertThat(exhausted.cachedRunKeyCount()).isZero();
        assertThat(transport.unwrapCalls).isZero();

        exhausted.close();
        KmsCellSession replacement = session(transport, 1, "kms/cell-a", 1);
        WalRunObjectSession ownerB =
                ObjectWalControlTestFixtures.openIsolatedSession(rootB, providerB, replacement, () -> 0);
        ownerB.close();
        replacement.close();
        assertThat(replacement.state()).isEqualTo(KmsCellSession.State.CLOSED);
    }

    @Test
    void selectedStreamingLeaseSurfaceNeverAcceptsOrReturnsPlaintextRunKeyOrFrameCollections() {
        for (Class<?> leaseType : List.of(KmsCellSession.RecoveryLease.class, KmsCellSession.WalRunLease.class)) {
            List<Method> selectedMethods = Arrays.stream(leaseType.getDeclaredMethods())
                    .filter(method -> method.getName().equals("readSelectedAppendUnitStreaming"))
                    .toList();
            assertThat(selectedMethods).hasSize(1);
            Method method = selectedMethods.getFirst();
            assertThat(method.getReturnType()).isEqualTo(Nwg1ObjectReaderV1.VerifiedAppendUnit.class);
            assertThat(method.getParameterTypes())
                    .containsExactly(
                            Nwg1ObjectReaderV1.AuthenticatedPrefix.class,
                            Nwg1ObjectReaderV1.ExactFrameSource.class,
                            long.class,
                            Nwg1VerificationContextV1.class,
                            Nwg1ObjectReaderV1.VerifiedFrameConsumer.class)
                    .doesNotContain(byte[].class, List.class);
            assertThat(Arrays.stream(leaseType.getMethods()).map(Method::getName))
                    .doesNotContain("readSelectedAppendUnit");
        }

        assertThat(Arrays.stream(Nwg1ObjectReaderV1.VerifiedAppendUnit.class.getRecordComponents())
                        .map(RecordComponent::getType))
                .doesNotContain(List.class, Nwg1ObjectReaderV1.DecodedObject.class);
    }

    private static C1ObjectProviderSession provider(WalRunRootRecord root) {
        return new C1ObjectProviderSession(
                new NoIoProviderTransport(),
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
    }

    private static KmsCellSession session(FakeKmsTransport transport, int scopeSeed, String keyIdentity, int cap) {
        return new KmsCellSession(
                transport, new CellProviderScopeId(digest(scopeSeed)), keyIdentity, cap, new FixedRandom());
    }

    private static Sha256Digest digest(int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return Sha256Digest.copyOf(value);
    }

    private static final class FixedRandom extends SecureRandom {
        @Override
        public void nextBytes(byte[] bytes) {
            for (int index = 0; index < bytes.length; index++) {
                bytes[index] = (byte) (17 + index);
            }
        }
    }

    private static final class FakeKmsTransport implements KmsTransport {
        private final Map<CanonicalBytes, String> keys = new LinkedHashMap<>();
        private final Map<CanonicalBytes, byte[]> registeredPlaintexts = new LinkedHashMap<>();
        private int wrapCalls;
        private int unwrapCalls;

        @Override
        public WrappedRunKeyEnvelope wrap(String keyIdentity, byte[] plaintextRunKey) {
            wrapCalls++;
            byte[] wrapped = plaintextRunKey.clone();
            for (int index = 0; index < wrapped.length; index++) {
                wrapped[index] ^= (byte) 0xa5;
            }
            CanonicalBytes ciphertext = CanonicalBytes.copyOf(wrapped);
            keys.put(ciphertext, keyIdentity);
            return new WrappedRunKeyEnvelope("fake-kms", "xor-test-v1", keyIdentity, "version-1", ciphertext);
        }

        @Override
        public byte[] unwrap(WrappedRunKeyEnvelope envelope) {
            unwrapCalls++;
            String storedKeyIdentity = keys.get(envelope.wrappedKey());
            if (storedKeyIdentity == null || !storedKeyIdentity.equals(envelope.wrappingKeyId())) {
                throw new IllegalArgumentException("KMS envelope identity mismatch");
            }
            byte[] registered = registeredPlaintexts.get(envelope.wrappedKey());
            if (registered != null) {
                return registered.clone();
            }
            byte[] plaintext = envelope.wrappedKey().toByteArray();
            for (int index = 0; index < plaintext.length; index++) {
                plaintext[index] ^= (byte) 0xa5;
            }
            return plaintext;
        }

        private void register(WrappedRunKeyEnvelope envelope, int fill) {
            keys.put(envelope.wrappedKey(), envelope.wrappingKeyId());
            byte[] plaintext = new byte[ObjectKeyDerivationV1.RUN_KEY_BYTES];
            Arrays.fill(plaintext, (byte) fill);
            registeredPlaintexts.put(envelope.wrappedKey(), plaintext);
        }
    }

    private static final class NoIoProviderTransport implements ObjectProviderTransport {
        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("no-io", true, true, true, true, true, 1024 * 1024, 4096, 100);
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) {
            throw new AssertionError("unexpected Provider PUT");
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) {
            throw new AssertionError("unexpected Provider full GET");
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken) {
            throw new AssertionError("unexpected Provider range GET");
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            throw new AssertionError("unexpected Provider LIST");
        }
    }
}
