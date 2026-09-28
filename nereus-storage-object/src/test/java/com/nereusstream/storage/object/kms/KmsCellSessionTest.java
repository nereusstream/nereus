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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    void admissionSuccessorRetainsOldRawFacadeFenceWithoutIncreasingCapacity() {
        var transport = new FakeKmsTransport();
        var rootA = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var rootB = ObjectWalControlTestFixtures.root(2, Optional.empty());
        var original = session(transport, 1, "kms/cell-a", 1);
        var ownerA = ObjectWalControlTestFixtures.openIsolatedSession(rootA, provider(rootA), original, () -> 0);
        ownerA.close();
        var ownerB = ObjectWalControlTestFixtures.openIsolatedSession(rootB, provider(rootB), original, () -> 0);
        assertThat(original.state()).isEqualTo(KmsCellSession.State.CLOSED);
        assertThatThrownBy(() -> original.deriveObjectKey(
                        new RunKeyCacheIdentity(7, 1), rootA.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0))
                .hasMessageContaining("no longer accepts operations");
        assertThatThrownBy(original::close).hasMessageContaining("lease is live");
        assertThat(original.resourceSnapshot().runSlots()).isOne();
        assertThat(original.resourceSnapshot().generations()).isOne();
        ownerB.close();
        original.close();
        assertThat(original.resourceSnapshot().runSlots()).isZero();
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void blockedKmsCallDoesNotBlockReadySibling(boolean wrap) throws Exception {
        var transport = new FakeKmsTransport();
        var cell = session(transport, 1, "kms/cell-a", 3);
        var ready = new RunKeyCacheIdentity(7, 1);
        var readyEnvelope = cell.createRunKey(ready);
        var pending = new RunKeyCacheIdentity(7, 2);
        var envelope = ObjectWalControlTestFixtures.root(2, Optional.empty()).wrappedRunKey();
        transport.register(envelope, 22);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Runnable block = () -> {
            entered.countDown();
            await(release);
        };
        if (wrap) {
            transport.beforeWrap = block;
        } else {
            transport.beforeUnwrap = block;
        }
        var executor = Executors.newFixedThreadPool(2);
        try {
            var blocked = executor.submit(() -> wrap
                    ? cell.createRunKey(pending)
                    : cell.deriveObjectKey(pending, envelope, digest(3), WalLaneId.OBJECT_COST, 0));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            var sibling = executor.submit(
                    () -> cell.deriveObjectKey(ready, readyEnvelope, digest(3), WalLaneId.OBJECT_COST, 0));
            assertThat(sibling.get(1, TimeUnit.SECONDS).length()).isEqualTo(32);
            release.countDown();
            assertThat(blocked.get(10, TimeUnit.SECONDS)).isNotNull();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            cell.close();
        }
    }

    @Test
    void longLivedRunDoesNotPreventContinuousRunRotation() {
        var transport = new FakeKmsTransport();
        var cell = session(transport, 1, "kms/cell-a", 2);
        var longRoot = ObjectWalControlTestFixtures.root(1, Optional.empty());
        transport.register(longRoot.wrappedRunKey(), 11);
        var longIdentity = new RunKeyCacheIdentity(longRoot.shardId(), longRoot.shardRunEpoch());
        cell.deriveObjectKey(longIdentity, longRoot.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0);
        var longOwner = ObjectWalControlTestFixtures.openIsolatedSession(longRoot, provider(longRoot), cell, () -> 0);
        try {
            for (int epoch = 2; epoch <= 12; epoch++) {
                var root = ObjectWalControlTestFixtures.root(epoch, Optional.empty());
                var owner = ObjectWalControlTestFixtures.openIsolatedSession(root, provider(root), cell, () -> 0);
                owner.close();
                assertThat(longOwner.state()).isEqualTo(WalRunObjectSession.State.OPEN);
                var resources = cell.resourceSnapshot();
                assertThat(resources.runSlots()).isOne();
                assertThat(resources.activeOperations()).isZero();
                assertThat(resources.pendingLoads()).isZero();
                assertThat(resources.generations()).isLessThanOrEqualTo(3);
                assertThat(resources.history()).isLessThanOrEqualTo(6);
                assertThatThrownBy(() -> cell.deriveObjectKey(
                                longIdentity, longRoot.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0))
                        .isInstanceOf(IllegalStateException.class);
            }
        } finally {
            longOwner.close();
            cell.close();
        }
    }

    @Test
    void admissionGenerationsShareTheOriginalActiveRunCapacity() {
        var cell = session(new FakeKmsTransport(), 1, "kms/cell-a", 2);
        var firstRoot = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var secondRoot = ObjectWalControlTestFixtures.root(2, Optional.empty());
        var thirdRoot = ObjectWalControlTestFixtures.root(3, Optional.empty());
        var first = ObjectWalControlTestFixtures.openIsolatedSession(firstRoot, provider(firstRoot), cell, () -> 0);
        var second = ObjectWalControlTestFixtures.openIsolatedSession(secondRoot, provider(secondRoot), cell, () -> 0);
        var rejectedProvider = provider(thirdRoot);
        assertThatThrownBy(() ->
                        ObjectWalControlTestFixtures.openIsolatedSession(thirdRoot, rejectedProvider, cell, () -> 0))
                .hasMessageContaining("cache capacity");
        rejectedProvider.close();
        assertThat(cell.resourceSnapshot().runSlots()).isEqualTo(2);
        assertThat(cell.resourceSnapshot().generations()).isEqualTo(2);
        first.close();
        var third = ObjectWalControlTestFixtures.openIsolatedSession(thirdRoot, provider(thirdRoot), cell, () -> 0);
        assertThat(second.state()).isEqualTo(WalRunObjectSession.State.OPEN);
        assertThat(cell.resourceSnapshot().runSlots()).isEqualTo(2);
        second.close();
        third.close();
        assertThat(cell.resourceSnapshot().runSlots()).isZero();
        cell.close();
    }

    @Test
    void retiredRawCellCannotCreateTheNextRootKeyDespiteAnAvailableSharedSlot() {
        var cell = session(new FakeKmsTransport(), 1, "kms/cell-a", 2);
        var firstRoot = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var secondRoot = ObjectWalControlTestFixtures.root(2, Optional.empty());
        var thirdRoot = ObjectWalControlTestFixtures.root(3, Optional.empty());
        var first = ObjectWalControlTestFixtures.openIsolatedSession(firstRoot, provider(firstRoot), cell, () -> 0);
        try {
            ObjectWalControlTestFixtures.openIsolatedSession(secondRoot, provider(secondRoot), cell, () -> 0)
                    .close();
            ObjectWalControlTestFixtures.openIsolatedSession(thirdRoot, provider(thirdRoot), cell, () -> 0)
                    .close();
            assertThat(cell.state()).isEqualTo(KmsCellSession.State.DRAINING);
            assertThat(cell.resourceSnapshot().runSlots()).isOne();
            assertThatThrownBy(() -> cell.createRunKey(new RunKeyCacheIdentity(7, 4)))
                    .hasMessageContaining("no longer accepts operations");
            try (var creation = cell.beginNewRunKey(new RunKeyCacheIdentity(7, 4))) {
                assertThat(creation.wrappedRunKey().wrappingKeyId()).isEqualTo("kms/cell-a");
                assertThat(cell.resourceSnapshot().runSlots()).isEqualTo(2);
                assertThatThrownBy(cell::close).hasMessageContaining("lease is live");
            }
            assertThat(cell.resourceSnapshot().runSlots()).isOne();
        } finally {
            first.close();
            cell.close();
        }
    }

    @Test
    void newRootWrapFailureAndUnpublishedCancellationReleaseTheExactSlot() {
        var transport = new FakeKmsTransport();
        var cell = session(transport, 1, "kms/cell-a", 1);
        var identity = new RunKeyCacheIdentity(7, 1);
        transport.beforeWrap = () -> {
            throw new IllegalStateException("wrap unavailable");
        };
        assertThatThrownBy(() -> cell.beginNewRunKey(identity)).hasMessageContaining("wrap unavailable");
        assertThat(transport.lastWrapRequest).containsOnly((byte) 0);
        assertThat(cell.resourceSnapshot().runSlots()).isZero();
        assertThat(cell.resourceSnapshot().pendingLoads()).isZero();
        assertThat(cell.resourceSnapshot().activeOperations()).isZero();
        try (var cancelled = cell.beginNewRunKey(identity)) {
            assertThat(cancelled.wrappedRunKey()).isNotNull();
            assertThat(cell.resourceSnapshot().runSlots()).isOne();
        }
        assertThat(cell.resourceSnapshot().runSlots()).isZero();
        assertThat(cell.resourceSnapshot().history()).isZero();
        try (var retry = cell.beginNewRunKey(identity)) {
            assertThat(retry.wrappedRunKey()).isNotNull();
        }
        cell.close();
    }

    @Test
    void newRootCreationCannotReplaceAnExistingRawKeyForTheSameRun() {
        var cell = session(new FakeKmsTransport(), 1, "kms/cell-a", 1);
        var identity = new RunKeyCacheIdentity(7, 1);
        var rawEnvelope = cell.createRunKey(identity);
        assertThatThrownBy(() -> cell.beginNewRunKey(identity)).hasMessageContaining("pristine run identity");
        assertThat(cell.cachedRunKeyCount()).isOne();
        assertThat(cell.resourceSnapshot().runSlots()).isOne();
        assertThat(cell.deriveObjectKey(identity, rawEnvelope, digest(3), WalLaneId.OBJECT_COST, 0)
                        .length())
                .isEqualTo(32);
        cell.close();
    }

    @Test
    void acceptedPreRootWrapSurvivesAdmissionRetirement() throws Exception {
        var transport = new FakeKmsTransport();
        var cell = session(transport, 1, "kms/cell-a", 3);
        var firstRoot = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var secondRoot = ObjectWalControlTestFixtures.root(2, Optional.empty());
        var fourthRoot = ObjectWalControlTestFixtures.root(4, Optional.empty());
        var first = ObjectWalControlTestFixtures.openIsolatedSession(firstRoot, provider(firstRoot), cell, () -> 0);
        ObjectWalControlTestFixtures.openIsolatedSession(secondRoot, provider(secondRoot), cell, () -> 0)
                .close();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        transport.beforeWrap = () -> {
            entered.countDown();
            await(release);
        };
        var executor = Executors.newSingleThreadExecutor();
        try {
            var pending = executor.submit(() -> cell.beginNewRunKey(new RunKeyCacheIdentity(7, 3)));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(cell.resourceSnapshot().pendingLoads()).isOne();
            assertThat(cell.resourceSnapshot().activeOperations()).isOne();
            var fourth =
                    ObjectWalControlTestFixtures.openIsolatedSession(fourthRoot, provider(fourthRoot), cell, () -> 0);
            assertThat(cell.state()).isEqualTo(KmsCellSession.State.DRAINING);
            assertThat(cell.resourceSnapshot().runSlots()).isEqualTo(3);
            assertThatThrownBy(cell::close).hasMessageContaining("actual key operations");
            release.countDown();
            try (var created = pending.get(10, TimeUnit.SECONDS)) {
                assertThat(created.wrappedRunKey().wrappingKeyId()).isEqualTo("kms/cell-a");
                assertThat(cell.resourceSnapshot().pendingLoads()).isZero();
                assertThat(cell.resourceSnapshot().activeOperations()).isZero();
                assertThat(transport.lastWrapRequest).containsOnly((byte) 0);
                assertThatThrownBy(cell::close).hasMessageContaining("lease is live");
            }
            fourth.close();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            first.close();
            cell.close();
        }
    }

    @Test
    void concurrentMissSharesUnwrapAndInterruptedWaiterCannotReleaseItsActualWork() throws Exception {
        var transport = new FakeKmsTransport();
        var cell = session(transport, 1, "kms/cell-a", 3);
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var identity = new RunKeyCacheIdentity(7, 1);
        transport.register(root.wrappedRunKey(), 11);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        transport.beforeUnwrap = () -> {
            entered.countDown();
            await(release);
        };
        var executor = Executors.newFixedThreadPool(3);
        try {
            var first = executor.submit(
                    () -> cell.deriveObjectKey(identity, root.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            var waiter = executor.submit(
                    () -> cell.deriveObjectKey(identity, root.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0));
            var otherWaiter = executor.submit(
                    () -> cell.deriveObjectKey(identity, root.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0));
            awaitOperations(cell, 3);
            assertThatThrownBy(() -> cell.deriveObjectKey(
                            new RunKeyCacheIdentity(7, 2), root.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0))
                    .hasMessageContaining("active operation capacity");
            assertThat(cell.resourceSnapshot().pendingLoads()).isOne();
            assertThat(transport.unwrapCalls).isOne();
            assertThatThrownBy(() -> cell.evict(identity)).hasMessageContaining("actual key operations");
            assertThatThrownBy(cell::drain).hasMessageContaining("actual key operations");
            waiter.cancel(true);
            awaitOperations(cell, 2);
            assertThat(cell.resourceSnapshot().runSlots()).isOne();
            assertThat(cell.resourceSnapshot().pendingLoads()).isOne();
            assertThatThrownBy(cell::close).hasMessageContaining("actual key operations");
            release.countDown();
            var key = first.get(10, TimeUnit.SECONDS);
            assertThat(otherWaiter.get(10, TimeUnit.SECONDS)).isEqualTo(key);
            assertThat(cell.deriveObjectKey(identity, root.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0))
                    .isEqualTo(key);
            assertThat(transport.unwrapCalls).isOne();
            assertThat(transport.lastUnwrapped).containsOnly((byte) 0);
            assertThat(cell.resourceSnapshot().activeOperations()).isZero();
            assertThat(cell.resourceSnapshot().pendingLoads()).isZero();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            cell.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedKeyLoadReleasesPendingSlotAndAllowsExactRetry(boolean wrap) {
        var transport = new FakeKmsTransport();
        var cell = session(transport, 1, "kms/cell-a", 1);
        var identity = new RunKeyCacheIdentity(7, 1);
        var envelope = ObjectWalControlTestFixtures.root(1, Optional.empty()).wrappedRunKey();
        transport.register(envelope, 11);
        Runnable fail = () -> {
            throw new IllegalStateException("KMS unavailable");
        };
        if (wrap) {
            transport.beforeWrap = fail;
        } else {
            transport.beforeUnwrap = fail;
        }
        assertThatThrownBy(() -> {
                    if (wrap) {
                        cell.createRunKey(identity);
                    } else {
                        cell.deriveObjectKey(identity, envelope, digest(3), WalLaneId.OBJECT_COST, 0);
                    }
                })
                .hasMessageContaining("KMS unavailable");
        assertThat(cell.resourceSnapshot().runSlots()).isZero();
        assertThat(cell.resourceSnapshot().pendingLoads()).isZero();
        assertThat(cell.resourceSnapshot().activeOperations()).isZero();
        if (wrap) {
            assertThat(transport.lastWrapRequest).containsOnly((byte) 0);
            assertThat(cell.createRunKey(identity)).isNotNull();
            assertThat(cell.wrapCalls()).isEqualTo(2);
        } else {
            assertThat(cell.deriveObjectKey(identity, envelope, digest(3), WalLaneId.OBJECT_COST, 0)
                            .length())
                    .isEqualTo(32);
            assertThat(transport.lastUnwrapped).containsOnly((byte) 0);
            assertThat(cell.unwrapCalls()).isEqualTo(2);
        }
        cell.evict(identity);
        cell.close();
    }

    @Test
    void cancellingUnwrapCallerDoesNotReleaseActualTransportOrAllowTransfer() throws Exception {
        var transport = new FakeKmsTransport();
        var cell = session(transport, 1, "kms/cell-a", 1);
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var identity = new RunKeyCacheIdentity(7, 1);
        transport.register(root.wrappedRunKey(), 11);
        var entered = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var finished = new CountDownLatch(1);
        transport.beforeUnwrap = () -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException failure) {
                interrupted.countDown();
                // A transport may finish its actual call after the caller has stopped waiting.
                await(release);
            }
        };
        var executor = Executors.newSingleThreadExecutor();
        try {
            var caller = executor.submit(() -> {
                try {
                    return cell.deriveObjectKey(identity, root.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0);
                } finally {
                    finished.countDown();
                }
            });
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(caller.cancel(true)).isTrue();
            assertThat(interrupted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(cell.resourceSnapshot().activeOperations()).isOne();
            assertThat(cell.resourceSnapshot().pendingLoads()).isOne();
            assertThat(cell.resourceSnapshot().runSlots()).isOne();
            assertThatThrownBy(() -> cell.evict(identity)).hasMessageContaining("actual key operations");
            assertThatThrownBy(cell::close).hasMessageContaining("actual key operations");
            assertThatThrownBy(
                            () -> ObjectWalControlTestFixtures.openIsolatedSession(root, provider(root), cell, () -> 0))
                    .hasMessageContaining("actual key operations");
            release.countDown();
            assertThat(finished.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(cell.resourceSnapshot().activeOperations()).isZero();
            assertThat(cell.resourceSnapshot().pendingLoads()).isZero();
            assertThat(transport.lastUnwrapped).containsOnly((byte) 0);
            assertThat(cell.deriveObjectKey(identity, root.wrappedRunKey(), digest(3), WalLaneId.OBJECT_COST, 0)
                            .length())
                    .isEqualTo(32);
            assertThat(transport.unwrapCalls).isOne();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            cell.close();
        }
    }

    private static void awaitOperations(KmsCellSession cell, int count) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (cell.resourceSnapshot().activeOperations() != count && System.nanoTime() < end) {
            Thread.sleep(10);
        }
        assertThat(cell.resourceSnapshot().activeOperations()).isEqualTo(count);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(15, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
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
        private volatile Runnable beforeWrap;
        private volatile Runnable beforeUnwrap;
        private byte[] lastWrapRequest;
        private byte[] lastUnwrapped;

        @Override
        public WrappedRunKeyEnvelope wrap(String keyIdentity, byte[] plaintextRunKey) {
            wrapCalls++;
            lastWrapRequest = plaintextRunKey;
            var hook = beforeWrap;
            beforeWrap = null;
            if (hook != null) {
                hook.run();
            }
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
            var hook = beforeUnwrap;
            beforeUnwrap = null;
            if (hook != null) {
                hook.run();
            }
            String storedKeyIdentity = keys.get(envelope.wrappedKey());
            if (storedKeyIdentity == null || !storedKeyIdentity.equals(envelope.wrappingKeyId())) {
                throw new IllegalArgumentException("KMS envelope identity mismatch");
            }
            byte[] registered = registeredPlaintexts.get(envelope.wrappedKey());
            if (registered != null) {
                lastUnwrapped = registered.clone();
                return lastUnwrapped;
            }
            byte[] plaintext = envelope.wrappedKey().toByteArray();
            for (int index = 0; index < plaintext.length; index++) {
                plaintext[index] ^= (byte) 0xa5;
            }
            lastUnwrapped = plaintext;
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
