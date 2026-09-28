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

package com.nereusstream.storage.object.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.ObjectWalControlTestFixtures;
import com.nereusstream.storage.object.control.ObjectWalLeafKeyV1;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

class BoundedObjectTailRecoveryTest {
    @Test
    void listUsesCallerAndCumulativeRemainderBeforeNetwork() throws Exception {
        FakeTransport transport = new FakeTransport();
        ObjectIdentity identity = transport.store("cell-a/lane/object", 1, 2, 3);
        CumulativeRecoveryBudget budget = budget(2, 1, 1024, 2, 1, 100, 100);
        BoundedObjectTailRecovery recovery = new BoundedObjectTailRecovery(session(transport), budget);

        assertThat(recovery.discoverUncoveredLane("cell-a/lane/", 10, 100, 10_000)
                        .objects())
                .extracting(ObjectProviderTransport.ListedObject::key)
                .containsExactly(identity.key());

        assertThat(transport.listCalls).isEqualTo(1);
        assertThat(transport.lastListMaximumKeys).isEqualTo(1);
        assertThat(budget.snapshot().listPages()).isEqualTo(1);
        assertThat(budget.snapshot().listedKeys()).isEqualTo(1);
        assertThat(budget.snapshot().listedKeyBytes()).isEqualTo(identity.key().length());
        assertThat(budget.snapshot().headRequests()).isZero();
        assertThatThrownBy(() -> recovery.discoverUncoveredLane("cell-a/lane/", 10, 100, 10_000))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("LIST keys");
        assertThat(transport.listCalls).isEqualTo(1);
    }

    @Test
    void listPageExhaustionStopsBeforeAnUnreservedNetworkPage() {
        FakeTransport transport = new FakeTransport();
        transport.store("cell-a/lane/a", 1);
        transport.store("cell-a/lane/b", 2);
        CumulativeRecoveryBudget budget = budget(1, 10, 10_000, 2, 1, 100, 100);
        BoundedObjectTailRecovery recovery = new BoundedObjectTailRecovery(session(transport), budget);

        assertThatThrownBy(() -> recovery.discoverUncoveredLane("cell-a/lane/", 10, 100, 10_000))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("page bound");

        assertThat(transport.listCalls).isEqualTo(1);
        assertThat(budget.snapshot().listPages()).isEqualTo(1);
        assertThatThrownBy(() -> recovery.discoverUncoveredLane("cell-a/lane/", 1, 1, 1))
                .isInstanceOf(RecoveryEnvelopeExceededException.class);
        assertThat(transport.listCalls).isEqualTo(1);
    }

    @Test
    void listRequiresOneWholeExactRootLeafKeyAllowanceBeforeNetwork() {
        FakeTransport transport = new FakeTransport();
        transport.store("cell-a/lane/object", 1);
        CumulativeRecoveryBudget budget = budget(1, 1, 146, 2, 1, 100, 100);
        BoundedObjectTailRecovery recovery = new BoundedObjectTailRecovery(session(transport), budget);

        assertThatThrownBy(() -> recovery.discoverUncoveredLane("cell-a/lane/", 10, 100, 10_000))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before the next page");

        assertThat(transport.listCalls).isZero();
    }

    @Test
    void productionRootBoundInventoryParsesExactLeafAndRejectsRuntimeExpansion() throws Exception {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        FakeTransport transport = new FakeTransport();
        byte[] body = new byte[512];
        Arrays.fill(body, (byte) 7);
        Sha256Digest bodySha = Sha256Digest.hash(CanonicalBytes.copyOf(body));
        ObjectWalLeafKeyV1 leaf = new ObjectWalLeafKeyV1(WalLaneId.OBJECT_LATENCY, 0, 256, body.length, bodySha);
        ObjectIdentity identity = transport.store(leaf.fullKey(root.providerConfiguration()), body);
        BoundedObjectTailRecovery recovery = new BoundedObjectTailRecovery(rootSession(transport, root), root, () -> 0);

        BoundedObjectTailRecovery.RecoveredLaneInventory inventory =
                recovery.discoverUncoveredLane(WalLaneId.OBJECT_LATENCY);

        assertThat(inventory.extents()).hasSize(1);
        assertThat(inventory.extents().get(0).leaf()).isEqualTo(leaf);
        assertThat(inventory.extents().get(0).identity()).isEqualTo(identity);
        assertThat(recovery.snapshot().headRequests()).isZero();

        FakeTransport expandedTransport = new FakeTransport();
        expandedTransport.store(
                root.providerConfiguration().exclusiveNamespacePrefix() + "/0/not-an-object-wal-leaf", 1);
        BoundedObjectTailRecovery expanded =
                new BoundedObjectTailRecovery(rootSession(expandedTransport, root), root, () -> 0);
        assertThatThrownBy(() -> expanded.discoverUncoveredLane(WalLaneId.OBJECT_LATENCY))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("expanded outside");
    }

    @Test
    void rangeAndFullGetArePrechargedAndFailuresConsumeTheirBudget() throws Exception {
        FakeTransport transport = new FakeTransport();
        ObjectIdentity identity = transport.store("cell-a/lane/object", 1, 2, 3, 4);
        CumulativeRecoveryBudget budget = budget(2, 10, 1000, 1, 1, 100, 100);
        BoundedObjectTailRecovery recovery = new BoundedObjectTailRecovery(session(transport), budget);

        assertThat(recovery.reconstructDirectoryPrefixes(Map.of(identity, 3))
                        .get(identity)
                        .toByteArray())
                .containsExactly(1, 2, 3);
        assertThatThrownBy(() -> recovery.reconstructDirectoryPrefixes(Map.of(identity, 3)))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("range GET requests");
        assertThat(transport.rangeGetCalls).isEqualTo(1);

        assertThat(recovery.readVerifiedProtocolCheckpoint(identity).toByteArray())
                .containsExactly(1, 2, 3, 4);
        assertThatThrownBy(() -> recovery.readVerifiedProtocolCheckpoint(identity))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("full GET requests");
        assertThat(transport.fullGetCalls).isEqualTo(1);
        assertThat(budget.snapshot().canonicalBodyBytes()).isEqualTo(7);
        assertThat(budget.snapshot().headRequests()).isZero();

        FakeTransport failingTransport = new FakeTransport();
        ObjectIdentity failingIdentity = failingTransport.store("cell-a/lane/failing", 1, 2, 3, 4);
        failingTransport.failNextRange = true;
        CumulativeRecoveryBudget failingBudget = budget(2, 10, 1000, 1, 1, 100, 100);
        BoundedObjectTailRecovery failingRecovery =
                new BoundedObjectTailRecovery(session(failingTransport), failingBudget);

        assertThatThrownBy(() -> failingRecovery.reconstructDirectoryPrefixes(Map.of(failingIdentity, 3)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected range failure");
        assertThat(failingBudget.snapshot().rangeGetRequests()).isEqualTo(1);
        assertThat(failingBudget.snapshot().canonicalBodyBytes()).isEqualTo(3);
        assertThatThrownBy(() -> failingRecovery.reconstructDirectoryPrefixes(Map.of(failingIdentity, 3)))
                .isInstanceOf(RecoveryEnvelopeExceededException.class);
        assertThat(failingTransport.rangeGetCalls).isEqualTo(1);
    }

    @Test
    void workingSetIsAcquiredBeforeRequestBudgetOrNetwork() {
        FakeTransport transport = new FakeTransport();
        ObjectIdentity identity = transport.store("cell-a/lane/object", 1, 2, 3, 4);
        CumulativeRecoveryBudget budget = budget(2, 10, 1000, 1, 1, 100, 2);
        BoundedObjectTailRecovery recovery = new BoundedObjectTailRecovery(session(transport), budget);

        assertThatThrownBy(() -> recovery.reconstructDirectoryPrefixes(Map.of(identity, 3)))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("working memory bytes");

        assertThat(transport.rangeGetCalls).isZero();
        assertThat(budget.snapshot().rangeGetRequests()).isZero();
        assertThat(budget.snapshot().canonicalBodyBytes()).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void temporaryWorkingSetRejectionDoesNotSpendListOrPreventExactCandidateRetry(boolean protocol) throws Exception {
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var transport = new FakeTransport();
        byte[] body = new byte[512];
        var sha = Sha256Digest.hash(CanonicalBytes.copyOf(body));
        String key = protocol
                ? root.providerConfiguration().exclusiveNamespacePrefix()
                        + "/protocol/kafka/nwkcp1-v1/objects/sha256-v1-" + sha.toHex() + ".nwkcp1"
                : new ObjectWalLeafKeyV1(WalLaneId.OBJECT_LATENCY, 0, 256, body.length, sha)
                        .fullKey(root.providerConfiguration());
        var identity = transport.store(key, body);
        var recovery = new BoundedObjectTailRecovery(rootSession(transport, root), root, () -> 0);
        recovery.acquireWorkingSet(root.recoveryEnvelope().maxWorkingMemoryBytes());
        var before = recovery.snapshot();
        assertThatThrownBy(() -> {
                    if (protocol) {
                        recovery.reconcileUnknownProtocolObject(identity);
                    } else {
                        recovery.reconcileUnknownExtent(identity);
                    }
                })
                .hasMessageContaining("working memory bytes");
        assertThat(transport.listCalls).isZero();
        assertThat(transport.fullGetCalls).isZero();
        recovery.releaseWorkingSet(root.recoveryEnvelope().maxWorkingMemoryBytes());
        var result = protocol
                ? recovery.reconcileUnknownProtocolObject(identity)
                : recovery.reconcileUnknownExtent(identity);
        assertThat(result.outcome())
                .isEqualTo(com.nereusstream.storage.object.provider.ProviderObjectOutcome.EXISTING_EXACT);
        assertThat(recovery.snapshot().listPages()).isEqualTo(before.listPages() + 1);
        assertThat(recovery.snapshot().listedKeys()).isEqualTo(before.listedKeys() + 1);
        assertThat(recovery.snapshot().listedKeyBytes()).isEqualTo(before.listedKeyBytes() + key.length());
        assertThat(recovery.snapshot().fullGetRequests()).isEqualTo(before.fullGetRequests() + 1);
        assertThat(recovery.snapshot().currentConcurrency()).isZero();
        assertThat(recovery.snapshot().workingMemoryBytes()).isZero();
        assertThat(transport.listCalls).isOne();
        assertThat(transport.fullGetCalls).isOne();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentReconciliationOwnsExactIdentityWithoutLosingOtherCandidate(boolean protocol) throws Exception {
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var transport = new FakeTransport();
        var first = reconciliationIdentity(transport, root, protocol, 0);
        var second = reconciliationIdentity(transport, root, protocol, 1);
        var recovery = new BoundedObjectTailRecovery(rootSession(transport, root), root, () -> 0);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        transport.beforeFullGet = () -> {
            entered.countDown();
            try {
                assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new AssertionError(failure);
            }
        };
        var executor = Executors.newSingleThreadExecutor();
        try {
            var firstCall = executor.submit(() -> reconcile(recovery, first, protocol));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            var inFlight = recovery.snapshot();
            assertThatThrownBy(() -> reconcile(recovery, first, protocol)).hasMessageContaining("in-flight I/O");
            assertThat(recovery.snapshot()).isEqualTo(inFlight);
            long held = root.recoveryEnvelope().maxWorkingMemoryBytes() - inFlight.workingMemoryBytes();
            recovery.acquireWorkingSet(held);
            var occupied = recovery.snapshot();
            try {
                assertThatThrownBy(() -> reconcile(recovery, second, protocol))
                        .isInstanceOf(RecoveryEnvelopeExceededException.class);
                assertThat(recovery.snapshot()).isEqualTo(occupied);
                assertThat(transport.listCalls).isOne();
                assertThat(transport.fullGetCalls).isOne();
            } finally {
                recovery.releaseWorkingSet(held);
            }
            release.countDown();
            assertThat(firstCall.get(10, TimeUnit.SECONDS)).isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
            assertThat(reconcile(recovery, second, protocol)).isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
            assertThat(recovery.snapshot().listPages()).isEqualTo(2);
            assertThat(recovery.snapshot().fullGetRequests()).isEqualTo(2);
            assertThat(recovery.snapshot().retryAttempts()).isZero();
            assertThat(recovery.snapshot().currentConcurrency()).isZero();
            assertThat(recovery.snapshot().workingMemoryBytes()).isZero();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedListKeepsOriginalReservationAcrossTemporaryRejectionAndExactRetry(boolean protocol) throws Exception {
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var transport = new FakeTransport();
        var identity = reconciliationIdentity(transport, root, protocol, 0);
        var recovery = new BoundedObjectTailRecovery(rootSession(transport, root), root, () -> 0);
        transport.failNextList = true;
        assertThatThrownBy(() -> reconcile(recovery, identity, protocol))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected LIST failure");
        var failed = recovery.snapshot();
        assertThat(failed.listPages())
                .isEqualTo(protocol ? 1 : root.recoveryEnvelope().maxListPages());
        assertThat(failed.listedKeys())
                .isEqualTo(protocol ? 1 : root.recoveryEnvelope().maxListedKeys());
        assertThat(failed.listedKeyBytes())
                .isEqualTo(
                        protocol
                                ? identity.key().length()
                                : root.recoveryEnvelope().maxListedKeyBytes());
        assertThat(failed.fullGetRequests()).isOne();
        assertThat(failed.currentConcurrency()).isZero();
        assertThat(failed.workingMemoryBytes()).isZero();
        recovery.acquireWorkingSet(root.recoveryEnvelope().maxWorkingMemoryBytes());
        var occupied = recovery.snapshot();
        try {
            assertThatThrownBy(() -> reconcile(recovery, identity, protocol))
                    .hasMessageContaining("working memory bytes");
            assertThat(recovery.snapshot()).isEqualTo(occupied);
            assertThat(transport.listCalls).isOne();
            assertThat(transport.fullGetCalls).isZero();
        } finally {
            recovery.releaseWorkingSet(root.recoveryEnvelope().maxWorkingMemoryBytes());
        }
        assertThat(reconcile(recovery, identity, protocol)).isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
        assertThat(recovery.snapshot().listPages()).isOne();
        assertThat(recovery.snapshot().listedKeys()).isOne();
        assertThat(recovery.snapshot().listedKeyBytes())
                .isEqualTo(identity.key().length());
        assertThat(recovery.snapshot().fullGetRequests()).isEqualTo(2);
        assertThat(recovery.snapshot().retryAttempts()).isOne();
        assertThat(recovery.snapshot().currentConcurrency()).isZero();
        assertThat(recovery.snapshot().workingMemoryBytes()).isZero();
        assertThat(transport.listCalls).isEqualTo(2);
        assertThat(transport.fullGetCalls).isOne();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unknownFullGetKeepsKnownListChargeAndRetriesWithFreshBudget(boolean protocol) throws Exception {
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var transport = new FakeTransport();
        var identity = reconciliationIdentity(transport, root, protocol, 0);
        var recovery = new BoundedObjectTailRecovery(rootSession(transport, root), root, () -> 0);
        transport.failNextFullGet = true;
        assertThat(reconcile(recovery, identity, protocol)).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        assertThat(recovery.snapshot().listPages()).isOne();
        assertThat(recovery.snapshot().fullGetRequests()).isOne();
        assertThat(reconcile(recovery, identity, protocol)).isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
        assertThat(recovery.snapshot().listPages()).isEqualTo(2);
        assertThat(recovery.snapshot().listedKeys()).isEqualTo(2);
        assertThat(recovery.snapshot().listedKeyBytes())
                .isEqualTo(2L * identity.key().length());
        assertThat(recovery.snapshot().fullGetRequests()).isEqualTo(2);
        assertThat(recovery.snapshot().retryAttempts()).isOne();
        assertThat(recovery.snapshot().currentConcurrency()).isZero();
        assertThat(recovery.snapshot().workingMemoryBytes()).isZero();
        assertThat(transport.listCalls).isEqualTo(2);
        assertThat(transport.fullGetCalls).isEqualTo(2);
    }

    @Test
    void invalidSettledInventoryIsNotReturnedAsAnUnsettledReservation() {
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var transport = new FakeTransport();
        var identity = reconciliationIdentity(transport, root, false, 0);
        transport.store(root.providerConfiguration().exclusiveNamespacePrefix() + "/0/invalid-leaf", 1);
        var recovery = new BoundedObjectTailRecovery(rootSession(transport, root), root, () -> 0);
        for (int attempt = 1; attempt <= 2; attempt++) {
            assertThatThrownBy(() -> recovery.reconcileUnknownExtent(identity))
                    .hasMessageContaining("expanded outside");
            assertThat(recovery.snapshot().listPages()).isEqualTo(attempt * 2);
            assertThat(recovery.snapshot().listedKeys()).isEqualTo(attempt * 2);
            assertThat(recovery.snapshot().fullGetRequests()).isEqualTo(attempt);
            assertThat(recovery.snapshot().currentConcurrency()).isZero();
        }
    }

    private static ObjectIdentity reconciliationIdentity(
            FakeTransport transport, WalRunRootRecord root, boolean protocol, int seed) {
        byte[] body = new byte[512];
        Arrays.fill(body, (byte) seed);
        var sha = Sha256Digest.hash(CanonicalBytes.copyOf(body));
        String key = protocol
                ? root.providerConfiguration().exclusiveNamespacePrefix()
                        + "/protocol/kafka/nwkcp1-v1/objects/sha256-v1-" + sha.toHex() + ".nwkcp1"
                : new ObjectWalLeafKeyV1(
                                seed == 0 ? WalLaneId.OBJECT_LATENCY : WalLaneId.OBJECT_BALANCED,
                                0,
                                256,
                                body.length,
                                sha)
                        .fullKey(root.providerConfiguration());
        return transport.store(key, body);
    }

    private static ProviderObjectOutcome reconcile(
            BoundedObjectTailRecovery recovery, ObjectIdentity identity, boolean protocol) throws IOException {
        return (protocol
                        ? recovery.reconcileUnknownProtocolObject(identity)
                        : recovery.reconcileUnknownExtent(identity))
                .outcome();
    }

    private static C1ObjectProviderSession session(FakeTransport transport) {
        byte[] scope = new byte[Sha256Digest.LENGTH];
        Arrays.fill(scope, (byte) 1);
        return new C1ObjectProviderSession(
                transport, new CellProviderScopeId(Sha256Digest.copyOf(scope)), "cell-a", 1024 * 1024, 4096);
    }

    private static C1ObjectProviderSession rootSession(FakeTransport transport, WalRunRootRecord root) {
        return new C1ObjectProviderSession(
                transport,
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
    }

    private static CumulativeRecoveryBudget budget(
            int listPages,
            long listedKeys,
            long listedKeyBytes,
            int rangeGets,
            int fullGets,
            long canonicalBytes,
            long workingBytes) {
        RecoveryEnvelopeLimits limits = new RecoveryEnvelopeLimits(
                4,
                3,
                listPages,
                listedKeys,
                listedKeyBytes,
                10,
                rangeGets,
                fullGets,
                canonicalBytes,
                100,
                100,
                100,
                workingBytes,
                1,
                2,
                1_000_000);
        return new CumulativeRecoveryBudget(limits, () -> 0);
    }

    private static final class FakeTransport implements ObjectProviderTransport {
        @Override
        public FailureKind classifyFailure(IOException failure) {
            return failure.getMessage().equals("injected full GET failure")
                    ? FailureKind.OUTCOME_UNKNOWN
                    : FailureKind.FATAL;
        }

        private final Map<String, byte[]> objects = new LinkedHashMap<>();
        private int listCalls;
        private int lastListMaximumKeys;
        private int rangeGetCalls;
        private int fullGetCalls;
        private boolean failNextRange;
        private boolean failNextList;
        private boolean failNextFullGet;
        private volatile Runnable beforeFullGet;

        private ObjectIdentity store(String key, int... values) {
            byte[] bytes = new byte[values.length];
            for (int index = 0; index < values.length; index++) {
                bytes[index] = (byte) values[index];
            }
            objects.put(key, bytes);
            CanonicalBytes canonical = CanonicalBytes.copyOf(bytes);
            return new ObjectIdentity(key, bytes.length, Sha256Digest.hash(canonical));
        }

        private ObjectIdentity store(String key, byte[] value) {
            objects.put(key, value.clone());
            return new ObjectIdentity(key, value.length, Sha256Digest.hash(CanonicalBytes.copyOf(value)));
        }

        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("fake", true, true, true, true, true, 1024 * 1024, 4096, 16);
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException {
            fullGetCalls++;
            var hook = beforeFullGet;
            beforeFullGet = null;
            if (hook != null) {
                hook.run();
            }
            if (failNextFullGet) {
                failNextFullGet = false;
                throw new IOException("injected full GET failure");
            }
            byte[] value = required(key);
            return new StreamingObject(
                    value.length, 0, value.length, Optional.empty(), new ByteArrayInputStream(value));
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            rangeGetCalls++;
            if (failNextRange) {
                failNextRange = false;
                throw new IOException("injected range failure");
            }
            byte[] value = required(key);
            byte[] range = Arrays.copyOfRange(value, Math.toIntExact(inclusiveStart), Math.toIntExact(exclusiveEnd));
            return new StreamingObject(
                    value.length, inclusiveStart, exclusiveEnd, Optional.empty(), new ByteArrayInputStream(range));
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys)
                throws IOException {
            listCalls++;
            if (failNextList) {
                failNextList = false;
                throw new IOException("injected LIST failure");
            }
            lastListMaximumKeys = maximumKeys;
            List<String> keys = objects.keySet().stream()
                    .filter(key -> key.startsWith(prefix))
                    .sorted()
                    .toList();
            int start = continuationToken
                    .map(value -> ByteBuffer.wrap(value.toByteArray()).getInt())
                    .orElse(0);
            int end = Math.min(keys.size(), Math.addExact(start, Math.min(maximumKeys, 1)));
            ArrayList<ListedObject> page = new ArrayList<>();
            for (int index = start; index < end; index++) {
                String key = keys.get(index);
                page.add(new ListedObject(key, objects.get(key).length, Optional.empty()));
            }
            Optional<CanonicalBytes> next = end < keys.size()
                    ? Optional.of(CanonicalBytes.copyOf(
                            ByteBuffer.allocate(4).putInt(end).array()))
                    : Optional.empty();
            return new ListPage(page, next);
        }

        private byte[] required(String key) throws IOException {
            byte[] value = objects.get(key);
            if (value == null) {
                throw new IOException("missing fake Object: " + key);
            }
            return value;
        }
    }
}
