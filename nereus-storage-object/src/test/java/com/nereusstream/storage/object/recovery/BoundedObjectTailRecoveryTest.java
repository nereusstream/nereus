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
import org.junit.jupiter.api.Test;

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
        private final Map<String, byte[]> objects = new LinkedHashMap<>();
        private int listCalls;
        private int lastListMaximumKeys;
        private int rangeGetCalls;
        private int fullGetCalls;
        private boolean failNextRange;

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
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            listCalls++;
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
