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

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Native quota transactions on isolated actual ZooKeeper fixture namespaces; full BK/Oxia is covered separately. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5BookKeeperNativeDeleteQuotaV2RealTest {
    @Test
    void firstEpochAndCapacityAreAtomicAtExhaustionWithLostReplyAndExplicitExpansion() throws Exception {
        try (var f = new Fixture()) {
            var quota = f.quota;
            long capacity = quota.capacityForResources(1);
            f.zk.loseNextMulti = true;
            var initial = await(quota.initialize(capacity));
            assertThat(initial.reservedResources()).isZero();
            assertThat(f.zk.lost.get()).isEqualTo(1);
            var epoch = f.epoch(1);
            var concurrent = f.epoch(2);
            var firstOps = await(quota.firstClaimChecks(true, epoch));
            var staleOps = await(quota.firstClaimChecks(true, concurrent));
            f.zk.loseNextMulti = true;
            assertThat(await(f.commit(firstOps, epoch))).isEqualTo(KeeperException.Code.CONNECTIONLOSS.intValue());
            var full = await(quota.snapshot());
            assertThat(full.reservedResources()).isEqualTo(1);
            assertThat(quota.chargedBytes(full)).isEqualTo(capacity);
            assertThat(f.zk.getData(quota.epochPath(1), false, null))
                    .isEqualTo(epoch.encode().toByteArray());
            assertThat(await(f.commit(staleOps, concurrent))).isEqualTo(KeeperException.Code.BADVERSION.intValue());
            assertThat(f.zk.exists(quota.epochPath(2), false)).isNull();
            assertThat(await(quota.snapshot())).isEqualTo(full);
            assertThatThrownBy(() -> await(quota.firstClaimChecks(true, concurrent)))
                    .hasRootCauseMessage("native GC quota exhausted");
            await(quota.requireReserved(true));
            assertThat(await(quota.initialize(capacity))).isEqualTo(full);
            assertThatThrownBy(() -> await(quota.initialize(quota.capacityForResources(2))))
                    .hasRootCauseMessage("explicit native GC quota expansion required");
            assertThatThrownBy(() -> await(quota.expand(capacity - 1)))
                    .hasRootCauseMessage("native GC capacity cannot shrink");
            f.zk.loseNextMulti = true;
            var expanded = await(quota.expand(quota.capacityForResources(2)));
            assertThat(expanded.reservedResources()).isEqualTo(1);
            assertThat(expanded.nativeVersion()).isEqualTo(full.nativeVersion() + 1);
            var retry = await(quota.firstClaimChecks(true, epoch));
            // A repeated create can never charge twice, even with a fresh capacity-head version.
            assertThat(await(f.commit(retry, epoch))).isEqualTo(KeeperException.Code.NODEEXISTS.intValue());
            assertThat(await(quota.snapshot())).isEqualTo(expanded);
            assertThat(await(f.commit(await(quota.firstClaimChecks(true, concurrent)), concurrent)))
                    .isZero();
            var current = await(quota.snapshot());
            assertThat(current.reservedResources()).isEqualTo(2);
            assertThat(current.encode().length()).isEqualTo(initial.encode().length());
            assertThat(await(quota.expand(current.capacityBytes()))).isEqualTo(current);
            assertThat(f.zk.lost.get()).isEqualTo(3);
            assertThat(M5BookKeeperNativeDeleteQuotaV2.decode(current.encode(), current.nativeVersion()))
                    .isEqualTo(current);
            assertThatThrownBy(
                            () -> M5BookKeeperNativeDeleteQuotaV2.decode(current.encode(), current.nativeVersion() + 1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void existingUnaccountedResourcesAndDeletedOrChangedHeadCannotBeImported() throws Exception {
        try (var f = new Fixture()) {
            f.zk.create(
                    f.root + "/nereus-m5-native-v2/ledgers",
                    new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            assertThatThrownBy(() -> await(f.quota.initialize(f.quota.capacityForResources(2))))
                    .hasRootCauseMessage("native GC quota is not initialized; existing resources cannot be imported");
            assertThat(f.zk.exists(f.quota.path(), false)).isNull();
            assertThatThrownBy(() -> await(f.quota.firstClaimChecks(true, f.epoch(1))))
                    .hasRootCauseMessage("native GC quota is not initialized; existing resources cannot be imported");
        }
        try (var f = new Fixture()) {
            var value = await(f.quota.initialize(f.quota.capacityForResources(2)));
            assertThatThrownBy(() -> await(f.quota.requireReserved(true)))
                    .hasRootCauseMessage("native epoch lacks a permanent capacity reservation");
            // Out-of-profile corruption is injected only in this test-owned, isolated native namespace.
            f.zk.setData(f.quota.path(), value.encode().toByteArray(), 0);
            assertThatThrownBy(() -> await(f.quota.snapshot()))
                    .hasRootCauseMessage("native GC quota checksum/version differs");
            f.zk.delete(f.quota.path(), 1);
            assertThatThrownBy(() -> await(f.quota.initialize(value.capacityBytes())))
                    .hasRootCauseMessage("native GC quota is not initialized; existing resources cannot be imported");
        }
    }

    private static final class Fixture implements AutoCloseable {
        final M5BookKeeperNativeCreateV2RealTest.FaultZooKeeper zk;
        final String root = "/nereus-m5-quota-test-" + UUID.randomUUID();
        final M5BookKeeperNamespaceGateV2 gate;
        final M5BookKeeperNativeDeleteQuotaV2 quota;

        Fixture() throws Exception {
            var connected = new CountDownLatch(1);
            zk = new M5BookKeeperNativeCreateV2RealTest.FaultZooKeeper(
                    URI.create(System.getProperty("nereus.bookkeeper.metadataServiceUri"))
                            .getAuthority(),
                    event -> {
                        if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
                            connected.countDown();
                        }
                    });
            assertThat(connected.await(10, TimeUnit.SECONDS)).isTrue();
            String instance = UUID.randomUUID().toString();
            zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(
                    root + "/INSTANCEID",
                    instance.getBytes(StandardCharsets.US_ASCII),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            gate = new M5BookKeeperNamespaceGateV2(zk, root, instance, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            await(gate.initializeUnbound());
            quota = new M5BookKeeperNativeDeleteQuotaV2(zk, gate, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }

        M5BookKeeperNativeDeleteAuthorityV2.Snapshot epoch(long id) {
            return new M5BookKeeperNativeDeleteAuthorityV2.Snapshot(
                    new PhysicalResourceIdV2.BookKeeperLedger(gate.namespace(), id),
                    UUID.randomUUID(),
                    1,
                    Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1})),
                    0);
        }

        CompletionStage<Integer> commit(List<Op> reservation, M5BookKeeperNativeDeleteAuthorityV2.Snapshot epoch)
                throws Exception {
            String path = quota.epochPath(epoch.resource().ledgerId());
            String parent = path.substring(0, path.lastIndexOf('/'));
            try {
                zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException expected) {
                // Permanent fixture shard already exists.
            }
            var ops = new ArrayList<>(reservation);
            ops.add(Op.create(path, epoch.encode().toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
            var result = new CompletableFuture<Integer>();
            zk.multi(ops, (rc, ignored, context, replies) -> result.complete(rc), null);
            return result;
        }

        @Override
        public void close() throws Exception {
            zk.close();
        }
    }

    private static <T> T await(CompletionStage<T> value) throws Exception {
        return value.toCompletableFuture().get(20, TimeUnit.SECONDS);
    }
}
