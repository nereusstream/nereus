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

import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/** Test-only native transport: fail one delete callback, then deliver its original exact ZooKeeper transaction. */
public final class M5BookKeeperDeleteFaultFixtureV2 implements AutoCloseable {
    private final BookKeeper client;
    private final FaultZooKeeper zk;
    private final M5BookKeeperNativeCreateGuardV2 guard;
    private final M5BookKeeperNativeLedgerManagerV2 manager;
    private final M5BookKeeperNativeCreateSpecV2 spec;
    private final BookKeeperCapabilitySnapshotV1 capability;
    private final List<org.apache.zookeeper.data.ACL> acls;

    public M5BookKeeperDeleteFaultFixtureV2(
            String uri,
            BookKeeperCapabilitySnapshotV1 capability,
            M5BookKeeperNativeCreateSpecV2 spec,
            PhysicalNamespaceAuthorityBindingV2 binding)
            throws Exception {
        this.capability = capability;
        this.spec = spec;
        var configuration = RealBookKeeperClientConfigurationV1.from(uri, capability);
        client = (BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration)
                .build();
        var connected = new CountDownLatch(1);
        zk = new FaultZooKeeper(URI.create(uri).getAuthority(), event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        if (!connected.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("native delete fault client did not connect");
        }
        acls = ZkUtils.getACLs(configuration);
        guard = new M5BookKeeperNativeCreateGuardV2(zk, URI.create(uri).getPath(), spec, acls, Optional.of(binding));
        var delegate =
                (AbstractZkLedgerManager) client.getLedgerManagerFactory().newLedgerManager();
        manager = new M5BookKeeperNativeLedgerManagerV2(delegate, guard, spec, zk, acls);
    }

    public M5BookKeeperNativeDeleteAuthorityV2 deleteAuthority(RunLedgerHandleV1 handle) {
        return new M5BookKeeperNativeDeleteAuthorityV2(
                zk,
                manager,
                guard,
                acls,
                handle,
                spec.namespace(),
                capability,
                () -> CompletableFuture.completedFuture(null));
    }

    public void dropNextDeleteCallback() {
        if (!zk.dropNextDelete.compareAndSet(false, true) || zk.dropped.get() != null) {
            throw new IllegalStateException("delete callback fault is already armed");
        }
    }

    public CompletionStage<Void> deliverDroppedDelete() {
        var ops = zk.dropped.getAndSet(null);
        if (ops == null || zk.dropNextDelete.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("no exact dropped native delete exists"));
        }
        var delivered = new CompletableFuture<Void>();
        zk.multi(
                ops,
                (rc, path, context, results) -> {
                    if (rc == KeeperException.Code.OK.intValue()) {
                        delivered.complete(null);
                    } else {
                        delivered.completeExceptionally(M5BookKeeperNativeCreateGuardV2.failure(rc, path));
                    }
                },
                null);
        return delivered;
    }

    @Override
    public void close() throws Exception {
        try {
            manager.close();
        } finally {
            try {
                zk.close();
            } finally {
                client.close();
            }
        }
    }

    private static final class FaultZooKeeper extends ZooKeeper {
        private final AtomicBoolean dropNextDelete = new AtomicBoolean();
        private final AtomicReference<List<Op>> dropped = new AtomicReference<>();

        private FaultZooKeeper(String connect, Watcher watcher) throws Exception {
            super(connect, 10_000, watcher);
        }

        @Override
        public void multi(Iterable<Op> ops, AsyncCallback.MultiCallback callback, Object context) {
            var exact = new ArrayList<Op>();
            boolean delete = false;
            for (var op : ops) {
                exact.add(op);
                delete |= op.getType() == ZooDefs.OpCode.delete;
            }
            if (delete && dropNextDelete.compareAndSet(true, false)) {
                if (!dropped.compareAndSet(null, List.copyOf(exact))) {
                    throw new IllegalStateException("another native delete transaction is already dropped");
                }
                callback.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), null, context, null);
                return;
            }
            super.multi(exact, callback, context);
        }
    }
}
