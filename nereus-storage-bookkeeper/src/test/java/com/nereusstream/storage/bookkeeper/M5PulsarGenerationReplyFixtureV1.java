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
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/** Actual profile/SDK generator; delays two successful native CREATE replies without blocking callbacks. */
public final class M5PulsarGenerationReplyFixtureV1 implements AutoCloseable {
    private final ReplyZooKeeper zk;
    private final LedgerIdGenerator generator;
    private final AtomicInteger allocations = new AtomicInteger();
    private final AtomicIntegerArray codes = new AtomicIntegerArray(3);
    private final CountDownLatch delivered = new CountDownLatch(2);

    public M5PulsarGenerationReplyFixtureV1(
            M5PulsarNativeMetadataDriverV1 driver, String uri, BookKeeperCapabilitySnapshotV1 capability)
            throws Exception {
        var ready = new CountDownLatch(1);
        zk = new ReplyZooKeeper(URI.create(uri).getAuthority(), event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                ready.countDown();
            }
        });
        try {
            if (!ready.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Native generation reply connection did not become ready");
            }
            var configuration = RealBookKeeperClientConfigurationV1.from(uri, capability);
            var factory = driver.getLedgerManagerFactory();
            factory.initialize(
                    configuration,
                    new ZkLayoutManager(zk, URI.create(uri).getPath(), ZkUtils.getACLs(configuration)),
                    1);
            generator = factory.newLedgerIdGenerator();
        } catch (Exception failure) {
            zk.close();
            throw failure;
        }
        for (int index = 0; index < codes.length(); index++) {
            codes.set(index, Integer.MIN_VALUE);
        }
    }

    public CompletableFuture<Long> allocate() {
        int index = allocations.getAndIncrement();
        var observer = new CompletableFuture<Long>();
        generator.generateLedgerId((rc, id) -> {
            if (index < codes.length()) {
                codes.set(index, rc);
            }
            if (rc == BKException.Code.OK) {
                observer.complete(id);
            } else {
                observer.completeExceptionally(BKException.create(rc));
            }
            if (index < 2) {
                delivered.countDown();
            }
        });
        return observer;
    }

    public boolean awaitNativeReplies() throws InterruptedException {
        return zk.created.await(30, TimeUnit.SECONDS) && zk.paths.get(0) != null && zk.paths.get(1) != null;
    }

    public int nativeDispatches() {
        return zk.dispatches.get();
    }

    public int terminalCode(int index) {
        return codes.get(index);
    }

    public void releaseReplies() {
        zk.release.complete(null);
    }

    public boolean awaitDelivery() throws InterruptedException {
        return delivered.await(30, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        releaseReplies();
        try {
            generator.close();
        } finally {
            zk.close();
        }
    }

    private static final class ReplyZooKeeper extends ZooKeeper {
        private final AtomicInteger dispatches = new AtomicInteger();
        private final AtomicReferenceArray<String> paths = new AtomicReferenceArray<>(2);
        private final CountDownLatch created = new CountDownLatch(2);
        private final CompletableFuture<Void> release = new CompletableFuture<>();

        private ReplyZooKeeper(String connect, Watcher watcher) throws java.io.IOException {
            super(connect, 10_000, watcher);
        }

        @Override
        public void create(
                String path,
                byte[] bytes,
                List<ACL> acls,
                CreateMode mode,
                AsyncCallback.StringCallback callback,
                Object context) {
            if (mode != CreateMode.EPHEMERAL_SEQUENTIAL) {
                super.create(path, bytes, acls, mode, callback, context);
                return;
            }
            int index = dispatches.getAndIncrement();
            super.create(
                    path,
                    bytes,
                    acls,
                    mode,
                    (rc, returnedPath, returnedContext, name) -> {
                        if (index < 2) {
                            if (rc == 0) {
                                paths.set(index, name);
                            }
                            created.countDown();
                            release.thenRun(() -> callback.processResult(rc, returnedPath, returnedContext, name));
                        } else {
                            callback.processResult(rc, returnedPath, returnedContext, name);
                        }
                    },
                    context);
        }
    }
}
