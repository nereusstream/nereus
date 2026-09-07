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

import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/** Create-only SPI decorator. Native read/recovery/write/delete semantics stay in the locked BK implementation. */
final class M5BookKeeperNativeLedgerManagerV2 implements LedgerManager {
    private final AbstractZkLedgerManager delegate;
    private final M5BookKeeperNativeCreateGuardV2 guard;
    private final M5BookKeeperNativeCreateSpecV2 spec;
    private final ZooKeeper zk;
    private final List<ACL> acls;

    M5BookKeeperNativeLedgerManagerV2(
            AbstractZkLedgerManager delegate,
            M5BookKeeperNativeCreateGuardV2 guard,
            M5BookKeeperNativeCreateSpecV2 spec,
            ZooKeeper zk,
            List<ACL> acls) {
        this.delegate = delegate;
        this.guard = guard;
        this.spec = spec;
        this.zk = zk;
        this.acls = List.copyOf(acls);
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long ledgerId, LedgerMetadata input) {
        final LedgerMetadata metadata;
        final byte[] bytes;
        try {
            metadata = LedgerMetadataBuilder.from(input)
                    .withId(ledgerId)
                    .withCToken(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
                    .build();
            if (metadata.getMetadataFormatVersion() != 3
                    || metadata.getDigestType() != DigestType.CRC32C
                    || metadata.getPassword().length != 0
                    || metadata.getCustomMetadata().size() != 4
                    || spec.configurations().stream()
                            .noneMatch(configuration -> metadata.getEnsembleSize() == configuration.ensembleSize()
                                    && metadata.getWriteQuorumSize() == configuration.writeQuorumSize()
                                    && metadata.getAckQuorumSize() == configuration.ackQuorumSize()
                                    && RealBookKeeperCellSessionV1.metadataMatches(
                                            metadata,
                                            new RunLedgerHandleV1(
                                                    configuration.providerScopeId(),
                                                    configuration.runId(),
                                                    new BookKeeperLedgerIdentity(ledgerId),
                                                    configuration.configurationDigest())))) {
                return CompletableFuture.failedFuture(new BKException.BKUnauthorizedAccessException());
            }
            bytes = new LedgerMetadataSerDe().serialize(metadata);
        } catch (Exception failure) {
            return CompletableFuture.failedFuture(new BKException.BKMetadataSerializationException(failure));
        }
        String path = delegate.getLedgerPath(ledgerId);
        return guard.requireOwned(ledgerId)
                .thenCompose(ignored -> guard.createParents(path.substring(0, path.lastIndexOf('/'))))
                .thenCompose(ignored -> {
                    var result = new CompletableFuture<Versioned<LedgerMetadata>>();
                    var ops = new ArrayList<>(guard.createChecks(ledgerId));
                    ops.add(Op.create(path, bytes, acls, CreateMode.PERSISTENT));
                    zk.multi(
                            ops,
                            (rc, actualPath, context, responses) -> {
                                if (rc == KeeperException.Code.OK.intValue()) {
                                    result.complete(new Versioned<>(metadata, new LongVersion(0)));
                                } else if (rc == KeeperException.Code.NODEEXISTS.intValue()
                                        || rc == KeeperException.Code.CONNECTIONLOSS.intValue()
                                        || rc == KeeperException.Code.OPERATIONTIMEOUT.intValue()) {
                                    // Exact original creator token may reconcile a lost response. Never reissue a
                                    // create.
                                    delegate.readLedgerMetadata(ledgerId).whenComplete((observed, failure) -> {
                                        if (failure == null
                                                && observed.getValue().getCToken() == metadata.getCToken()) {
                                            result.complete(observed);
                                        } else {
                                            result.completeExceptionally(
                                                    failure == null
                                                            ? new BKException.BKLedgerExistException()
                                                            : failure);
                                        }
                                    });
                                } else {
                                    result.completeExceptionally(
                                            M5BookKeeperNativeCreateGuardV2.failure(rc, guard.taskPath()));
                                }
                            },
                            null);
                    return result;
                });
    }

    @Override
    public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
        return delegate.removeLedgerMetadata(ledgerId, version);
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
        return delegate.readLedgerMetadata(ledgerId);
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(
            long ledgerId, LedgerMetadata metadata, Version version) {
        return delegate.writeLedgerMetadata(ledgerId, metadata, version);
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        delegate.registerLedgerMetadataListener(ledgerId, listener);
    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        delegate.unregisterLedgerMetadataListener(ledgerId, listener);
    }

    @Override
    public void asyncProcessLedgers(
            Processor<Long> processor,
            AsyncCallback.VoidCallback callback,
            Object context,
            int successRc,
            int failureRc) {
        delegate.asyncProcessLedgers(processor, callback, context, successRc, failureRc);
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long timeoutMillis) {
        return delegate.getLedgerRanges(timeoutMillis);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
