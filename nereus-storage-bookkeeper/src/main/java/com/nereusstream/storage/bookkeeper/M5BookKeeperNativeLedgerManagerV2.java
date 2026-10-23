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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

/** Guarded create SPI decorator; ordinary BK delete cannot bypass the separate M5 native delete authority. */
final class M5BookKeeperNativeLedgerManagerV2 implements LedgerManager {
    private final AbstractZkLedgerManager delegate;
    private final java.util.function.Supplier<M5BookKeeperNativeMetadataDriverV2.CreateScope> scopes;
    private final ZooKeeper zk;
    private final List<ACL> acls;
    private final M5PulsarNativeCreateSpecV1 pulsarSpec;
    private final M5BookKeeperNativeCreateGuardV2 pulsarGuard;
    private final M5PulsarNativeMetadataDriverV1.BirthAdmission pulsarBirth;

    M5BookKeeperNativeLedgerManagerV2(
            AbstractZkLedgerManager delegate,
            java.util.function.Supplier<M5BookKeeperNativeMetadataDriverV2.CreateScope> scopes,
            ZooKeeper zk,
            List<ACL> acls) {
        this.delegate = delegate;
        this.scopes = scopes;
        this.zk = zk;
        this.acls = List.copyOf(acls);
        pulsarSpec = null;
        pulsarGuard = null;
        pulsarBirth = null;
    }

    M5BookKeeperNativeLedgerManagerV2(
            AbstractZkLedgerManager delegate,
            M5PulsarNativeCreateSpecV1 spec,
            M5BookKeeperNativeCreateGuardV2 guard,
            M5PulsarNativeMetadataDriverV1.BirthAdmission birth,
            ZooKeeper zk,
            List<ACL> acls) {
        this.delegate = delegate;
        this.scopes = null;
        this.zk = zk;
        this.acls = List.copyOf(acls);
        pulsarSpec = spec;
        pulsarGuard = guard;
        pulsarBirth = birth;
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long ledgerId, LedgerMetadata input) {
        var scope = scopes == null ? null : scopes.get();
        var spec = scope == null ? null : scope.spec();
        var guard = scope == null ? pulsarGuard : scope.guard();
        final LedgerMetadata metadata;
        final byte[] bytes;
        try {
            if (pulsarSpec != null) {
                M5PulsarNativeCreateSpecV1.requireBoundedMetadata(input);
            }
            var builder = LedgerMetadataBuilder.from(input)
                    .withId(ledgerId)
                    .withCToken(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE));
            if (pulsarSpec != null) {
                var binding =
                        pulsarSpec.bindingFor(input, false).orElseThrow(BKException.BKUnauthorizedAccessException::new);
                var values = new java.util.HashMap<>(input.getCustomMetadata());
                values.put(
                        M5PulsarNativeCreateSpecV1.SCOPE_KEY,
                        pulsarSpec.sha256().bytes().toByteArray());
                values.put(
                        M5PulsarNativeCreateSpecV1.BINDING_KEY,
                        binding.identity().toByteArray());
                builder.withCustomMetadata(values);
            }
            metadata = builder.build();
            if (pulsarSpec == null
                    && (metadata.getMetadataFormatVersion() != 3
                            || metadata.getDigestType() != DigestType.CRC32C
                            || metadata.getPassword().length != 0
                            || metadata.getCustomMetadata().size() != 4
                            || spec.configurations().stream()
                                    .noneMatch(
                                            configuration -> metadata.getEnsembleSize() == configuration.ensembleSize()
                                                    && metadata.getWriteQuorumSize() == configuration.writeQuorumSize()
                                                    && metadata.getAckQuorumSize() == configuration.ackQuorumSize()
                                                    && RealBookKeeperCellSessionV1.metadataMatches(
                                                            metadata,
                                                            new RunLedgerHandleV1(
                                                                    configuration.providerScopeId(),
                                                                    configuration.runId(),
                                                                    new BookKeeperLedgerIdentity(ledgerId),
                                                                    configuration.configurationDigest()))))) {
                return CompletableFuture.failedFuture(new BKException.BKUnauthorizedAccessException());
            }
            bytes = pulsarSpec == null ? new LedgerMetadataSerDe().serialize(metadata) : null;
        } catch (Exception failure) {
            return CompletableFuture.failedFuture(new BKException.BKMetadataSerializationException(failure));
        }
        String path = delegate.getLedgerPath(ledgerId);
        if (pulsarBirth != null) {
            return pulsarBirth.mutate(
                    ledgerId, metadata, true, () -> createNativeMetadata(ledgerId, metadata, guard, path));
        }
        return guard.requireOwned(ledgerId)
                .thenCompose(ignored -> guard.createParents(path.substring(0, path.lastIndexOf('/'))))
                .thenCompose(ignored -> createNativeMetadata(ledgerId, metadata, guard, bytes, path));
    }

    private CompletableFuture<Versioned<LedgerMetadata>> createNativeMetadata(
            long ledgerId, LedgerMetadata metadata, M5BookKeeperNativeCreateGuardV2 guard, String path) {
        try {
            byte[] bytes = new LedgerMetadataSerDe().serialize(metadata);
            var requested = Sha256Digest.hash(CanonicalBytes.copyOf(bytes));
            long creator = metadata.getCToken();
            return guard.requireOwned(ledgerId)
                    .thenCompose(ignored -> guard.createParents(path.substring(0, path.lastIndexOf('/'))))
                    .thenCompose(ignored -> createNativeMetadata(ledgerId, metadata, guard, bytes, path))
                    .handle((value, failure) -> {
                        if (failure == null) {
                            return value;
                        }
                        throw new java.util.concurrent.CompletionException(
                                M5PulsarNativeMetadataDriverV1.unknownMetadataWrite(
                                        ledgerId,
                                        pulsarSpec.sha256(),
                                        requested,
                                        failure,
                                        () -> observePulsarCreateTerminal(ledgerId, creator, requested, path)));
                    });
        } catch (IOException failure) {
            return CompletableFuture.failedFuture(new BKException.BKMetadataSerializationException(failure));
        }
    }

    private CompletableFuture<Sha256Digest> observePulsarCreateTerminal(
            long ledgerId, long creator, Sha256Digest requested, String path) {
        return pulsarGuard
                .fenceCreates()
                .thenCompose(ignored -> pulsarGuard.requireOwned(ledgerId))
                .thenCompose(ignored -> pulsarGuard.readInstanceFact())
                .thenCompose(ignored -> pulsarGuard.readClosedTaskFact())
                .thenCompose(fence -> pulsarGuard.readNativeRecord(path).handle((actual, failure) -> {
                    if (failure != null) {
                        Throwable cause = failure;
                        while (cause.getCause() != null && !(cause instanceof KeeperException.NoNodeException)) {
                            cause = cause.getCause();
                        }
                        if (!(cause instanceof KeeperException.NoNodeException)) {
                            throw new java.util.concurrent.CompletionException(failure);
                        }
                        return pulsarCreateTerminal(ledgerId, creator, requested, fence, Optional.empty());
                    }
                    if (actual.rawStoredBytes().length() > M5PulsarNativeCreateSpecV1.MAX_METADATA_BYTES) {
                        throw new IllegalStateException("Native create terminal exceeds its admitted workspace");
                    }
                    final LedgerMetadata observed;
                    try {
                        observed = new LedgerMetadataSerDe()
                                .parseConfig(
                                        actual.rawStoredBytes().toByteArray(),
                                        ledgerId,
                                        Optional.of(actual.nativeCreatedAtMillis()));
                    } catch (IOException invalid) {
                        throw new java.util.concurrent.CompletionException(invalid);
                    }
                    if (observed.getCToken() != creator
                            || pulsarSpec.bindingFor(observed, true).isEmpty()) {
                        throw new IllegalStateException("Native create terminal belongs to another invocation");
                    }
                    return pulsarCreateTerminal(ledgerId, creator, requested, fence, Optional.of(actual));
                }));
    }

    private Sha256Digest pulsarCreateTerminal(
            long ledgerId,
            long creator,
            Sha256Digest requested,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact fence,
            Optional<M5BookKeeperNativeCreateClientV2.NativeRecordFact> actual) {
        var bytes = ByteBuffer.allocate(4 + 32 * 4 + 8 * 5 + 1);
        bytes.putInt(0x4e4d4354); // NMCT, exact original create termination, never physical-delete eligibility.
        bytes.put(pulsarSpec.sha256().bytes().toByteArray());
        bytes.put(requested.bytes().toByteArray()).putLong(ledgerId).putLong(creator);
        bytes.put(fence.rawStoredSha256().bytes().toByteArray()).putLong(fence.nativeVersion());
        bytes.put((byte) (actual.isPresent() ? 1 : 0));
        bytes.putLong(actual.map(value -> (long) value.nativeVersion()).orElse(-1L));
        bytes.putLong(actual.map(M5BookKeeperNativeCreateClientV2.NativeRecordFact::nativeCreateZxid)
                .orElse(-1L));
        bytes.put(actual.map(value -> value.rawStoredSha256().bytes().toByteArray())
                .orElseGet(() -> new byte[32]));
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.array()));
    }

    private CompletableFuture<Versioned<LedgerMetadata>> createNativeMetadata(
            long ledgerId, LedgerMetadata metadata, M5BookKeeperNativeCreateGuardV2 guard, byte[] bytes, String path) {
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
                                    && observed.getValue().getCToken() == metadata.getCToken()
                                    && (pulsarSpec == null
                                            || pulsarSpec
                                                    .bindingFor(observed.getValue(), true)
                                                    .isPresent())) {
                                result.complete(observed);
                            } else {
                                result.completeExceptionally(
                                        failure == null ? new BKException.BKLedgerExistException() : failure);
                            }
                        });
                    } else {
                        result.completeExceptionally(M5BookKeeperNativeCreateGuardV2.failure(rc, guard.taskPath()));
                    }
                },
                null);
        return result;
    }

    String nativeLedgerPath(long ledgerId) {
        return delegate.getLedgerPath(ledgerId);
    }

    @Override
    public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
        return CompletableFuture.failedFuture(new BKException.BKUnauthorizedAccessException());
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
        return delegate.readLedgerMetadata(ledgerId);
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(
            long ledgerId, LedgerMetadata metadata, Version version) {
        if (pulsarSpec != null) {
            if (pulsarSpec.bindingFor(metadata, true).isEmpty()) {
                return CompletableFuture.failedFuture(new BKException.BKUnauthorizedAccessException());
            }
            if (!(version instanceof LongVersion expected)) {
                return CompletableFuture.failedFuture(new BKException.BKMetadataVersionException());
            }
            long expectedValue = expected.getLongVersion();
            if (expectedValue < 0 || expectedValue > Integer.MAX_VALUE) {
                return CompletableFuture.failedFuture(new BKException.BKMetadataVersionException());
            }
            var frozenVersion = new LongVersion(expectedValue);
            return pulsarBirth.mutate(ledgerId, metadata, false, () -> pulsarGuard
                    .requireOwned(ledgerId)
                    .thenCompose(ignored -> writePulsarMetadata(ledgerId, metadata, frozenVersion)));
        }
        return delegate.writeLedgerMetadata(ledgerId, metadata, version);
    }

    private CompletableFuture<Versioned<LedgerMetadata>> writePulsarMetadata(
            long ledgerId, LedgerMetadata metadata, LongVersion expected) {
        final Sha256Digest requested;
        try {
            requested = Sha256Digest.hash(CanonicalBytes.copyOf(new LedgerMetadataSerDe().serialize(metadata)));
        } catch (IOException invalid) {
            return CompletableFuture.failedFuture(new BKException.BKMetadataSerializationException(invalid));
        }
        long creator = metadata.getCToken();
        long predecessorVersion = expected.getLongVersion();
        return delegate.writeLedgerMetadata(ledgerId, metadata, new LongVersion(predecessorVersion))
                .handle((value, failure) -> {
                    if (failure == null) {
                        return value;
                    }
                    int code = BKException.getExceptionCode(failure);
                    throw new java.util.concurrent.CompletionException(
                            M5PulsarNativeMetadataDriverV1.unknownMetadataWrite(
                                    ledgerId,
                                    pulsarSpec.sha256(),
                                    requested,
                                    failure,
                                    () -> observePulsarWriteTerminal(
                                            ledgerId, creator, predecessorVersion, requested, code)));
                });
    }

    private CompletableFuture<Sha256Digest> observePulsarWriteTerminal(
            long ledgerId, long creator, long expected, Sha256Digest requested, int errorCode) {
        // ZooKeeperClient can retry SET_DATA after response loss. Even a final BadVersion is ambiguous.
        return pulsarGuard
                .requireOwned(ledgerId)
                .thenCompose(ignored -> pulsarGuard.readInstanceFact())
                .thenCompose(ignored -> pulsarGuard.readNativeRecord(delegate.getLedgerPath(ledgerId)))
                .thenApply(actual -> {
                    if (actual.rawStoredBytes().length() > M5PulsarNativeCreateSpecV1.MAX_METADATA_BYTES) {
                        throw new IllegalStateException("Native metadata terminal exceeds its admitted workspace");
                    }
                    final LedgerMetadata observed;
                    try {
                        observed = new LedgerMetadataSerDe()
                                .parseConfig(
                                        actual.rawStoredBytes().toByteArray(),
                                        ledgerId,
                                        Optional.of(actual.nativeCreatedAtMillis()));
                    } catch (IOException invalid) {
                        throw new java.util.concurrent.CompletionException(invalid);
                    }
                    if (observed.getCToken() != creator
                            || pulsarSpec.bindingFor(observed, true).isEmpty()) {
                        throw new IllegalStateException("Native metadata creation identity changed");
                    }
                    if (actual.nativeVersion() <= expected) {
                        throw new IllegalStateException("Original Native metadata version is not fenced");
                    }
                    return pulsarWriteTerminal(ledgerId, expected, requested, errorCode, actual);
                });
    }

    private Sha256Digest pulsarWriteTerminal(
            long ledgerId,
            long expected,
            Sha256Digest requested,
            int errorCode,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact actual) {
        var bytes = ByteBuffer.allocate(4 + 32 + 32 + 8 + 8 + 4 + 8 * 4 + 32);
        bytes.putInt(0x4e4d5754); // NMWT, an original SDK invocation terminal, not deletion eligibility.
        bytes.put(pulsarSpec.sha256().bytes().toByteArray());
        bytes.put(requested.bytes().toByteArray())
                .putLong(ledgerId)
                .putLong(expected)
                .putInt(errorCode);
        bytes.putLong(actual.nativeVersion());
        bytes.putLong(actual.nativeCreateZxid());
        bytes.putLong(actual.nativeModifyZxid());
        bytes.putLong(actual.nativeCreatedAtMillis());
        bytes.put(actual.rawStoredSha256().bytes().toByteArray());
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.array()));
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
