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
import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;

/** Explicit Native Pulsar profile; no physical metadata create can bypass its native scope and birth admission. */
public final class M5PulsarNativeMetadataDriverV1 extends ZKMetadataClientDriver {
    static final String SCHEME = "m5pzk";
    static final String SPEC_PROPERTY = "nereusM5PulsarCreateSpecV1";
    static final String OBSERVE_PROPERTY = "nereusM5PulsarObserveExistingScopeV1";
    static final String BIRTH_PROPERTY = "nereusM5PulsarBirthAdmissionV1";

    @FunctionalInterface
    public interface BirthAdmission {
        CompletableFuture<org.apache.bookkeeper.versioning.Versioned<LedgerMetadata>> mutate(
                long ledgerId,
                LedgerMetadata metadata,
                boolean creating,
                java.util.function.Supplier<
                                CompletableFuture<org.apache.bookkeeper.versioning.Versioned<LedgerMetadata>>>
                        mutation);
    }

    /** Only an actual guarded SDK metadata create/update constructs this original-operation handle. */
    public static final class NativeMetadataWriteUnknownException extends IllegalStateException {
        private final long ledgerId;
        private final Sha256Digest scope;
        private final Sha256Digest requestedMetadata;
        private final java.util.function.Supplier<CompletableFuture<Sha256Digest>> observation;
        private CompletableFuture<Sha256Digest> observed;

        private NativeMetadataWriteUnknownException(
                long ledgerId,
                Sha256Digest scope,
                Sha256Digest requestedMetadata,
                Throwable originalFailure,
                java.util.function.Supplier<CompletableFuture<Sha256Digest>> observation) {
            super("Original Native metadata write requires terminal reconciliation", originalFailure);
            this.ledgerId = ledgerId;
            this.scope = scope;
            this.requestedMetadata = requestedMetadata;
            this.observation = observation;
        }

        public long ledgerId() {
            return ledgerId;
        }

        public Sha256Digest scopeSha256() {
            return scope;
        }

        public Sha256Digest requestedMetadataSha256() {
            return requestedMetadata;
        }

        /** Original native fence/read pass; it cannot reissue metadata creation/update or report operation success. */
        public synchronized CompletableFuture<Sha256Digest> observeTerminal() {
            if (observed == null || observed.isCompletedExceptionally()) {
                observed = observation.get();
            }
            return observed.copy();
        }
    }

    static NativeMetadataWriteUnknownException unknownMetadataWrite(
            long ledgerId,
            Sha256Digest scope,
            Sha256Digest requestedMetadata,
            Throwable originalFailure,
            java.util.function.Supplier<CompletableFuture<Sha256Digest>> observation) {
        return new NativeMetadataWriteUnknownException(
                ledgerId, scope, requestedMetadata, originalFailure, observation);
    }

    private M5PulsarNativeCreateSpecV1 spec;
    private M5BookKeeperNativeCreateGuardV2 guard;
    private BirthAdmission birth;
    private final java.util.Set<Long> dataLedgers = new java.util.HashSet<>();
    private M5BookKeeperNativeLedgerManagerV2 ledgerManager;
    private final java.util.Set<CompletableFuture<?>> mutations = new java.util.HashSet<>();
    private final java.util.Set<CompletableFuture<?>> generations = new java.util.HashSet<>();
    private final java.util.Set<CompletableFuture<?>> birthReads = new java.util.HashSet<>();
    private boolean metadataClosed;
    private boolean birthReadsClosed;

    static void register() {
        MetadataDrivers.registerClientDriver(SCHEME, M5PulsarNativeMetadataDriverV1.class);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public synchronized MetadataClientDriver initialize(
            ClientConfiguration configuration,
            ScheduledExecutorService scheduler,
            StatsLogger statsLogger,
            Optional<Object> context)
            throws MetadataException {
        try {
            if (context.isPresent() || !configuration.getMetadataServiceUri().startsWith(SCHEME + "://")) {
                throw new IllegalArgumentException("Native Pulsar driver requires its own exact connection");
            }
            spec = M5PulsarNativeCreateSpecV1.decode(
                    CanonicalBytes.copyOf(Base64.getDecoder().decode(configuration.getString(SPEC_PROPERTY))));
            if (!(configuration.getProperty(BIRTH_PROPERTY) instanceof BirthAdmission admission)) {
                throw new IllegalArgumentException("Native Pulsar driver has no actual birth admission");
            }
            birth = admission;
            var nativeConfiguration = new ClientConfiguration(configuration);
            nativeConfiguration.setMetadataServiceUri(
                    "zk" + configuration.getMetadataServiceUri().substring(SCHEME.length()));
            super.initialize(nativeConfiguration, scheduler, statsLogger, Optional.empty());
            var layout = layoutManager.readLedgerLayout();
            if (layout == null
                    || layout.getLayoutFormatVersion() != 2
                    || layout.getManagerVersion() != 1
                    || !layout.getManagerFactoryClass().equals(HierarchicalLedgerManagerFactory.class.getName())) {
                throw new IllegalArgumentException(
                        "Native Pulsar profile requires exact hierarchical layout version 1");
            }
            if (ledgersRootPath.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 2048) {
                throw new IllegalArgumentException("Native Pulsar ledger root exceeds its observation workspace");
            }
            guard = new M5BookKeeperNativeCreateGuardV2(
                    zk,
                    ledgersRootPath,
                    spec.nativeInstanceId(),
                    spec.sha256(),
                    spec.encode(),
                    ZkUtils.getACLs(nativeConfiguration),
                    Optional.of(spec.namespaceBinding()),
                    configuration.getBoolean(OBSERVE_PROPERTY, false));
            var factory = new PulsarFactory();
            factory.initialize(nativeConfiguration, layoutManager, layout.getManagerVersion());
            lmFactory = factory;
            return this;
        } catch (Exception failure) {
            close();
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new MetadataException(Code.METADATA_SERVICE_ERROR, "Native Pulsar profile admission failed", failure);
        }
    }

    public CompletableFuture<Void> fenceCreates() {
        return guard.fenceCreates().copy();
    }

    public CompletableFuture<Void> requireReserved(long ledgerId) {
        return guard.requireOwned(ledgerId).copy();
    }

    public M5PulsarNativeCreateSpecV1 createSpec() {
        return spec;
    }

    M5BookKeeperNativeDeleteAuthorityV2 deleteAuthority(
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger resource,
            com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1 capability) {
        if (ledgerManager == null) {
            throw new IllegalStateException("Native Pulsar SDK ledger manager is not open");
        }
        return new M5BookKeeperNativeDeleteAuthorityV2(getZk(), ledgerManager, guard, acls, spec, resource, capability);
    }

    public record BirthFact(
            long ledgerId,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact indexEntry,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact reservation,
            Optional<M5BookKeeperNativeCreateClientV2.NativeRecordFact> ledgerMetadata) {}

    public record FencedBirthFacts(
            M5PulsarNativeCreateSpecV1 scope,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact instanceIdentity,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact taskFence,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact inventoryHead,
            java.util.List<BirthFact> births) {
        public FencedBirthFacts {
            births = java.util.List.copyOf(births);
        }
    }

    /** Reads a frozen reservation set. Supplies no SDK drain or deletion proof and does not recover ledgers. */
    public CompletableFuture<FencedBirthFacts> readFencedBirths() {
        return readFencedBirths(M5PulsarNativeCreateSpecV1.MAX_BULK_METADATA_BYTES);
    }

    public CompletableFuture<FencedBirthFacts> readFencedBirths(int metadataByteLimit) {
        if (metadataByteLimit < 1 || metadataByteLimit > M5PulsarNativeCreateSpecV1.MAX_BULK_METADATA_BYTES) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native birth byte limit differs"));
        }
        return ownBirthRead(() -> captureFencedBirths(metadataByteLimit));
    }

    private CompletableFuture<FencedBirthFacts> captureFencedBirths(int metadataByteLimit) {
        var metadataBytes = new java.util.concurrent.atomic.AtomicInteger();
        return guard.readFencedBirthIndex()
                .thenCompose(index -> {
                    CompletableFuture<java.util.List<BirthFact>> reading =
                            CompletableFuture.completedFuture(new java.util.ArrayList<>());
                    for (var birth : index.births()) {
                        reading = reading.thenCompose(
                                facts -> readBirthMetadata(birth.ledgerId()).thenApply(metadata -> {
                                    metadata.ifPresent(value -> {
                                        if (metadataBytes.addAndGet(
                                                        value.rawStoredBytes().length())
                                                > metadataByteLimit) {
                                            throw new IllegalStateException(
                                                    "Native birth bulk metadata budget exhausted");
                                        }
                                    });
                                    facts.add(new BirthFact(
                                            birth.ledgerId(), birth.indexEntry(), birth.reservation(), metadata));
                                    return facts;
                                }));
                    }
                    return reading.thenCompose(
                            facts -> guard.readFencedBirthIndex().thenCompose(after -> {
                                if (!index.equals(after)) {
                                    throw new IllegalStateException(
                                            "Native birth index changed while reading physical metadata");
                                }
                                CompletableFuture<Void> validated = CompletableFuture.completedFuture(null);
                                for (var fact : facts) {
                                    validated = validated.thenCompose(ignored -> guard.readOptionalNativeRecord(
                                                    ledgerManager.nativeLedgerPath(fact.ledgerId()))
                                            .thenAccept(current -> {
                                                if (!current.equals(fact.ledgerMetadata())) {
                                                    throw new IllegalStateException(
                                                            "Native birth physical metadata changed during capture");
                                                }
                                            }));
                                }
                                return validated.thenApply(ignored -> new FencedBirthFacts(
                                        spec, index.instance(), index.task(), index.head(), facts));
                            }));
                })
                .copy();
    }

    public record NativeMetadataSummary(
            String nativePath,
            com.nereusstream.domain.bytes.Sha256Digest rawStoredSha256,
            int rawBytes,
            long nativeCreateZxid,
            long nativeModifyZxid,
            int nativeVersion,
            long nativeCreatedAtMillis) {}

    public record BirthSummary(
            long ledgerId,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact indexEntry,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact reservation,
            Optional<NativeMetadataSummary> ledgerMetadata) {}

    public record FencedBirthSummaries(
            M5PulsarNativeCreateSpecV1 scope,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact instanceIdentity,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact taskFence,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact inventoryHead,
            java.util.List<BirthSummary> births) {
        public FencedBirthSummaries {
            births = java.util.List.copyOf(births);
        }
    }

    public record PriorSealedBirth(
            M5PulsarNativeCreateSpecV1 scope,
            java.util.List<M5BookKeeperNativeCreateClientV2.NativeRecordFact> facts,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact ledgerMetadata) {
        public PriorSealedBirth {
            facts = java.util.List.copyOf(facts);
        }
    }

    /** Reads one prior tagged CLOSED birth on this exact SDK connection; grants no SDK mutation authority. */
    public CompletableFuture<PriorSealedBirth> readPriorSealedBirth(long ledgerId, LedgerMetadata expected) {
        return ownBirthRead(() -> {
            M5PulsarNativeCreateSpecV1.requireBoundedMetadata(expected);
            if (!expected.isClosed()) {
                throw new IllegalArgumentException("prior Native Pulsar source is not CLOSED");
            }
            var marker = expected.getCustomMetadata().get(M5PulsarNativeCreateSpecV1.SCOPE_KEY);
            if (marker == null || marker.length != 32) {
                throw new IllegalArgumentException("prior Native Pulsar source has no exact scope marker");
            }
            var priorSha = Sha256Digest.copyOf(marker);
            return guard.readPriorPulsarBirth(priorSha, ledgerId).thenCompose(prior -> {
                var original = prior.scope();
                var member = original.bindingFor(expected, true)
                        .orElseThrow(() -> new IllegalArgumentException(
                                "prior Native Pulsar source header differs from its scope"));
                if (!original.protocolCell().equals(spec.protocolCell())
                        || !original.providerScope().equals(spec.providerScope())
                        || !original.configuration().equals(spec.configuration())
                        || !spec.bindings().contains(member)) {
                    throw new IllegalArgumentException(
                            "prior Native Pulsar source belongs to another Cell/Binding/profile");
                }
                String path = ledgerManager.nativeLedgerPath(ledgerId);
                return guard.readNativeRecord(path).thenCompose(header -> {
                    try {
                        if (header.rawStoredBytes().length() > M5PulsarNativeCreateSpecV1.MAX_METADATA_BYTES
                                || !M5PulsarNativeCreateSpecV1.sameMetadata(
                                        expected,
                                        new org.apache.bookkeeper.meta.LedgerMetadataSerDe()
                                                .parseConfig(
                                                        header.rawStoredBytes().toByteArray(),
                                                        ledgerId,
                                                        Optional.of(header.nativeCreatedAtMillis())))) {
                            throw new IllegalStateException("prior Native Pulsar source SDK metadata differs");
                        }
                    } catch (IOException invalid) {
                        return CompletableFuture.failedFuture(invalid);
                    }
                    return guard.readPriorPulsarBirth(priorSha, ledgerId).thenCompose(current -> {
                        if (!current.equals(prior)) {
                            throw new IllegalStateException("prior Native Pulsar scope changed during source capture");
                        }
                        return guard.readNativeRecord(path).thenApply(after -> {
                            if (!header.equals(after)) {
                                throw new IllegalStateException(
                                        "prior Native Pulsar source metadata changed during capture");
                            }
                            return new PriorSealedBirth(original, prior.facts(), header);
                        });
                    });
                });
            });
        });
    }

    private static NativeMetadataSummary summarize(M5BookKeeperNativeCreateClientV2.NativeRecordFact value) {
        return new NativeMetadataSummary(
                value.nativePath(),
                value.rawStoredSha256(),
                value.rawStoredBytes().length(),
                value.nativeCreateZxid(),
                value.nativeModifyZxid(),
                value.nativeVersion(),
                value.nativeCreatedAtMillis());
    }

    private CompletableFuture<Optional<M5BookKeeperNativeCreateClientV2.NativeRecordFact>> readBirthMetadata(long id) {
        return guard.readOptionalNativeRecord(ledgerManager.nativeLedgerPath(id))
                .thenApply(metadata -> {
                    metadata.ifPresent(value -> {
                        if (value.rawStoredBytes().length() > M5PulsarNativeCreateSpecV1.MAX_METADATA_BYTES) {
                            throw new IllegalStateException("Native birth metadata exceeds its limit");
                        }
                        try {
                            var decoded = new org.apache.bookkeeper.meta.LedgerMetadataSerDe()
                                    .parseConfig(value.rawStoredBytes().toByteArray(), id, Optional.empty());
                            if (spec.bindingFor(decoded, true).isEmpty()) {
                                throw new IllegalStateException("Native birth has foreign physical metadata");
                            }
                            M5PulsarNativeCreateSpecV1.requireBoundedMetadata(decoded);
                        } catch (IOException invalid) {
                            throw new IllegalStateException("Native birth metadata is invalid", invalid);
                        }
                    });
                    return metadata;
                });
    }

    /** Captures every indexed birth without retaining all raw ledger metadata in one result. */
    public CompletableFuture<FencedBirthSummaries> readFencedBirthSummaries() {
        return ownBirthRead(() -> guard.readFencedBirthIndex().thenCompose(index -> {
            CompletableFuture<java.util.List<BirthSummary>> reading =
                    CompletableFuture.completedFuture(new java.util.ArrayList<>());
            for (var birth : index.births()) {
                reading = reading.thenCompose(
                        facts -> readBirthMetadata(birth.ledgerId()).thenApply(metadata -> {
                            facts.add(new BirthSummary(
                                    birth.ledgerId(),
                                    birth.indexEntry(),
                                    birth.reservation(),
                                    metadata.map(value -> summarize(value))));
                            return facts;
                        }));
            }
            return reading.thenCompose(facts -> {
                CompletableFuture<Void> validated = CompletableFuture.completedFuture(null);
                for (var fact : facts) {
                    validated = validated.thenCompose(
                            ignored -> readBirthMetadata(fact.ledgerId()).thenAccept(current -> {
                                if (!current.map(value -> summarize(value)).equals(fact.ledgerMetadata())) {
                                    throw new IllegalStateException(
                                            "Native birth metadata changed during summary capture");
                                }
                            }));
                }
                return validated
                        .thenCompose(ignored -> guard.readFencedBirthIndex())
                        .thenApply(after -> {
                            if (!index.equals(after)) {
                                throw new IllegalStateException("Native birth index changed during summary capture");
                            }
                            return new FencedBirthSummaries(spec, index.instance(), index.task(), index.head(), facts);
                        });
            });
        }));
    }

    private void requireInventory(
            M5BookKeeperNativeCreateGuardV2.FencedBirthIndex index, FencedBirthSummaries expected) {
        if (!spec.equals(expected.scope())
                || !index.instance().equals(expected.instanceIdentity())
                || !index.task().equals(expected.taskFence())
                || !index.head().equals(expected.inventoryHead())
                || index.births().size() != expected.births().size()) {
            throw new IllegalStateException("Native birth summary differs from its actual inventory");
        }
        for (int i = 0; i < index.births().size(); i++) {
            var actual = index.births().get(i);
            var fact = expected.births().get(i);
            if (actual.ledgerId() != fact.ledgerId()
                    || !actual.indexEntry().equals(fact.indexEntry())
                    || !actual.reservation().equals(fact.reservation())) {
                throw new IllegalStateException("Native birth summary membership differs");
            }
        }
    }

    /** Reads one original header and rejects a stale summary, without adopting or recovering that resource. */
    public CompletableFuture<BirthFact> readBirthMetadata(FencedBirthSummaries expected, long ledgerId) {
        return ownBirthRead(() -> guard.readFencedBirthIndex().thenCompose(index -> {
            requireInventory(index, expected);
            var birth = expected.births().stream()
                    .filter(value -> value.ledgerId() == ledgerId)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Native ledger is outside the birth summary"));
            return readBirthMetadata(ledgerId).thenCompose(metadata -> {
                if (!metadata.map(value -> summarize(value)).equals(birth.ledgerMetadata())) {
                    throw new IllegalStateException("Native birth metadata differs from its summary");
                }
                return guard.readFencedBirthIndex().thenCompose(after -> {
                    requireInventory(after, expected);
                    return readBirthMetadata(ledgerId).thenApply(current -> {
                        if (!current.equals(metadata)) {
                            throw new IllegalStateException(
                                    "Native birth metadata changed during single-header capture");
                        }
                        return new BirthFact(ledgerId, birth.indexEntry(), birth.reservation(), metadata);
                    });
                });
            });
        }));
    }

    private synchronized <T> CompletableFuture<T> ownBirthRead(
            java.util.function.Supplier<CompletableFuture<T>> action) {
        if (birthReadsClosed) {
            return CompletableFuture.failedFuture(new BKException.BKClientClosedException());
        }
        if (birthReads.size() == M5PulsarNativeCreateSpecV1.MAX_BIRTH_READS) {
            return CompletableFuture.failedFuture(new BKException.BKTooManyRequestsException());
        }
        var original = new CompletableFuture<T>();
        birthReads.add(original);
        final CompletableFuture<T> actual;
        try {
            actual = action.get();
        } catch (Throwable failure) {
            birthReads.remove(original);
            return CompletableFuture.failedFuture(failure);
        }
        actual.whenComplete((value, failure) -> {
            synchronized (M5PulsarNativeMetadataDriverV1.this) {
                birthReads.remove(original);
            }
            if (failure == null) {
                original.complete(value);
            } else {
                original.completeExceptionally(failure);
            }
        });
        return original.copy();
    }

    public synchronized CompletableFuture<Void> drainBirthReads() {
        birthReadsClosed = true;
        return CompletableFuture.allOf(birthReads.stream()
                        .map(value -> value.handle((result, failure) -> null))
                        .toArray(CompletableFuture[]::new))
                .copy();
    }

    public synchronized CompletableFuture<Void> drainMetadata() {
        metadataClosed = true;
        return CompletableFuture.allOf(java.util.stream.Stream.concat(mutations.stream(), generations.stream())
                        .map(value -> value.handle((result, failure) -> null))
                        .toArray(CompletableFuture[]::new))
                .copy();
    }

    private synchronized CompletableFuture<org.apache.bookkeeper.versioning.Versioned<LedgerMetadata>> mutate(
            long ledgerId,
            LedgerMetadata metadata,
            boolean creating,
            java.util.function.Supplier<CompletableFuture<org.apache.bookkeeper.versioning.Versioned<LedgerMetadata>>>
                    mutation) {
        if (metadataClosed) {
            return CompletableFuture.failedFuture(new BKException.BKClientClosedException());
        }
        if (mutations.size() == M5PulsarNativeCreateSpecV1.MAX_PENDING_METADATA_MUTATIONS) {
            return CompletableFuture.failedFuture(new BKException.BKTooManyRequestsException());
        }
        try {
            M5PulsarNativeCreateSpecV1.requireBoundedMetadata(metadata);
        } catch (IllegalArgumentException oversized) {
            return CompletableFuture.failedFuture(new BKException.BKMetadataSerializationException(oversized));
        }
        var original = new CompletableFuture<org.apache.bookkeeper.versioning.Versioned<LedgerMetadata>>();
        mutations.add(original);
        final CompletableFuture<org.apache.bookkeeper.versioning.Versioned<LedgerMetadata>> accepted;
        try {
            accepted = birth.mutate(ledgerId, metadata, creating, mutation);
        } catch (Throwable failure) {
            mutations.remove(original);
            return CompletableFuture.failedFuture(failure);
        }
        accepted.whenComplete((result, failure) -> {
            synchronized (M5PulsarNativeMetadataDriverV1.this) {
                mutations.remove(original);
                if (failure == null && spec.factoryInstance().isPresent()) {
                    dataLedgers.add(ledgerId);
                }
            }
            if (failure == null) {
                original.complete(result);
            } else {
                original.completeExceptionally(failure);
            }
        });
        return original.copy();
    }

    synchronized boolean isDataLedgerAdmitted(long ledgerId) {
        return dataLedgers.contains(ledgerId);
    }

    private void generateLedgerId(LedgerIdGenerator delegate, GenericCallback<Long> callback) {
        var original = new CompletableFuture<Void>();
        int rejected = BKException.Code.OK;
        synchronized (this) {
            if (metadataClosed) {
                rejected = BKException.Code.ClientClosedException;
            } else if (generations.size() == M5PulsarNativeCreateSpecV1.MAX_PENDING_LEDGER_ID_GENERATIONS) {
                rejected = BKException.Code.TooManyRequestsException;
            } else {
                generations.add(original);
            }
        }
        if (rejected != BKException.Code.OK) {
            callback.operationComplete(rejected, null);
            return;
        }
        try {
            delegate.generateLedgerId((rc, id) -> {
                if (rc != BKException.Code.OK) {
                    completeGeneration(original, callback, rc, null);
                } else {
                    guard.reserve(id)
                            .whenComplete((ignored, failure) -> completeGeneration(
                                    original,
                                    callback,
                                    failure == null ? BKException.Code.OK : BKException.Code.ZKException,
                                    failure == null ? id : null));
                }
            });
        } catch (RuntimeException failure) {
            if (original.isDone()) {
                throw failure;
            }
            completeGeneration(original, callback, BKException.Code.ZKException, null);
        }
    }

    private void completeGeneration(CompletableFuture<Void> original, GenericCallback<Long> callback, int rc, Long id) {
        try {
            callback.operationComplete(rc, id);
        } finally {
            synchronized (this) {
                generations.remove(original);
            }
            original.complete(null);
        }
    }

    private final class PulsarFactory extends HierarchicalLedgerManagerFactory {
        @Override
        public LedgerManager newLedgerManager() {
            var created = new M5BookKeeperNativeLedgerManagerV2(
                    (AbstractZkLedgerManager) super.newLedgerManager(),
                    spec,
                    guard,
                    M5PulsarNativeMetadataDriverV1.this::mutate,
                    getZk(),
                    acls);
            ledgerManager = created;
            return created;
        }

        @Override
        public LedgerIdGenerator newLedgerIdGenerator() {
            var delegate = super.newLedgerIdGenerator();
            return new LedgerIdGenerator() {
                @Override
                public void generateLedgerId(GenericCallback<Long> callback) {
                    M5PulsarNativeMetadataDriverV1.this.generateLedgerId(delegate, callback);
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }
    }
}
