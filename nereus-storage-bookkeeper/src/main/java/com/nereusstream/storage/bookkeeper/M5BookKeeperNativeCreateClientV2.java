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
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.ExactLedgerEntryV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;

/** Owns the guarded BK client. Create fencing grants no append, publication, cleanup or delete authority. */
public final class M5BookKeeperNativeCreateClientV2 implements AutoCloseable {
    private final BookKeeper client;
    private final BookKeeperCapabilitySnapshotV1 capability;
    private final M5BookKeeperNativeCreateSpecV2 spec;

    private M5BookKeeperNativeCreateClientV2(
            BookKeeper client, BookKeeperCapabilitySnapshotV1 capability, M5BookKeeperNativeCreateSpecV2 spec) {
        this.client = client;
        this.capability = capability;
        this.spec = spec;
    }

    public static String discoverInstanceId(String metadataServiceUri, BookKeeperCapabilitySnapshotV1 capability)
            throws Exception {
        var configuration = RealBookKeeperClientConfigurationV1.from(metadataServiceUri, capability);
        try (var owned = (BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration)
                .build()) {
            var driver = (ZKMetadataClientDriver) owned.getMetadataClientDriver();
            var instance = new String(
                    driver.getZk().getData(URI.create(metadataServiceUri).getPath() + "/INSTANCEID", false, null),
                    StandardCharsets.US_ASCII);
            M5BookKeeperNativeCreateSpecV2.namespace(instance);
            return instance;
        }
    }

    public static M5BookKeeperNativeCreateClientV2 connect(
            String metadataServiceUri, BookKeeperCapabilitySnapshotV1 capability, M5BookKeeperNativeCreateSpecV2 spec)
            throws Exception {
        return connect(metadataServiceUri, capability, spec, Optional.empty());
    }

    public static M5BookKeeperNativeCreateClientV2 connect(
            String metadataServiceUri,
            BookKeeperCapabilitySnapshotV1 capability,
            M5BookKeeperNativeCreateSpecV2 spec,
            PhysicalNamespaceAuthorityBindingV2 binding)
            throws Exception {
        return connect(metadataServiceUri, capability, spec, Optional.of(binding));
    }

    private static M5BookKeeperNativeCreateClientV2 connect(
            String metadataServiceUri,
            BookKeeperCapabilitySnapshotV1 capability,
            M5BookKeeperNativeCreateSpecV2 spec,
            Optional<PhysicalNamespaceAuthorityBindingV2> binding)
            throws Exception {
        var configuration = RealBookKeeperClientConfigurationV1.from(metadataServiceUri, capability);
        if (!capability.credentialIdentityVersion().equals("bk-k0-no-auth:v1")
                || spec.configurations().stream()
                        .anyMatch(run -> !run.providerScopeId().equals(capability.providerScopeId())
                                || !run.configurationDigest().equals(capability.configurationDigest())
                                || run.ensembleSize() != capability.ensembleSize()
                                || run.writeQuorumSize() != capability.writeQuorumSize()
                                || run.ackQuorumSize() != capability.ackQuorumSize()
                                || run.digestType() != capability.digestType())) {
            throw new IllegalArgumentException("native M5 create scope differs from the admitted no-auth capability");
        }
        binding.ifPresent(value -> {
            if (!value.physicalNamespace().equals(spec.namespace())) {
                throw new IllegalArgumentException("native create binding has another physical namespace");
            }
            configuration.setProperty(
                    M5BookKeeperNativeMetadataDriverV2.NAMESPACE_BINDING_PROPERTY,
                    Base64.getEncoder().encodeToString(value.encode().toByteArray()));
        });
        M5BookKeeperNativeMetadataDriverV2.register();
        configuration.setProperty(
                M5BookKeeperNativeMetadataDriverV2.SPEC_PROPERTY,
                Base64.getEncoder().encodeToString(spec.encode().toByteArray()));
        configuration.setMetadataServiceUri(
                M5BookKeeperNativeMetadataDriverV2.DRIVER_SCHEME + metadataServiceUri.substring(2));
        var client = (BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration)
                .build();
        if (!(client.getMetadataClientDriver() instanceof M5BookKeeperNativeMetadataDriverV2)) {
            client.close();
            throw new IllegalStateException("native M5 scheme resolved to an unguarded metadata driver");
        }
        return new M5BookKeeperNativeCreateClientV2(client, capability, spec);
    }

    public M5BookKeeperNativeCreateSpecV2 spec() {
        return spec;
    }

    public BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
        return capability;
    }

    /** Reads the permanent assignment through this same owned connection; an unbound client cannot qualify. */
    public CompletionStage<PhysicalNamespaceAuthorityBindingV2> requireNamespaceBinding() {
        return driver().guard().requireNamespaceBinding();
    }

    public RealBookKeeperCellSessionV1 newSession() {
        return new RealBookKeeperCellSessionV1(client, capability, new byte[0]);
    }

    /** Native delete fencing is a low-level primitive; the M5 coordinator must supply current intent/eligibility. */
    public M5BookKeeperNativeDeleteAuthorityV2 deleteAuthority(RunLedgerHandleV1 handle) {
        return deleteAuthority(handle, () -> CompletableFuture.completedFuture(null));
    }

    M5BookKeeperNativeDeleteAuthorityV2 deleteAuthority(
            RunLedgerHandleV1 handle, java.util.function.Supplier<CompletionStage<Void>> beforeDelete) {
        var configuration = RunLedgerConfigurationV1.from(capability, handle.runId());
        if (!spec.configurations().contains(configuration)
                || !handle.providerScopeId().equals(capability.providerScopeId())
                || !handle.configurationDigest().equals(capability.configurationDigest())
                || !(client.getLedgerManager() instanceof org.apache.bookkeeper.meta.CleanupLedgerManager cleanup)
                || !(cleanup.getUnderlying() instanceof M5BookKeeperNativeLedgerManagerV2 manager)) {
            throw new IllegalArgumentException("native delete handle/manager differs from the admitted scope");
        }
        var driver = driver();
        return new M5BookKeeperNativeDeleteAuthorityV2(
                driver.getZk(),
                manager,
                driver.guard(),
                driver.nativeAcls(),
                handle,
                spec.namespace(),
                capability,
                beforeDelete);
    }

    /** Captures sealed native metadata through this same owned connection and admitted capability. */
    public java.util.concurrent.CompletionStage<M5BookKeeperDeleteAdapterV1.CaptureResult> captureExactTarget(
            com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1 handle) {
        return new M5BookKeeperDeleteAdapterV1(client, capability, new byte[0]).captureExactTarget(handle);
    }

    /**
     * Observes only entry zero without fencing or a reader-LAC requirement. A newly quorum-written header can still
     * have reader LAC -1. These exact bytes are not an ACK/quorum or protocol-owner proof; the lifecycle retains those
     * admission obligations. Every call validates native run metadata and closes its independent read handle.
     */
    public CompletionStage<ExactLedgerEntryV1> readNativeRunHeader(RunLedgerHandleV1 handle) {
        var configuration = RunLedgerConfigurationV1.from(capability, handle.runId());
        if (!spec.configurations().contains(configuration)
                || !handle.providerScopeId().equals(capability.providerScopeId())
                || !handle.configurationDigest().equals(capability.configurationDigest())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("header handle is outside native scope"));
        }
        var digest = org.apache.bookkeeper.client.api.DigestType.valueOf(
                capability.digestType().name());
        return client.newOpenLedgerOp()
                .withLedgerId(handle.ledgerIdentity().ledgerId())
                .withDigestType(digest)
                .withPassword(new byte[0])
                .withRecovery(false)
                .execute()
                .thenCompose(open -> {
                    CompletionStage<ExactLedgerEntryV1> read;
                    try {
                        var metadata = open.getLedgerMetadata();
                        if (!RealBookKeeperCellSessionV1.metadataMatches(metadata, handle)
                                || metadata.getEnsembleSize() != capability.ensembleSize()
                                || metadata.getWriteQuorumSize() != capability.writeQuorumSize()
                                || metadata.getAckQuorumSize() != capability.ackQuorumSize()
                                || metadata.getDigestType() != digest
                                || !Arrays.equals(metadata.getPassword(), new byte[0])) {
                            throw new IllegalArgumentException("native run header metadata differs from its handle");
                        }
                        read = open.readUnconfirmedAsync(0, 0).thenApply(entries -> {
                            try (var owned = entries) {
                                var entry = owned.getEntry(0);
                                if (entry.getLedgerId()
                                                != handle.ledgerIdentity().ledgerId()
                                        || entry.getEntryId() != 0) {
                                    throw new IllegalStateException("native header read returned another entry");
                                }
                                var bytes = CanonicalBytes.copyOf(entry.getEntryBytes());
                                return new ExactLedgerEntryV1(handle, 0, bytes, Sha256Digest.hash(bytes));
                            }
                        });
                    } catch (RuntimeException failure) {
                        read = CompletableFuture.failedFuture(failure);
                    }
                    var terminal = new CompletableFuture<ExactLedgerEntryV1>();
                    read.whenComplete((entry, failure) -> open.closeAsync().whenComplete((ignored, closeFailure) -> {
                        if (failure != null) {
                            terminal.completeExceptionally(failure);
                        } else if (closeFailure != null) {
                            terminal.completeExceptionally(closeFailure);
                        } else {
                            terminal.complete(entry);
                        }
                    }));
                    return terminal;
                });
    }

    public CompletableFuture<Void> fenceCreates() {
        return driver().fenceCreates();
    }

    M5BookKeeperNativeMetadataDriverV2 driver() {
        return (M5BookKeeperNativeMetadataDriverV2) client.getMetadataClientDriver();
    }

    BookKeeper nativeClient() {
        return client;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
