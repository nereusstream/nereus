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
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.util.ZkUtils;

/** Owned real native namespace connection. Assignment closes unbound M5 creates, not existing append/read handles. */
public final class M5BookKeeperNamespaceAuthorityV2 implements NativePhysicalNamespaceAuthorityV2, AutoCloseable {
    private final BookKeeper client;
    private final BookKeeperCapabilitySnapshotV1 capability;
    private final M5BookKeeperNamespaceGateV2 gate;
    private final M5BookKeeperNativeDeleteQuotaV2 deleteQuota;
    private final M5BookKeeperDeleteCellBudgetV2 deleteCellBudget;

    private M5BookKeeperNamespaceAuthorityV2(
            BookKeeper client,
            M5BookKeeperNamespaceGateV2 gate,
            java.util.List<org.apache.zookeeper.data.ACL> acls,
            BookKeeperCapabilitySnapshotV1 capability) {
        this.client = client;
        this.capability = capability;
        this.gate = gate;
        this.deleteQuota = new M5BookKeeperNativeDeleteQuotaV2(
                ((ZKMetadataClientDriver) client.getMetadataClientDriver()).getZk(), gate, acls);
        this.deleteCellBudget = new M5BookKeeperDeleteCellBudgetV2(
                ((ZKMetadataClientDriver) client.getMetadataClientDriver()).getZk(),
                gate,
                acls,
                capability.providerScopeId());
    }

    /** May initialize ordinary BK metadata and the permanent unbound creation gate; performs no ledger allocation. */
    public static M5BookKeeperNamespaceAuthorityV2 connect(
            String metadataServiceUri, BookKeeperCapabilitySnapshotV1 capability) throws Exception {
        var configuration = RealBookKeeperClientConfigurationV1.from(metadataServiceUri, capability);
        var client = (BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration)
                .build();
        try {
            var driver = (ZKMetadataClientDriver) client.getMetadataClientDriver();
            String root = URI.create(metadataServiceUri).getPath();
            String instance =
                    new String(driver.getZk().getData(root + "/INSTANCEID", false, null), StandardCharsets.US_ASCII);
            var gate = new M5BookKeeperNamespaceGateV2(driver.getZk(), root, instance, ZkUtils.getACLs(configuration));
            gate.initializeUnbound().get();
            return new M5BookKeeperNamespaceAuthorityV2(client, gate, ZkUtils.getACLs(configuration), capability);
        } catch (Exception failure) {
            client.close();
            throw failure;
        }
    }

    public M5BookKeeperDeleteCellBudgetV2 nativeDeleteCellBudget() {
        return deleteCellBudget;
    }

    /**
     * Minimum of this source-bound confirmed-drain profile, not the lease/orphan profile.
     *
     * <p>The guarded create transaction checks the permanent scope fence, so an already fenced,
     * resolved create cannot appear after a timer. Exact sealed metadata and completed M4 drain
     * similarly grant no time-based lease. This implementation adds no further provider hold.
     * The target coordinator must still prove all those conditions and wait its positive audit
     * grace; this namespace observation alone grants no target eligibility.
     */
    public CompletionStage<Long> confirmedDrainProviderMinimumMillis(PhysicalNamespaceAuthorityBindingV2 expected) {
        if (!capability.credentialIdentityVersion().equals("bk-k0-no-auth:v1")
                || capability.digestType() != com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1.CRC32C) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("BK capability has no admitted confirmed-drain profile"));
        }
        return gate.read().thenApply(observed -> {
            if (!observed.binding().equals(Optional.of(expected))) {
                throw new IllegalStateException("confirmed-drain profile namespace changed");
            }
            return 0L;
        });
    }

    /** Explicit Native Pulsar create profile; the actual protocol adapter must provide pre-commit birth admission. */
    public static M5BookKeeperNamespaceAuthorityV2 connectPulsar(
            String metadataServiceUri,
            BookKeeperCapabilitySnapshotV1 capability,
            M5PulsarNativeCreateSpecV1 spec,
            M5PulsarNativeMetadataDriverV1.BirthAdmission admission)
            throws Exception {
        return connectPulsar(metadataServiceUri, capability, spec, admission, false);
    }

    private static M5BookKeeperNamespaceAuthorityV2 connectPulsar(
            String metadataServiceUri,
            BookKeeperCapabilitySnapshotV1 capability,
            M5PulsarNativeCreateSpecV1 spec,
            M5PulsarNativeMetadataDriverV1.BirthAdmission admission,
            boolean existingOnly)
            throws Exception {
        if (!capability.credentialIdentityVersion().equals("bk-k0-no-auth:v1")
                || capability.digestType() != com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1.CRC32C
                || !capability.providerScopeId().digest().equals(spec.providerScope())
                || !capability.configurationDigest().equals(spec.configuration())
                || capability.ensembleSize() != spec.ensemble()
                || capability.writeQuorumSize() != spec.writeQuorum()
                || capability.ackQuorumSize() != spec.ackQuorum()) {
            throw new IllegalArgumentException("Native Pulsar scope differs from the actual no-auth capability");
        }
        var configuration = RealBookKeeperClientConfigurationV1.from(metadataServiceUri, capability);
        M5PulsarNativeMetadataDriverV1.register();
        configuration.setProperty(M5PulsarNativeMetadataDriverV1.OBSERVE_PROPERTY, existingOnly);
        configuration.setProperty(
                M5PulsarNativeMetadataDriverV1.SPEC_PROPERTY,
                java.util.Base64.getEncoder().encodeToString(spec.encode().toByteArray()));
        configuration.setProperty(
                M5PulsarNativeMetadataDriverV1.BIRTH_PROPERTY,
                java.util.Objects.requireNonNull(admission, "actual birth admission"));
        configuration.setMetadataServiceUri(M5PulsarNativeMetadataDriverV1.SCHEME + metadataServiceUri.substring(2));
        var client = spec.factoryInstance().isPresent() && !existingOnly
                ? new M5PulsarNativeDataClientV2(configuration)
                : (BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration)
                        .build();
        try {
            if (!(client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1)) {
                throw new IllegalStateException("Native Pulsar scheme resolved to an unguarded driver");
            }
            var driver = (ZKMetadataClientDriver) client.getMetadataClientDriver();
            var gate = new M5BookKeeperNamespaceGateV2(
                    driver.getZk(),
                    URI.create(metadataServiceUri).getPath(),
                    spec.nativeInstanceId(),
                    ZkUtils.getACLs(configuration));
            if (!gate.read().get().binding().equals(Optional.of(spec.namespaceBinding()))) {
                throw new IllegalStateException("Native Pulsar namespace assignment changed");
            }
            return new M5BookKeeperNamespaceAuthorityV2(client, gate, ZkUtils.getACLs(configuration), capability);
        } catch (Exception failure) {
            client.close();
            throw failure;
        }
    }

    public record SdkDataWriteSnapshot(
            long dispatchedAdds,
            long dispatchedLacs,
            long dispatchedFences,
            long dispatchedForces,
            long completedWrites,
            long fencedWrites,
            long harmlessForces,
            int unresolvedWrites,
            long retainedBytes,
            boolean admissionClosed) {}

    public SdkDataWriteSnapshot sdkDataWriteSnapshot() {
        if (!(client instanceof M5PulsarNativeDataClientV2 original)) {
            throw new IllegalStateException("Native SDK data lifetime profile is not installed");
        }
        return original.dataWriteSnapshot();
    }

    public java.util.concurrent.CompletableFuture<Void> drainSdkDataWrites() {
        if (!(client instanceof M5PulsarNativeDataClientV2 original)) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("Native SDK data lifetime profile is not installed"));
        }
        return original.drainDataWrites();
    }

    /** Original factory data admission must already be closed; this only reconciles original failed append packets. */
    public java.util.concurrent.CompletableFuture<Void> reconcileSdkDataWrites() {
        if (!(client instanceof M5PulsarNativeDataClientV2 original)
                || !(client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1 driver)) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("Native SDK data lifetime profile is not installed"));
        }
        var expected = driver.createSpec().namespaceBinding();
        return gate.read()
                .thenCompose(before -> {
                    if (!before.binding().equals(java.util.Optional.of(expected))) {
                        return java.util.concurrent.CompletableFuture.failedFuture(
                                new IllegalStateException("original SDK append namespace changed"));
                    }
                    return original.reconcileDataWrites(() -> gate.read().thenAccept(after -> {
                        if (!after.binding().equals(java.util.Optional.of(expected))) {
                            throw new IllegalStateException("original SDK append namespace changed during fencing");
                        }
                    }));
                })
                .copy();
    }

    public java.util.concurrent.CompletableFuture<Void> fencePulsarCreates() {
        if (!(client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1 driver)) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("Native connection has no Pulsar create profile"));
        }
        return driver.fenceCreates();
    }

    public java.util.concurrent.CompletableFuture<Void> requirePulsarReservation(long ledgerId) {
        if (!(client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1 driver)) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("Native connection has no Pulsar create profile"));
        }
        return driver.requireReserved(ledgerId);
    }

    public java.util.concurrent.CompletableFuture<M5PulsarNativeMetadataDriverV1.FencedBirthFacts> readPulsarBirths() {
        return readPulsarBirths(M5PulsarNativeCreateSpecV1.MAX_BULK_METADATA_BYTES);
    }

    public java.util.concurrent.CompletableFuture<M5PulsarNativeMetadataDriverV1.FencedBirthFacts> readPulsarBirths(
            int metadataByteLimit) {
        if (!(client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1 driver)) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("Native connection has no Pulsar create profile"));
        }
        return driver.readFencedBirths(metadataByteLimit);
    }

    public java.util.concurrent.CompletableFuture<M5PulsarNativeMetadataDriverV1.FencedBirthSummaries>
            readPulsarBirthSummaries() {
        if (!(client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1 driver)) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("Native connection has no Pulsar create profile"));
        }
        return driver.readFencedBirthSummaries();
    }

    public java.util.concurrent.CompletableFuture<M5PulsarNativeMetadataDriverV1.BirthFact> readPulsarBirthMetadata(
            M5PulsarNativeMetadataDriverV1.FencedBirthSummaries expected, long ledgerId) {
        if (!(client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1 driver)) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("Native connection has no Pulsar create profile"));
        }
        return driver.readBirthMetadata(expected, ledgerId);
    }

    public java.util.concurrent.CompletableFuture<M5PulsarNativeMetadataDriverV1.PriorSealedBirth>
            readPriorPulsarSealedBirth(long ledgerId, org.apache.bookkeeper.client.api.LedgerMetadata expected) {
        if (!(client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1 driver)) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("Native connection has no Pulsar create profile"));
        }
        return driver.readPriorSealedBirth(ledgerId, expected);
    }

    /** Reopens an existing fenced SDK scope for native observations; admits no metadata mutation. */
    public static M5BookKeeperNamespaceAuthorityV2 observePulsarScope(
            String metadataServiceUri, BookKeeperCapabilitySnapshotV1 capability, M5PulsarNativeCreateSpecV1 spec)
            throws Exception {
        return connectPulsar(
                metadataServiceUri,
                capability,
                spec,
                (id, metadata, creating, mutation) -> java.util.concurrent.CompletableFuture.failedFuture(
                        new IllegalStateException("Native scope connection permits observation only")),
                true);
    }

    /**
     * Borrows the exact client used by this namespace authority for a Native ManagedLedger factory.
     * The borrower must drain its Native owners before this authority closes the client.
     * This does not grant native ledger creation, append or deletion authority.
     */
    public BookKeeper borrowClient() {
        return client;
    }

    /** Configuration snapshot used to construct this exact owned SDK client. */
    public BookKeeperCapabilitySnapshotV1 capability() {
        return capability;
    }

    public M5BookKeeperNativeDeleteQuotaV2 nativeDeleteQuota() {
        return deleteQuota;
    }

    /** Low-level bound deleter for this original Native scope; supplies no M5 eligibility or read release. */
    public M5BookKeeperNativeDeleteAuthorityV2 pulsarDeleteAuthority(PhysicalResourceIdV2.BookKeeperLedger resource) {
        if (!(client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1 driver)) {
            throw new IllegalStateException("Native Pulsar deletion requires its actual tagged SDK scope");
        }
        return driver.deleteAuthority(resource, capability);
    }

    @Override
    public PhysicalResourceIdV2.Namespace namespace() {
        return gate.namespace();
    }

    @Override
    public CompletionStage<Optional<PhysicalNamespaceAuthorityBindingV2>> readBinding() {
        return gate.read().thenApply(M5BookKeeperNamespaceGateV2.Snapshot::binding);
    }

    @Override
    public CompletionStage<PhysicalNamespaceAuthorityBindingV2> bind(MetadataNamespaceIdentityV2 metadataNamespace) {
        return gate.bind(new PhysicalNamespaceAuthorityBindingV2(namespace(), metadataNamespace));
    }

    @Override
    public void close() throws Exception {
        if (client.getMetadataClientDriver() instanceof M5PulsarNativeMetadataDriverV1 driver) {
            driver.fenceCreates().get();
            driver.drainMetadata().get();
            driver.drainBirthReads().get();
        }
        client.close();
    }
}
