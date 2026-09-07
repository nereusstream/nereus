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
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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

    public RealBookKeeperCellSessionV1 newSession() {
        return new RealBookKeeperCellSessionV1(client, capability, new byte[0]);
    }

    /** Captures sealed native metadata through this same owned connection and admitted capability. */
    public java.util.concurrent.CompletionStage<M5BookKeeperDeleteAdapterV1.CaptureResult> captureExactTarget(
            com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1 handle) {
        return new M5BookKeeperDeleteAdapterV1(client, capability, new byte[0]).captureExactTarget(handle);
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
