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

package com.nereusstream.metadata.oxia.v2.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Actual immutable namespace-marker read, independent of Cell roots and endpoint aliases. The explicit provisioning
 * path creates once with a random identity; operational reconnect requires the previously bound native identity.
 * Namespace cloning, marker deletion/rewrite and compatibility migration are outside this admitted profile.
 */
public final class OxiaPhysicalMetadataNamespaceV2 {
    public static final String MARKER_KEY = "v2/m5-physical-metadata-namespace-v2";
    private final AsyncOxiaClient client;
    private final OxiaConditionalClient conditional;
    private final MetadataNamespaceIdentityV2 identity;

    private OxiaPhysicalMetadataNamespaceV2(
            AsyncOxiaClient client, OxiaConditionalClient conditional, MetadataNamespaceIdentityV2 identity) {
        this.client = client;
        this.conditional = conditional;
        this.identity = identity;
    }

    /** Explicit namespace provisioning is not an old-authority compatibility check or runtime activation receipt. */
    public static CompletionStage<OxiaPhysicalMetadataNamespaceV2> provision(AsyncOxiaClient client) {
        Objects.requireNonNull(client, "client");
        var conditional = new AsyncOxiaConditionalClient(client);
        return conditional.read(MARKER_KEY).thenCompose(observed -> {
            if (observed.isPresent()) {
                return CompletableFuture.completedFuture(from(client, conditional, observed.orElseThrow()));
            }
            var id = UUID.randomUUID();
            var candidate = new MetadataNamespaceIdentityV2(
                    new Id128(id.getMostSignificantBits(), id.getLeastSignificantBits()), 0);
            return conditional
                    .createIfAbsent(MARKER_KEY, candidate.markerBytes())
                    .handle((ignored, failure) -> null)
                    .thenCompose(ignored -> conditional.read(MARKER_KEY))
                    .thenApply(current -> from(
                            client,
                            conditional,
                            current.orElseThrow(() -> new IllegalStateException(
                                    "native metadata namespace provisioning is unresolved"))));
        });
    }

    /** Never recreates a missing marker, including after a client/server restart. */
    public static CompletionStage<OxiaPhysicalMetadataNamespaceV2> connect(
            AsyncOxiaClient client, MetadataNamespaceIdentityV2 expected) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(expected, "expected");
        var conditional = new AsyncOxiaConditionalClient(client);
        return conditional.read(MARKER_KEY).thenApply(observed -> {
            var namespace = from(
                    client,
                    conditional,
                    observed.orElseThrow(
                            () -> new IllegalStateException("bound native metadata namespace marker is missing")));
            if (!namespace.identity.equals(expected)) {
                throw new IllegalArgumentException(
                        "actual metadata backend namespace differs from the physical binding");
            }
            return namespace;
        });
    }

    public MetadataNamespaceIdentityV2 identity() {
        return identity;
    }

    public CompletionStage<Void> requireCurrent() {
        return conditional.read(MARKER_KEY).thenApply(observed -> {
            var actual = observed.map(value -> from(client, conditional, value).identity);
            if (actual.filter(identity::equals).isEmpty()) {
                throw new IllegalStateException("native metadata namespace identity disappeared or changed");
            }
            return null;
        });
    }

    /** Bind at the physical backend first; a different metadata namespace cannot take over an existing assignment. */
    public CompletionStage<PhysicalNamespaceAuthorityBindingV2> bind(NativePhysicalNamespaceAuthorityV2 backend) {
        Objects.requireNonNull(backend, "backend");
        return requireCurrent().thenCompose(ignored -> backend.bind(identity)).thenApply(binding -> {
            requireBindingIdentity(backend, binding);
            return binding;
        });
    }

    /** The physical backend selects the root; callers cannot supply another root or metadata client. */
    public CompletionStage<OxiaQuotaTargetDeleteStoreV2> openAuthorityRoute(
            NativePhysicalNamespaceAuthorityV2 backend, ExactMetadataTransactionStoreV1 authoritativeFacts) {
        Objects.requireNonNull(backend, "backend");
        Objects.requireNonNull(authoritativeFacts, "authoritativeFacts");
        return currentBinding(backend).thenApply(binding -> {
            return new OxiaQuotaTargetDeleteStoreV2(
                    client,
                    guardedClient(backend),
                    new Layout(binding.authorityRoot(), binding.physicalNamespace()),
                    authoritativeFacts,
                    () -> currentBinding(backend));
        });
    }

    /** Run roots share the actual bound metadata namespace and pre-admitted permanent physical ticket route. */
    public CompletionStage<com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaRunRootAuthorityV2> openKafkaRunRoots(
            NativePhysicalNamespaceAuthorityV2 backend,
            ExactMetadataTransactionStoreV1 authoritativeFacts,
            com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2.Scope scope,
            com.nereusstream.storage.api.kafka.KafkaRunRootVerifierV2 verifier) {
        return openAuthorityRoute(backend, authoritativeFacts).thenCompose(route -> currentBinding(backend)
                .thenApply(binding -> new com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaRunRootAuthorityV2(
                        guardedClient(backend),
                        binding,
                        scope,
                        verifier,
                        new com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2(
                                new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(route)))));
    }

    private OxiaConditionalClient guardedClient(NativePhysicalNamespaceAuthorityV2 backend) {
        return new OxiaConditionalClient() {
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return currentBinding(backend).thenCompose(ignored -> conditional.read(key));
            }

            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
                return currentBinding(backend).thenCompose(ignored -> conditional.createIfAbsent(key, value));
            }

            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
                return currentBinding(backend).thenCompose(ignored -> conditional.compareAndSet(key, value, version));
            }
        };
    }

    private CompletionStage<PhysicalNamespaceAuthorityBindingV2> currentBinding(
            NativePhysicalNamespaceAuthorityV2 backend) {
        return requireCurrent().thenCompose(ignored -> backend.readBinding()).thenApply(observed -> {
            var binding = observed.orElseThrow(
                    () -> new IllegalStateException("physical namespace has no metadata authority assignment"));
            requireBindingIdentity(backend, binding);
            return binding;
        });
    }

    private void requireBindingIdentity(
            NativePhysicalNamespaceAuthorityV2 backend, PhysicalNamespaceAuthorityBindingV2 binding) {
        if (!binding.physicalNamespace().equals(backend.namespace())
                || !binding.metadataNamespace().equals(identity)) {
            throw new IllegalArgumentException("physical backend metadata namespace assignment differs");
        }
    }

    private static OxiaPhysicalMetadataNamespaceV2 from(
            AsyncOxiaClient client, OxiaConditionalClient conditional, AuthorityRecord record) {
        if (!record.key().equals(MARKER_KEY)) {
            throw new IllegalArgumentException("native namespace read returned another key");
        }
        return new OxiaPhysicalMetadataNamespaceV2(
                client,
                conditional,
                MetadataNamespaceIdentityV2.fromNativeMarker(record.storedBytes(), record.versionId()));
    }
}
