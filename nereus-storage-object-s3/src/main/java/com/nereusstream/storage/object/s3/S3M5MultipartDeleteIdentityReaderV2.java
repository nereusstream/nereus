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

package com.nereusstream.storage.object.s3;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2;
import com.nereusstream.storage.object.gc.DeleteExternalIdentityReaderV2;
import com.nereusstream.storage.object.gc.DeleteObservationAuthorityVerifierV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.provider.M5MultipartCleanupSessionV1;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Native one-upload identity and INTENT-bound exact abort through the same S3/Oxia namespace assignment. */
public final class S3M5MultipartDeleteIdentityReaderV2 implements DeleteExternalIdentityReaderV2, AutoCloseable {
    private final S3M5PhysicalNamespaceAuthorityV2 backend;
    private final BoundPhysicalDeleteAuthorityRouteV2 route;
    private final S3C1ObjectProviderTransport transport;
    private final M5MultipartCleanupSessionV1 cleanup;
    private final Set<ObjectProviderTransport.MultipartUploadIdentity> owned;
    private final com.nereusstream.domain.bytes.Sha256Digest inventoryRoot;
    private final PhysicalResourceIdV2.MultipartUpload resource;

    S3M5MultipartDeleteIdentityReaderV2(
            S3M5PhysicalNamespaceAuthorityV2 backend,
            BoundPhysicalDeleteAuthorityRouteV2 route,
            S3C1ObjectProviderTransport transport,
            CellProviderScopeId scope,
            String exclusiveNamespacePrefix,
            String exactKey,
            CanonicalBytes uploadId,
            int maximumListPages,
            long maximumListUploads,
            long maximumListIdentityBytes,
            int maximumSingleKeyBytes) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.route = Objects.requireNonNull(route, "route");
        this.transport = Objects.requireNonNull(transport, "transport");
        CanonicalBytes token = CanonicalBytes.copyOf(
                Objects.requireNonNull(uploadId, "uploadId").toByteArray());
        this.resource = new PhysicalResourceIdV2.MultipartUpload(
                backend.namespace(), CanonicalUtf8.fromString(exactKey), CanonicalUtf8.fromBytes(token.toByteArray()));
        this.owned = Set.of(new ObjectProviderTransport.MultipartUploadIdentity(exactKey, token));
        this.cleanup = new M5MultipartCleanupSessionV1(
                transport,
                scope,
                exclusiveNamespacePrefix,
                maximumListPages,
                maximumListUploads,
                maximumListIdentityBytes,
                maximumSingleKeyBytes);
        this.inventoryRoot = cleanup.inventoryRoot(owned);
    }

    public PhysicalResourceIdV2.MultipartUpload resource() {
        return resource;
    }

    public CompletionStage<ExactExternalIdentityV1> capture(VersionedValue exactFenced) {
        Objects.requireNonNull(exactFenced, "exactFenced");
        var fenced = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exactFenced.canonicalStoredBytes());
        if (!exactFenced.key().equals(resource.authorityKey())
                || !fenced.target().resourceId().equals(resource)
                || fenced.state() != TargetDeleteAuthorityStateV1.READ_FENCED_V1
                || fenced.recoveryVeto().isPresent()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("S3 upload identity capture requires this exact read fence"));
        }
        return requireRoute()
                .thenCompose(ignored -> route.read(resource.authorityKey()))
                .thenCompose(observed -> {
                    if (!observed.equals(Optional.of(exactFenced))) {
                        return CompletableFuture.failedFuture(new IllegalStateException(
                                "S3 upload identity capture lacks the exact persisted read fence"));
                    }
                    return observe()
                            .thenApply(observation ->
                                    ExactExternalIdentityV1.create(fenced, observation, encodeIdentity(observation)));
                });
    }

    public CompletionStage<M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1> deletePersistedIntent(
            VersionedValue exactIntent, DeleteObservationAuthorityVerifierV2 ownerAuthority) {
        Objects.requireNonNull(exactIntent, "exactIntent");
        var intent = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exactIntent.canonicalStoredBytes());
        if (!exactIntent.key().equals(resource.authorityKey())
                || !intent.target().resourceId().equals(resource)
                || intent.state() != TargetDeleteAuthorityStateV1.DELETE_INTENT_V1) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("S3 upload abort requires this exact target INTENT"));
        }
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                route, Objects.requireNonNull(ownerAuthority, "ownerAuthority"), this);
        return coordinator.inspect(resource.authorityKey()).thenCompose(stored -> {
            if (stored.isPresent() && stored.orElseThrow().state() == TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
                return coordinator.completeAbsent(exactIntent);
            }
            return coordinator
                    .requireCurrentDispatch(exactIntent)
                    .thenCompose(observation -> {
                        if (observation == ExternalIdentityObservationV1.ABSENT_EXACT_V1) {
                            return coordinator.completeAbsent(exactIntent);
                        }
                        try {
                            var result = cleanup.cleanupOwned(inventoryRoot, owned);
                            return result.kind() == M5MultipartCleanupSessionV1.CleanupKindV1.AUTHORITATIVELY_ABSENT
                                    ? coordinator.completeAbsent(exactIntent)
                                    : CompletableFuture.failedFuture(new IllegalStateException(
                                            "S3 exact upload abort remains pending: " + result.kind()));
                        } catch (IOException | RuntimeException failure) {
                            return CompletableFuture.failedFuture(failure);
                        }
                    })
                    .exceptionallyCompose(failure -> coordinator
                            .inspect(resource.authorityKey())
                            .thenCompose(current -> current.isPresent()
                                            && current.orElseThrow().state()
                                                    == TargetDeleteAuthorityStateV1.DELETE_DONE_V1
                                    ? coordinator.completeAbsent(exactIntent)
                                    : CompletableFuture.failedFuture(failure)));
        });
    }

    @Override
    public CompletionStage<ExternalIdentityObservationV1> rereadExact(ExactExternalIdentityV1 expected) {
        Objects.requireNonNull(expected, "expected");
        if (!expected.resourceId().equals(resource)
                || !expected.exactIdentityBytes().equals(encodeIdentity(expected.observation()))) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("S3 upload identity differs from the bound target"));
        }
        return observe().thenApply(observation -> {
            if (expected.observation() == ExternalIdentityObservationV1.ABSENT_EXACT_V1
                    && observation == ExternalIdentityObservationV1.PRESENT_EXACT_V1) {
                throw new IllegalStateException("an absent S3 upload ID reappeared");
            }
            return observation;
        });
    }

    private CompletionStage<ExternalIdentityObservationV1> observe() {
        return requireRoute().thenCompose(ignored -> {
            try {
                var result = cleanup.observeOwned(inventoryRoot, owned);
                return switch (result.kind()) {
                    case AUTHORITATIVELY_ABSENT ->
                        CompletableFuture.completedFuture(ExternalIdentityObservationV1.ABSENT_EXACT_V1);
                    case EXACT_OWNED_RESIDUE_REMAINS ->
                        CompletableFuture.completedFuture(ExternalIdentityObservationV1.PRESENT_EXACT_V1);
                    case DIFFERENT_OR_FOREIGN_IDENTITY, OUTCOME_UNKNOWN ->
                        CompletableFuture.failedFuture(
                                new IllegalStateException("S3 exact upload inventory is foreign or incomplete"));
                };
            } catch (IOException | RuntimeException failure) {
                return CompletableFuture.failedFuture(failure);
            }
        });
    }

    private CompletionStage<Void> requireRoute() {
        return backend.readBinding().thenCompose(binding -> {
            if (binding.isEmpty()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("S3 native physical namespace binding is absent"));
            }
            return route.requireActiveResource(resource).thenCompose(actual -> {
                if (!actual.equals(binding.orElseThrow())) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("S3 upload route differs from native backend binding"));
                }
                return backend.readBinding().thenAccept(current -> {
                    if (!current.equals(binding)) {
                        throw new IllegalStateException("S3 backend binding changed during upload route admission");
                    }
                });
            });
        });
    }

    private CanonicalBytes encodeIdentity(ExternalIdentityObservationV1 observation) {
        CanonicalBytes resourceBytes = resource.canonicalBytes();
        return CanonicalBytes.copyOf(ByteBuffer.allocate(49 + resourceBytes.length())
                .putInt(0x4d354d49) // M5MI
                .putInt(1)
                .putInt(resourceBytes.length())
                .put(resourceBytes.toByteArray())
                .put(inventoryRoot.bytes().toByteArray())
                .putInt(owned.size())
                .put((byte) (observation == ExternalIdentityObservationV1.PRESENT_EXACT_V1 ? 1 : 2))
                .array());
    }

    @Override
    public void close() {
        transport.close();
    }
}
