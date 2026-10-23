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
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.provider.M5ObjectDeleteSessionV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Native exact-version reread through the same S3 client and bucket that own the physical Oxia binding. */
public final class S3M5ObjectDeleteIdentityReaderV2 implements DeleteExternalIdentityReaderV2, AutoCloseable {
    private final S3M5PhysicalNamespaceAuthorityV2 backend;
    private final BoundPhysicalDeleteAuthorityRouteV2 route;
    private final S3C1ObjectProviderTransport transport;
    private final M5ObjectDeleteSessionV1 delete;
    private final ObjectIdentity identity;
    private final String leafPrefix;
    private final CanonicalBytes versionToken;
    private final PhysicalResourceIdV2.ObjectVersion resource;
    private final java.util.concurrent.Executor ioExecutor;

    S3M5ObjectDeleteIdentityReaderV2(
            S3M5PhysicalNamespaceAuthorityV2 backend,
            BoundPhysicalDeleteAuthorityRouteV2 route,
            S3C1ObjectProviderTransport transport,
            CellProviderScopeId scope,
            String exclusiveNamespacePrefix,
            String leafPrefix,
            ObjectIdentity identity,
            CanonicalBytes versionToken,
            long maximumObjectBytes,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes,
            java.util.concurrent.Executor ioExecutor) {
        this.ioExecutor = Objects.requireNonNull(ioExecutor, "ioExecutor");
        this.backend = Objects.requireNonNull(backend, "backend");
        this.route = Objects.requireNonNull(route, "route");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.leafPrefix = Objects.requireNonNull(leafPrefix, "leafPrefix");
        this.versionToken = CanonicalBytes.copyOf(
                Objects.requireNonNull(versionToken, "versionToken").toByteArray());
        if (this.versionToken.isEmpty()
                || this.versionToken.length() > transport.deleteCapabilities().maximumVersionTokenBytes()) {
            throw new IllegalArgumentException("S3 delete version token is outside the admitted bound");
        }
        this.resource = new PhysicalResourceIdV2.ObjectVersion(
                backend.namespace(),
                CanonicalUtf8.fromString(identity.key()),
                PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION,
                CanonicalUtf8.fromBytes(this.versionToken.toByteArray()));
        this.delete = new M5ObjectDeleteSessionV1(
                transport,
                scope,
                exclusiveNamespacePrefix,
                maximumObjectBytes,
                maximumListPages,
                maximumListKeys,
                maximumListKeyBytes,
                maximumSingleKeyBytes);
    }

    public PhysicalResourceIdV2.ObjectVersion resource() {
        return resource;
    }

    /** Capture only after CAS-1; a changed body/version or incomplete inventory cannot create an intent. */
    public CompletionStage<ExactExternalIdentityV1> capture(VersionedValue exactFenced) {
        Objects.requireNonNull(exactFenced, "exactFenced");
        TargetDeleteAuthorityV1 fenced =
                M5TargetDeleteAuthorityCodecV1.decodeAuthority(exactFenced.canonicalStoredBytes());
        if (!exactFenced.key().equals(resource.authorityKey())
                || !fenced.target().resourceId().equals(resource)
                || fenced.state() != TargetDeleteAuthorityStateV1.READ_FENCED_V1
                || fenced.recoveryVeto().isPresent()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("S3 identity capture requires this exact read fence"));
        }
        return requireRoute()
                .thenCompose(ignored -> route.read(resource.authorityKey()))
                .thenCompose(observed -> {
                    if (!observed.equals(Optional.of(exactFenced))) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("S3 identity capture lacks the exact persisted read fence"));
                    }
                    return observe()
                            .thenApply(observation ->
                                    ExactExternalIdentityV1.create(fenced, observation, encodeIdentity(observation)));
                });
    }

    /** Dispatches only the version named by an exact persisted INTENT; uncertain results leave INTENT pending. */
    public CompletionStage<M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1> deletePersistedIntent(
            VersionedValue exactIntent, DeleteObservationAuthorityVerifierV2 ownerAuthority) {
        Objects.requireNonNull(exactIntent, "exactIntent");
        var intent = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exactIntent.canonicalStoredBytes());
        if (!exactIntent.key().equals(resource.authorityKey())
                || !intent.target().resourceId().equals(resource)
                || intent.state() != TargetDeleteAuthorityStateV1.DELETE_INTENT_V1) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("S3 physical delete requires this exact target INTENT"));
        }
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                route, Objects.requireNonNull(ownerAuthority, "ownerAuthority"), this);
        return coordinator.inspect(resource.authorityKey()).thenCompose(stored -> {
            if (stored.isPresent() && stored.orElseThrow().state() == TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
                // A lost caller response must reconcile the exact full/compact successor without reopening dispatch.
                return coordinator.completeAbsent(exactIntent);
            }
            return dispatchCurrentIntent(coordinator, exactIntent).exceptionallyCompose(failure -> coordinator
                    .inspect(resource.authorityKey())
                    .thenCompose(current -> current.isPresent()
                                    && current.orElseThrow().state() == TargetDeleteAuthorityStateV1.DELETE_DONE_V1
                            ? coordinator.completeAbsent(exactIntent)
                            : CompletableFuture.failedFuture(failure)));
        });
    }

    private CompletionStage<M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1> dispatchCurrentIntent(
            M5TargetDeleteAuthorityCoordinatorV1 coordinator, VersionedValue exactIntent) {
        return coordinator
                .requireCurrentDispatch(exactIntent)
                .thenComposeAsync(
                        observation -> {
                            if (observation == ExternalIdentityObservationV1.ABSENT_EXACT_V1) {
                                return coordinator.completeAbsent(exactIntent);
                            }
                            ObjectProviderTransport.ConditionalDeleteResult providerResult;
                            try {
                                providerResult = delete.deleteExactVersion(identity, versionToken);
                            } catch (IOException | RuntimeException failure) {
                                return CompletableFuture.failedFuture(failure);
                            }
                            try {
                                var reconciled = delete.reconcile(identity, leafPrefix, versionToken);
                                return switch (reconciled.kind()) {
                                    case AUTHORITATIVELY_ABSENT, EXACT_OLD_VERSION_ABSENT_OTHER_CURRENT ->
                                        coordinator.completeAbsent(exactIntent);
                                    case EXACT_OLD_VERSION_REMAINS ->
                                        CompletableFuture.failedFuture(new IllegalStateException(
                                                "S3 exact old version remains after delete outcome " + providerResult));
                                    case DIFFERENT_VERSION_OR_BODY, OUTCOME_UNKNOWN ->
                                        CompletableFuture.failedFuture(new IllegalStateException(
                                                "S3 exact old version deletion remains unproven"));
                                };
                            } catch (IOException | RuntimeException failure) {
                                return CompletableFuture.failedFuture(failure);
                            }
                        },
                        ioExecutor);
    }

    @Override
    public CompletionStage<ExternalIdentityObservationV1> rereadExact(ExactExternalIdentityV1 expected) {
        Objects.requireNonNull(expected, "expected");
        if (!expected.resourceId().equals(resource)
                || !expected.exactIdentityBytes().equals(encodeIdentity(expected.observation()))) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("S3 delete identity differs from the bound target"));
        }
        return observe().thenApply(observation -> {
            if (expected.observation() == ExternalIdentityObservationV1.ABSENT_EXACT_V1
                    && observation == ExternalIdentityObservationV1.PRESENT_EXACT_V1) {
                throw new IllegalStateException("an absent S3 immutable version reappeared");
            }
            return observation;
        });
    }

    private CompletionStage<ExternalIdentityObservationV1> observe() {
        return requireRoute()
                .thenComposeAsync(
                        ignored -> {
                            try {
                                var result = delete.reconcile(identity, leafPrefix, versionToken);
                                return switch (result.kind()) {
                                    case EXACT_OLD_VERSION_REMAINS ->
                                        CompletableFuture.completedFuture(
                                                ExternalIdentityObservationV1.PRESENT_EXACT_V1);
                                    case AUTHORITATIVELY_ABSENT, EXACT_OLD_VERSION_ABSENT_OTHER_CURRENT ->
                                        CompletableFuture.completedFuture(
                                                ExternalIdentityObservationV1.ABSENT_EXACT_V1);
                                    case DIFFERENT_VERSION_OR_BODY, OUTCOME_UNKNOWN ->
                                        CompletableFuture.failedFuture(new IllegalStateException(
                                                "S3 exact version identity changed or remains unknown"));
                                };
                            } catch (IOException | RuntimeException failure) {
                                return CompletableFuture.failedFuture(failure);
                            }
                        },
                        ioExecutor);
    }

    private CompletionStage<Void> requireRoute() {
        return CompletableFuture.completedFuture(null)
                .thenComposeAsync(ignored -> backend.readBinding(), ioExecutor)
                .thenCompose(binding -> {
                    if (binding.isEmpty()) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("S3 native physical namespace binding is absent"));
                    }
                    return route.requireActiveResource(resource).thenCompose(actual -> {
                        if (!actual.equals(binding.orElseThrow())) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("S3 delete route differs from native backend binding"));
                        }
                        return CompletableFuture.completedFuture(null)
                                .thenComposeAsync(ignored -> backend.readBinding(), ioExecutor)
                                .thenAccept(current -> {
                                    if (!current.equals(binding)) {
                                        throw new IllegalStateException(
                                                "S3 backend binding changed during delete route admission");
                                    }
                                });
                    });
                });
    }

    private CanonicalBytes encodeIdentity(ExternalIdentityObservationV1 observation) {
        CanonicalBytes resourceBytes = resource.canonicalBytes();
        return CanonicalBytes.copyOf(ByteBuffer.allocate(85 + resourceBytes.length())
                .putInt(0x4d354f49) // M5OI
                .putInt(1)
                .putInt(resourceBytes.length())
                .put(resourceBytes.toByteArray())
                .put(delete.providerScopeId().digest().bytes().toByteArray())
                .putLong(identity.bodyLength())
                .put(identity.bodySha256().bytes().toByteArray())
                .put((byte) (observation == ExternalIdentityObservationV1.PRESENT_EXACT_V1 ? 1 : 2))
                .array());
    }

    @Override
    public void close() {
        transport.close();
    }
}
