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

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteCellBudgetV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteAuthorityV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteAuthorityV2.PulsarTarget;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteAuthorityV2.Snapshot;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteIntentV2;
import com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2;
import com.nereusstream.storage.object.gc.DeleteExternalIdentityReaderV2;
import com.nereusstream.storage.object.gc.DeleteObservationAuthorityVerifierV2;
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Original Native BK epoch, exact sealed identity and bound dispatch. Eligibility and grace are separate. */
final class PulsarNativeBookKeeperDeleteV1
        implements DeleteExternalIdentityReaderV2, DeleteObservationAuthorityVerifierV2 {
    private final M5BookKeeperNativeDeleteAuthorityV2 nativeAuthority;
    private final ExactMetadataTransactionStoreV1 metadata;
    private final PhysicalResourceIdV2.BookKeeperLedger resource;
    private final Sha256Digest resourceSha;
    private final String key;
    private Snapshot epoch;
    private UUID originalOwner;
    private CompletableFuture<Optional<Snapshot>> originalPredecessor;
    private CompletableFuture<DeleteObservationContextV2> originalClaim;

    PulsarNativeBookKeeperDeleteV1(
            M5BookKeeperNativeDeleteAuthorityV2 nativeAuthority, ExactMetadataTransactionStoreV1 metadata) {
        this.nativeAuthority = Objects.requireNonNull(nativeAuthority, "original native authority");
        this.metadata = Objects.requireNonNull(metadata, "original metadata observation port");
        resource = nativeAuthority.resource();
        resourceSha = Sha256Digest.hash(resource.canonicalBytes());
        key = "v2/bk-native-delete-epoch/" + resourceSha.toHex() + "/fact-v1";
    }

    synchronized CompletionStage<DeleteObservationContextV2> claim(UUID owner) {
        if (originalOwner != null && !originalOwner.equals(owner)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Original Native BK GC owner changed"));
        }
        originalOwner = owner;
        if (originalClaim != null && !originalClaim.isCompletedExceptionally()) {
            return originalClaim.copy();
        }
        if (originalPredecessor == null || originalPredecessor.isCompletedExceptionally()) {
            originalPredecessor = nativeAuthority.read().toCompletableFuture();
        }
        originalClaim = originalPredecessor
                .thenCompose(previous -> nativeAuthority.claim(previous, owner).thenApply(value -> {
                    epoch = value;
                    var fact = fact(value(value));
                    return new DeleteObservationContextV2(
                            value.epoch(), fact, fact, previous.isPresent() ? Optional.of(fact) : Optional.empty());
                }))
                .toCompletableFuture();
        return originalClaim.copy();
    }

    String factKey() {
        return key;
    }

    CompletionStage<VersionedValue> readFact() {
        return nativeAuthority
                .read()
                .thenApply(value ->
                        value(value.orElseThrow(() -> new IllegalStateException("Native BK GC epoch disappeared"))));
    }

    private VersionedValue value(Snapshot snapshot) {
        if (!resource.equals(snapshot.resource())) {
            throw new IllegalArgumentException("Native BK epoch belongs to another physical resource");
        }
        return VersionedValue.of(
                key,
                snapshot.encode(),
                new MetadataVersion(CanonicalBytes.copyOf(ByteBuffer.allocate(40)
                        .putInt(0x4d354556)
                        .put(resourceSha.bytes().toByteArray())
                        .putInt(snapshot.nativeVersion())
                        .array())));
    }

    private int nativeVersion(AuthorityFactV1 fact) {
        if (!key.equals(fact.key()) || fact.metadataVersion().value().length() != 40) {
            throw new IllegalArgumentException("Native BK epoch fact route differs");
        }
        var in = ByteBuffer.wrap(fact.metadataVersion().value().toByteArray());
        int magic = in.getInt();
        byte[] digest = new byte[32];
        in.get(digest);
        int version = in.getInt();
        if (magic != 0x4d354556 || !resourceSha.equals(Sha256Digest.copyOf(digest)) || version < 0) {
            throw new IllegalArgumentException("Native BK epoch fact version differs");
        }
        return version;
    }

    @Override
    public CompletionStage<Void> requireCurrent(PhysicalResourceIdV2 target, DeleteObservationContextV2 context) {
        if (epoch == null
                || !target.equals(resource)
                || !context.coordinatorOwner().equals(context.capability())
                || context.observationEpoch() != epoch.epoch()
                || !context.coordinatorOwner().equals(fact(value(epoch)))) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native BK GC context differs"));
        }
        return nativeAuthority.requireCurrent(epoch);
    }

    @Override
    public CompletionStage<Void> requirePredecessorFenced(
            PhysicalResourceIdV2 target, DeleteObservationContextV2 previous, DeleteObservationContextV2 successor) {
        if (!previous.coordinatorOwner().equals(previous.capability())
                || !successor.predecessorOwnerFenced().equals(Optional.of(successor.coordinatorOwner()))
                || nativeVersion(previous.coordinatorOwner()) >= nativeVersion(successor.coordinatorOwner())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native BK predecessor is not fenced"));
        }
        return requireCurrent(target, successor);
    }

    CompletionStage<ExactExternalIdentityV1> capture(TargetDeleteAuthorityV1 fenced) {
        if (!fenced.target().resourceId().equals(resource)
                || fenced.state() != TargetDeleteAuthorityStateV1.READ_FENCED_V1
                || fenced.recoveryVeto().isPresent()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native BK exact read fence differs"));
        }
        return nativeAuthority
                .capturePulsarTarget()
                .thenApply(target -> ExactExternalIdentityV1.create(
                        fenced,
                        target.isPresent()
                                ? ExternalIdentityObservationV1.PRESENT_EXACT_V1
                                : ExternalIdentityObservationV1.ABSENT_EXACT_V1,
                        identity(target)));
    }

    private CanonicalBytes identity(Optional<PulsarTarget> target) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(0x4d355049); // M5PI
                out.writeInt(1);
                out.writeInt(resource.canonicalBytes().length());
                out.write(resource.canonicalBytes().toByteArray());
                out.writeBoolean(target.isPresent());
                if (target.isPresent()) {
                    var exact = target.orElseThrow();
                    out.write(exact.createScope().bytes().toByteArray());
                    out.writeLong(exact.metadataVersion());
                    out.writeLong(exact.lastEntryId());
                    out.writeLong(exact.length());
                    out.write(exact.metadataSha256().bytes().toByteArray());
                }
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (java.io.IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private CompletionStage<Optional<PulsarTarget>> rereadTarget(ExactExternalIdentityV1 expected) {
        if (!expected.resourceId().equals(resource)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native BK identity route differs"));
        }
        return nativeAuthority.capturePulsarTarget().thenApply(actual -> {
            if (actual.isPresent()
                    && (expected.observation() != ExternalIdentityObservationV1.PRESENT_EXACT_V1
                            || !identity(actual).equals(expected.exactIdentityBytes()))) {
                throw new IllegalStateException("Native BK sealed target changed or absent ledger reappeared");
            }
            return actual;
        });
    }

    @Override
    public CompletionStage<ExternalIdentityObservationV1> rereadExact(ExactExternalIdentityV1 expected) {
        return rereadTarget(expected)
                .thenApply(actual -> actual.isPresent()
                        ? ExternalIdentityObservationV1.PRESENT_EXACT_V1
                        : ExternalIdentityObservationV1.ABSENT_EXACT_V1);
    }

    CompletionStage<M5BookKeeperNativeDeleteIntentV2> bindIntent(
            BoundPhysicalDeleteAuthorityRouteV2 route, VersionedValue exact, EpochGraceReady ready) {
        var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        var context = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(authority);
        return requireDispatch(route, exact, ready)
                .thenCompose(ignored -> nativeAuthority.readIntent())
                .thenCompose(previous -> {
                    if (previous.filter(value -> value.epoch().equals(epoch)
                                    && value.intentAuthoritySha256().equals(exact.canonicalStoredSha256())
                                    && value.dispatchTokenSha256()
                                            .equals(authority
                                                    .deleteIntent()
                                                    .orElseThrow()
                                                    .dispatchTokenSha256()))
                            .isPresent()) {
                        return nativeAuthority
                                .requireIntent(previous.orElseThrow())
                                .thenApply(ignored -> previous.orElseThrow());
                    }
                    return rereadTarget(authority.externalIdentity().orElseThrow())
                            .thenCompose(target -> nativeAuthority.bindIntent(
                                    epoch,
                                    authority.deleteIntent().orElseThrow().dispatchTokenSha256(),
                                    exact.canonicalStoredSha256(),
                                    target.orElseThrow(() ->
                                                    new IllegalStateException("Native BK is absent; reconcile DONE"))
                                            .metadataSha256()));
                })
                .thenCompose(bound -> requireCurrent(resource, context)
                        .thenCompose(ignored -> requireDispatch(route, exact, ready))
                        .thenApply(ignored -> bound));
    }

    CompletionStage<M5BookKeeperDeleteCellBudgetV2.Result> dispatch(
            BoundPhysicalDeleteAuthorityRouteV2 route,
            M5BookKeeperDeleteCellBudgetV2 budget,
            VersionedValue exact,
            M5BookKeeperNativeDeleteIntentV2 bound,
            EpochGraceReady ready) {
        var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        if (!bound.epoch().equals(epoch)
                || !bound.intentAuthoritySha256().equals(exact.canonicalStoredSha256())
                || !bound.dispatchTokenSha256()
                        .equals(authority.deleteIntent().orElseThrow().dispatchTokenSha256())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native BK bound INTENT differs"));
        }
        return requireDispatch(route, exact, ready)
                .thenCompose(
                        ignored -> rereadTarget(authority.externalIdentity().orElseThrow()))
                .thenCompose(target -> requireDispatch(route, exact, ready)
                        .thenCompose(ignored -> nativeAuthority.deletePulsarExact(
                                budget,
                                bound,
                                target.orElseThrow(
                                        () -> new IllegalStateException("Native BK is absent; reconcile DONE")))))
                .thenApply(result -> result);
    }

    CompletionStage<M5BookKeeperDeleteCellBudgetV2.Result> reconcileAbsence(
            BoundPhysicalDeleteAuthorityRouteV2 route,
            M5BookKeeperDeleteCellBudgetV2 budget,
            VersionedValue exact,
            M5BookKeeperNativeDeleteIntentV2 bound,
            EpochGraceReady ready) {
        var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        if (!bound.epoch().equals(epoch)
                || !bound.intentAuthoritySha256().equals(exact.canonicalStoredSha256())
                || !bound.dispatchTokenSha256()
                        .equals(authority.deleteIntent().orElseThrow().dispatchTokenSha256())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native BK recovery INTENT differs"));
        }
        return requireDispatch(route, exact, ready)
                .thenCompose(ignored -> nativeAuthority.reconcileCellDeleteAbsence(budget, bound))
                .thenApply(result -> result);
    }

    /** A persisted callback-terminal hold is observed; it can never cause a second original delete dispatch. */
    CompletionStage<Void> deleteOrReconcile(
            BoundPhysicalDeleteAuthorityRouteV2 route,
            M5BookKeeperDeleteCellBudgetV2 budget,
            VersionedValue exact,
            M5BookKeeperNativeDeleteIntentV2 bound,
            EpochGraceReady ready) {
        return requireDispatch(route, exact, ready)
                .thenCompose(ignored -> budget.snapshot())
                .thenCompose(cell -> {
                    var retained = cell.reservations().stream()
                            .filter(hold -> hold.resource().equals(resource.sha256()))
                            .findFirst();
                    CompletionStage<Void> work;
                    if (retained.isPresent()) {
                        if (!retained.orElseThrow().nativeIntent().equals(Sha256Digest.hash(bound.encode()))) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("Native BK Cell hold belongs to another intent"));
                        }
                        work = reconcileAbsence(route, budget, exact, bound, ready)
                                .thenAccept(this::requireAbsentResult);
                    } else {
                        var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
                        work = rereadTarget(authority.externalIdentity().orElseThrow())
                                .thenCompose(target -> target.isEmpty()
                                        ? CompletableFuture.completedFuture(null)
                                        : dispatch(route, budget, exact, bound, ready)
                                                .thenAccept(this::requireAbsentResult));
                    }
                    return work;
                });
    }

    private void requireAbsentResult(M5BookKeeperDeleteCellBudgetV2.Result result) {
        if (result.reservationRetained()
                || result.deleteResult().outcome()
                        != com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.DeleteOutcome
                                .AUTHORITATIVELY_ABSENT) {
            throw new IllegalStateException("Original Native BK delete remains unknown or retained");
        }
    }

    /** A new actual epoch fences the old native transaction before its exact Cell hold can be released. */
    CompletionStage<Void> reconcileFencedHold(M5BookKeeperDeleteCellBudgetV2 budget) {
        return nativeAuthority.readIntent().thenCompose(previous -> {
            if (previous.isEmpty() || previous.orElseThrow().epoch().equals(epoch)) {
                return CompletableFuture.completedFuture(null);
            }
            var old = previous.orElseThrow();
            return budget.snapshot().thenCompose(cell -> {
                var hold = cell.reservations().stream()
                        .filter(value -> value.resource().equals(resource.sha256()))
                        .findFirst();
                if (hold.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                if (!hold.orElseThrow().nativeIntent().equals(Sha256Digest.hash(old.encode()))) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Native BK old hold identity differs"));
                }
                return nativeAuthority.reconcileFencedCellDelete(budget, old).thenAccept(released -> {
                    if (!released) {
                        throw new IllegalStateException("Native BK old Cell hold remains retained");
                    }
                });
            });
        });
    }

    CompletionStage<Void> finishObservedAbsence(
            BoundPhysicalDeleteAuthorityRouteV2 route,
            M5BookKeeperDeleteCellBudgetV2 budget,
            VersionedValue exact,
            EpochGraceReady ready) {
        var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        return requireDispatch(route, exact, ready)
                .thenCompose(ignored -> rereadExact(authority.externalIdentity().orElseThrow()))
                .thenCompose(observed -> {
                    if (observed != ExternalIdentityObservationV1.ABSENT_EXACT_V1) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("Native BK is not authoritatively absent"));
                    }
                    return budget.snapshot().thenCompose(cell -> {
                        var hold = cell.reservations().stream()
                                .filter(value -> value.resource().equals(resource.sha256()))
                                .findFirst();
                        if (hold.isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return nativeAuthority.readIntent().thenCompose(bound -> {
                            var original = bound.orElseThrow(
                                    () -> new IllegalStateException("Native BK held intent is absent"));
                            if (!hold.orElseThrow().nativeIntent().equals(Sha256Digest.hash(original.encode()))) {
                                throw new IllegalStateException("Native BK absence has another Cell hold");
                            }
                            return reconcileAbsence(route, budget, exact, original, ready)
                                    .thenAccept(result -> {
                                        if (result.reservationRetained()
                                                || result.deleteResult().outcome()
                                                        != com.nereusstream.storage.bookkeeper
                                                                .M5BookKeeperDeleteAdapterV1.DeleteOutcome
                                                                .AUTHORITATIVELY_ABSENT) {
                                            throw new IllegalStateException(
                                                    "Native BK absence still retains its original Cell hold");
                                        }
                                    });
                        });
                    });
                });
    }

    private CompletionStage<Void> requireDispatch(
            BoundPhysicalDeleteAuthorityRouteV2 route, VersionedValue exact, EpochGraceReady ready) {
        var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        if (!authority.target().resourceId().equals(resource)
                || authority.state() != TargetDeleteAuthorityStateV1.DELETE_INTENT_V1
                || authority.recoveryVeto().isPresent()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native BK lacks exact M5 INTENT"));
        }
        var mark = authority.epochGrace().orElseThrow();
        var snapshot = authority.eligibilitySnapshot().orElseThrow();
        snapshot.requireCanonicalM4ReleaseKeys();
        CompletionStage<Void> work = ready.requireAdmittedCurrent(resource, mark)
                .thenCompose(ignored -> route.requireActiveResource(resource))
                .thenCompose(ignored ->
                        requireCurrent(resource, M5TargetDeleteAuthorityStateMachineV1.dispatchContext(authority)))
                .thenCompose(ignored -> route.read(exact.key()).thenAccept(current -> {
                    if (!current.equals(Optional.of(exact))) {
                        throw new IllegalStateException("Native BK M5 INTENT changed");
                    }
                }));
        for (var fact : snapshot.authorityFacts()) {
            work = work.thenCompose(ignored -> metadata.read(fact.key()).thenAccept(current -> {
                if (current.isEmpty() || !fact(current.orElseThrow()).equals(fact)) {
                    throw new IllegalStateException("Native BK eligibility changed: " + fact.key());
                }
            }));
        }
        return work;
    }

    private static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }
}
