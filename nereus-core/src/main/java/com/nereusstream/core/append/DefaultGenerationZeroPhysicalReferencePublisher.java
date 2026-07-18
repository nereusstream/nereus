/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.PhysicalObjectIdentityResolver;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Default exact-owner/root handshake for generation-zero append references. */
public final class DefaultGenerationZeroPhysicalReferencePublisher
        implements GenerationZeroPhysicalReferencePublisher {
    private final String cluster;
    private final OxiaMetadataStore metadata;
    private final PhysicalObjectMetadataStore physicalStore;
    private final ObjectProtectionManager protections;
    private final PhysicalObjectIdentityResolver identityResolver;

    public DefaultGenerationZeroPhysicalReferencePublisher(
            String cluster,
            OxiaMetadataStore metadata,
            PhysicalObjectMetadataStore physicalStore,
            ObjectProtectionManager protections) {
        this.cluster = requireText(cluster, "cluster");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.physicalStore = Objects.requireNonNull(physicalStore, "physicalStore");
        this.protections = Objects.requireNonNull(protections, "protections");
        this.identityResolver = new MetadataPhysicalObjectIdentityResolver(
                cluster,
                metadata,
                physicalStore);
    }

    @Override
    public CompletableFuture<Void> authorizeUpload(
            AppendSession session,
            PhysicalObjectIdentity object,
            Duration timeout) {
        AppendSession expectedSession = Objects.requireNonNull(session, "session");
        PhysicalObjectIdentity expectedObject = Objects.requireNonNull(object, "object");
        AppendDeadline deadline = new AppendDeadline(timeout);
        return deadline.bound(
                        () -> metadata.revalidateAppendSession(cluster, expectedSession),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "revalidate Object WAL append session before provider PUT")
                .thenCompose(ignored -> deadline.bound(
                        () -> physicalStore.getRoot(
                                cluster, expectedObject.objectKeyHash()),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "load Object WAL physical root before provider PUT"))
                .thenApply(optional -> {
                    requireUploadRoot(expectedObject, optional);
                    return null;
                })
                .thenCompose(ignored -> deadline.bound(
                        () -> metadata.revalidateAppendSession(cluster, expectedSession),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "reprove Object WAL append session before provider PUT"));
    }

    @Override
    public CompletableFuture<ProtectedStableAppend> protectBeforeHead(
            PreparedStableAppend append,
            Duration timeout) {
        PreparedStableAppend exact = Objects.requireNonNull(append, "append");
        AppendDeadline deadline = new AppendDeadline(timeout);
        ObjectSliceReadTarget target = objectTarget(exact.request().readTarget());
        return deadline.bound(
                        () -> identityResolver.resolve(target, ReadView.COMMITTED),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "resolve Object WAL physical identity")
                .thenCompose(object -> protectPrepared(exact, object, deadline));
    }

    @Override
    public CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
            MaterializedGenerationZero append,
            Duration timeout) {
        MaterializedGenerationZero exact = Objects.requireNonNull(append, "append");
        AppendDeadline deadline = new AppendDeadline(timeout);
        ObjectSliceReadTarget target = objectTarget(exact.committedAppend().readTarget());
        return deadline.bound(
                        () -> identityResolver.resolve(target, ReadView.COMMITTED),
                        AppendOutcome.KNOWN_COMMITTED,
                        "resolve generation-zero physical identity")
                .thenCompose(object -> protectMaterialized(exact, object, deadline));
    }

    private CompletableFuture<ProtectedStableAppend> protectPrepared(
            PreparedStableAppend prepared,
            PhysicalObjectIdentity object,
            AppendDeadline deadline) {
        if (!object.objectKeyHash().equals(prepared.objectKeyHash())) {
            return failed(invariant(
                    "prepared append object hash conflicts with resolved whole-object identity",
                    AppendOutcome.KNOWN_NOT_COMMITTED));
        }
        ObjectProtectionOwner owner = new ObjectProtectionOwner(
                prepared.commitKey(),
                prepared.commitMetadataVersion(),
                prepared.commitRecordSha256());
        ObjectProtectionRequest request = new ObjectProtectionRequest(
                object,
                ObjectProtectionType.REACHABLE_APPEND,
                GenerationZeroProtectionIdentities.reachableAppendReferenceId(prepared),
                owner,
                0);
        ObjectProtectionManager.OwnerRevalidator revalidator = expected -> deadline.bound(
                () -> revalidatePreparedOwner(prepared, owner, expected),
                AppendOutcome.KNOWN_NOT_COMMITTED,
                "revalidate stable append intent owner");
        return deadline.bound(
                        () -> protections.acquire(request, revalidator),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "acquire REACHABLE_APPEND protection")
                .thenCompose(protection -> deadline.bound(
                        () -> protections.revalidate(protection, revalidator),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "revalidate REACHABLE_APPEND protection"))
                .thenCompose(protection -> loadExactActiveRoot(
                        object,
                        deadline,
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "reload Object WAL physical root").thenCompose(root -> deadline.bound(
                                () -> revalidatePreparedOwner(prepared, owner, protection.owner()),
                                AppendOutcome.KNOWN_NOT_COMMITTED,
                                "reload stable append intent owner")
                        .thenApply(ignored -> protectedStableAppend(prepared, object, protection, root))));
    }

    private CompletableFuture<ProtectedGenerationZero> protectMaterialized(
            MaterializedGenerationZero materialized,
            PhysicalObjectIdentity object,
            AppendDeadline deadline) {
        if (!object.objectKeyHash().equals(
                GenerationZeroProtectionIdentities.objectKeyHash(materialized.committedAppend()))) {
            return failed(invariant(
                    "generation-zero index conflicts with resolved whole-object identity",
                    AppendOutcome.KNOWN_COMMITTED));
        }
        ObjectProtectionOwner owner = new ObjectProtectionOwner(
                materialized.indexKey(),
                materialized.indexMetadataVersion(),
                materialized.indexRecordSha256());
        ObjectProtectionRequest request = new ObjectProtectionRequest(
                object,
                ObjectProtectionType.VISIBLE_GENERATION,
                GenerationZeroProtectionIdentities.visibleGenerationReferenceId(materialized),
                owner,
                0);
        ObjectProtectionManager.OwnerRevalidator revalidator = expected -> deadline.bound(
                () -> revalidateMaterializedOwner(materialized, owner, expected),
                AppendOutcome.KNOWN_COMMITTED,
                "revalidate generation-zero index owner");
        return deadline.bound(
                        () -> protections.acquire(request, revalidator),
                        AppendOutcome.KNOWN_COMMITTED,
                        "acquire generation-zero VISIBLE_GENERATION protection")
                .thenCompose(protection -> deadline.bound(
                        () -> protections.revalidate(protection, revalidator),
                        AppendOutcome.KNOWN_COMMITTED,
                        "revalidate generation-zero VISIBLE_GENERATION protection"))
                .thenCompose(protection -> loadExactActiveRoot(
                        object,
                        deadline,
                        AppendOutcome.KNOWN_COMMITTED,
                        "reload generation-zero physical root").thenCompose(root -> deadline.bound(
                                () -> revalidateMaterializedOwner(
                                        materialized,
                                        owner,
                                        protection.owner()),
                                AppendOutcome.KNOWN_COMMITTED,
                                "reload generation-zero index owner")
                        .thenApply(ignored -> protectedGenerationZero(materialized, protection, root))));
    }

    private CompletableFuture<Void> revalidatePreparedOwner(
            PreparedStableAppend expected,
            ObjectProtectionOwner canonicalOwner,
            ObjectProtectionOwner actualOwner) {
        if (!canonicalOwner.equals(actualOwner)) {
            return failed(invariant(
                    "REACHABLE_APPEND owner fields changed",
                    AppendOutcome.KNOWN_NOT_COMMITTED));
        }
        return metadata.prepareStableAppend(cluster, expected.request()).thenApply(actual -> {
            if (!samePreparedIdentity(expected, actual)) {
                throw invariant(
                        "stable append intent changed during physical protection",
                        AppendOutcome.KNOWN_NOT_COMMITTED);
            }
            return null;
        });
    }

    private CompletableFuture<Void> revalidateMaterializedOwner(
            MaterializedGenerationZero materialized,
            ObjectProtectionOwner canonicalOwner,
            ObjectProtectionOwner actualOwner) {
        if (!canonicalOwner.equals(actualOwner)) {
            return failed(invariant(
                    "generation-zero VISIBLE_GENERATION owner fields changed",
                    AppendOutcome.KNOWN_COMMITTED));
        }
        return metadata.revalidateMaterializedGenerationZero(cluster, materialized);
    }

    private CompletableFuture<VersionedPhysicalObjectRoot> loadExactActiveRoot(
            PhysicalObjectIdentity object,
            AppendDeadline deadline,
            AppendOutcome outcome,
            String operation) {
        return deadline.bound(
                        () -> physicalStore.getRoot(cluster, object.objectKeyHash()),
                        outcome,
                        operation)
                .thenApply(optional -> requireExactActiveRoot(object, optional, outcome));
    }

    private static VersionedPhysicalObjectRoot requireExactActiveRoot(
            PhysicalObjectIdentity object,
            Optional<VersionedPhysicalObjectRoot> optional,
            AppendOutcome outcome) {
        VersionedPhysicalObjectRoot root = optional.orElseThrow(() -> invariant(
                "physical object root is absent after protection",
                outcome));
        if (root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                || !PhysicalObjectIdentity.from(root.value()).equals(object)) {
            throw invariant("physical object root changed after protection", outcome);
        }
        return root;
    }

    private static void requireUploadRoot(
            PhysicalObjectIdentity object,
            Optional<VersionedPhysicalObjectRoot> optional) {
        if (optional.isEmpty()) {
            return;
        }
        VersionedPhysicalObjectRoot root = optional.orElseThrow();
        if (root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                || !PhysicalObjectIdentity.from(root.value()).equals(object)) {
            throw new NereusException(
                    ErrorCode.FENCED_APPEND,
                    false,
                    "Object WAL provider PUT is fenced by a non-active or different physical root",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
    }

    private static ProtectedStableAppend protectedStableAppend(
            PreparedStableAppend prepared,
            PhysicalObjectIdentity object,
            ObjectProtection protection,
            VersionedPhysicalObjectRoot root) {
        if (root.value().lifecycleEpoch() != protection.rootLifecycleEpoch()) {
            throw invariant(
                    "REACHABLE_APPEND protection root epoch changed",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        return new ProtectedStableAppend(
                prepared,
                object,
                protection.identity(),
                root.metadataVersion(),
                root.value().lifecycleEpoch(),
                protection.metadataVersion(),
                protection.durableValueSha256());
    }

    private static ProtectedGenerationZero protectedGenerationZero(
            MaterializedGenerationZero materialized,
            ObjectProtection protection,
            VersionedPhysicalObjectRoot root) {
        if (root.value().lifecycleEpoch() != protection.rootLifecycleEpoch()) {
            throw invariant(
                    "generation-zero VISIBLE_GENERATION root epoch changed",
                    AppendOutcome.KNOWN_COMMITTED);
        }
        return new ProtectedGenerationZero(
                materialized,
                protection.identity(),
                root.metadataVersion(),
                root.value().lifecycleEpoch(),
                protection.metadataVersion(),
                protection.durableValueSha256());
    }

    private static boolean samePreparedIdentity(
            PreparedStableAppend left,
            PreparedStableAppend right) {
        return left.request().equals(right.request())
                && left.commitId().equals(right.commitId())
                && left.commitKey().equals(right.commitKey())
                && left.commitMetadataVersion() == right.commitMetadataVersion()
                && left.commitRecordSha256().equals(right.commitRecordSha256())
                && left.objectKeyHash().equals(right.objectKeyHash());
    }

    private static ObjectSliceReadTarget objectTarget(Object target) {
        if (!(target instanceof ObjectSliceReadTarget objectSlice)) {
            throw new IllegalArgumentException("generation-zero append target must be an object slice");
        }
        return objectSlice;
    }

    private static NereusException invariant(String message, AppendOutcome outcome) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                outcome);
    }

    private static <T> CompletableFuture<T> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
