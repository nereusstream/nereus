/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.read.PhysicalObjectIdentityResolver;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Existing F4 object-protection protocol exposed through the provider-neutral source SPI. */
public final class ObjectMaterializationSourceProtectionAdapter
        implements MaterializationSourceProtectionAdapter<ObjectSliceReadTarget> {
    private final PhysicalObjectIdentityResolver identities;
    private final ObjectProtectionManager protections;

    public ObjectMaterializationSourceProtectionAdapter(
            PhysicalObjectIdentityResolver identities,
            ObjectProtectionManager protections) {
        this.identities = Objects.requireNonNull(identities, "identities");
        this.protections = Objects.requireNonNull(protections, "protections");
    }

    @Override
    public ReadTargetType targetType() {
        return ReadTargetType.OBJECT_SLICE;
    }

    @Override
    public Class<ObjectSliceReadTarget> targetClass() {
        return ObjectSliceReadTarget.class;
    }

    @Override
    public CompletableFuture<MaterializationSourceProtection> acquireOrTransfer(
            StreamId streamId,
            SourceGeneration source,
            String referenceId,
            ObjectProtectionOwner owner,
            OwnerRevalidator ownerRevalidator) {
        Objects.requireNonNull(streamId, "streamId");
        ObjectSliceReadTarget target = requireTarget(source);
        return identities.resolve(target, source.view())
                .thenCompose(identity -> protections.acquireOrTransfer(
                        new ObjectProtectionRequest(
                                identity,
                                ObjectProtectionType.MATERIALIZATION_SOURCE,
                                referenceId,
                                owner,
                                0),
                        ownerRevalidator::revalidate))
                .thenApply(value -> wrap(referenceId, value));
    }

    @Override
    public CompletableFuture<MaterializationSourceProtection> revalidate(
            MaterializationSourceProtection protection,
            OwnerRevalidator ownerRevalidator) {
        ObjectProtection exact = requireHandle(protection);
        return protections.revalidate(exact, ownerRevalidator::revalidate)
                .thenApply(value -> wrap(protection.referenceId(), value));
    }

    @Override
    public CompletableFuture<MaterializationSourceProtection> transfer(
            MaterializationSourceProtection protection,
            ObjectProtectionOwner newOwner,
            OwnerRevalidator newOwnerRevalidator) {
        ObjectProtection exact = requireHandle(protection);
        return protections.transfer(exact, newOwner, newOwnerRevalidator::revalidate)
                .thenApply(value -> wrap(protection.referenceId(), value));
    }

    @Override
    public CompletableFuture<Void> release(
            MaterializationSourceProtection protection,
            RemovalAuthorizer removalAuthorizer) {
        ObjectProtection exact = requireHandle(protection);
        return protections.release(exact, current -> removalAuthorizer.authorize(
                wrap(protection.referenceId(), current)));
    }

    private static ObjectSliceReadTarget requireTarget(SourceGeneration source) {
        if (!(source.readTarget() instanceof ObjectSliceReadTarget target)) {
            throw new IllegalArgumentException("object source adapter received another target type");
        }
        return target;
    }

    private static ObjectProtection requireHandle(MaterializationSourceProtection protection) {
        if (protection.targetType() != ReadTargetType.OBJECT_SLICE) {
            throw new IllegalArgumentException("object source adapter received another protection type");
        }
        ObjectProtection value = protection.requireProviderHandle(ObjectProtection.class);
        if (!value.identity().referenceId().equals(protection.referenceId())
                || !value.owner().equals(protection.owner())
                || value.metadataVersion() != protection.metadataVersion()) {
            throw new IllegalArgumentException("object source protection wrapper is inconsistent");
        }
        return value;
    }

    private static MaterializationSourceProtection wrap(
            String referenceId,
            ObjectProtection protection) {
        return new MaterializationSourceProtection(
                ReadTargetType.OBJECT_SLICE,
                referenceId,
                protection.owner(),
                protection.metadataVersion(),
                protection);
    }
}
