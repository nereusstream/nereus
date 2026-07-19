/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Immutable exact-target registry for materialization source protection providers. */
public final class MaterializationSourceProtectionRegistry {
    private final Map<ReadTargetType, MaterializationSourceProtectionAdapter<?>> adapters;

    public MaterializationSourceProtectionRegistry(
            Collection<? extends MaterializationSourceProtectionAdapter<?>> adapters) {
        Objects.requireNonNull(adapters, "adapters");
        EnumMap<ReadTargetType, MaterializationSourceProtectionAdapter<?>> values =
                new EnumMap<>(ReadTargetType.class);
        for (MaterializationSourceProtectionAdapter<?> adapter : adapters) {
            MaterializationSourceProtectionAdapter<?> exact = Objects.requireNonNull(
                    adapter, "adapter");
            if (values.putIfAbsent(exact.targetType(), exact) != null) {
                throw new IllegalArgumentException(
                        "duplicate materialization source protection adapter for " + exact.targetType());
            }
        }
        this.adapters = Map.copyOf(values);
    }

    public CompletableFuture<MaterializationSourceProtection> acquireOrTransfer(
            StreamId streamId,
            SourceGeneration source,
            String referenceId,
            ObjectProtectionOwner owner,
            MaterializationSourceProtectionAdapter.OwnerRevalidator ownerRevalidator) {
        SourceGeneration exact = Objects.requireNonNull(source, "source");
        return require(exact.readTarget()).acquireOrTransfer(
                Objects.requireNonNull(streamId, "streamId"),
                exact,
                referenceId,
                Objects.requireNonNull(owner, "owner"),
                Objects.requireNonNull(ownerRevalidator, "ownerRevalidator"));
    }

    public CompletableFuture<MaterializationSourceProtection> revalidate(
            MaterializationSourceProtection protection,
            MaterializationSourceProtectionAdapter.OwnerRevalidator ownerRevalidator) {
        MaterializationSourceProtection exact = Objects.requireNonNull(protection, "protection");
        return require(exact.targetType()).revalidate(
                exact, Objects.requireNonNull(ownerRevalidator, "ownerRevalidator"));
    }

    public CompletableFuture<MaterializationSourceProtection> transfer(
            MaterializationSourceProtection protection,
            ObjectProtectionOwner newOwner,
            MaterializationSourceProtectionAdapter.OwnerRevalidator newOwnerRevalidator) {
        MaterializationSourceProtection exact = Objects.requireNonNull(protection, "protection");
        return require(exact.targetType()).transfer(
                exact,
                Objects.requireNonNull(newOwner, "newOwner"),
                Objects.requireNonNull(newOwnerRevalidator, "newOwnerRevalidator"));
    }

    public CompletableFuture<Void> release(
            MaterializationSourceProtection protection,
            MaterializationSourceProtectionAdapter.RemovalAuthorizer removalAuthorizer) {
        MaterializationSourceProtection exact = Objects.requireNonNull(protection, "protection");
        return require(exact.targetType()).release(
                exact, Objects.requireNonNull(removalAuthorizer, "removalAuthorizer"));
    }

    public boolean supports(ReadTargetType targetType) {
        return adapters.containsKey(Objects.requireNonNull(targetType, "targetType"));
    }

    private MaterializationSourceProtectionAdapter<?> require(ReadTargetType targetType) {
        MaterializationSourceProtectionAdapter<?> adapter = adapters.get(targetType);
        if (adapter == null) {
            throw unsupported(targetType);
        }
        return adapter;
    }

    @SuppressWarnings("unchecked")
    private <T extends ReadTarget> MaterializationSourceProtectionAdapter<T> require(T target) {
        Objects.requireNonNull(target, "target");
        MaterializationSourceProtectionAdapter<?> adapter = require(target.type());
        if (!adapter.targetClass().equals(target.getClass())) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "materialization source protection target class does not match " + target.type());
        }
        return (MaterializationSourceProtectionAdapter<T>) adapter;
    }

    private static NereusException unsupported(ReadTargetType type) {
        return new NereusException(
                ErrorCode.UNSUPPORTED_READ_TARGET,
                false,
                "no materialization source protection adapter is installed for " + type);
    }
}
