/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/** Immutable exact-type registry for provider physical-reference adapters. */
public final class PrimaryPhysicalReferenceAdapterRegistry {
    private final Map<ReadTargetType, PrimaryPhysicalReferenceAdapter<?>> adapters;

    public PrimaryPhysicalReferenceAdapterRegistry(
            Collection<? extends PrimaryPhysicalReferenceAdapter<?>> adapters) {
        Objects.requireNonNull(adapters, "adapters");
        EnumMap<ReadTargetType, PrimaryPhysicalReferenceAdapter<?>> values =
                new EnumMap<>(ReadTargetType.class);
        for (PrimaryPhysicalReferenceAdapter<?> adapter : adapters) {
            PrimaryPhysicalReferenceAdapter<?> exact = Objects.requireNonNull(adapter, "adapter");
            PrimaryPhysicalReferenceAdapter<?> previous = values.putIfAbsent(exact.targetType(), exact);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "duplicate physical-reference adapter for " + exact.targetType());
            }
        }
        this.adapters = Map.copyOf(values);
    }

    public boolean supports(ReadTargetType targetType) {
        return adapters.containsKey(Objects.requireNonNull(targetType, "targetType"));
    }

    public <T extends ReadTarget> PrimaryPhysicalReferenceAdapter<T> require(T target) {
        Objects.requireNonNull(target, "target");
        PrimaryPhysicalReferenceAdapter<?> adapter = adapters.get(target.type());
        if (adapter == null) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_READ_TARGET,
                    false,
                    "no primary physical-reference adapter is installed for " + target.type());
        }
        if (!adapter.targetClass().equals(target.getClass())) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "primary physical-reference adapter target class does not match " + target.type());
        }
        @SuppressWarnings("unchecked")
        PrimaryPhysicalReferenceAdapter<T> typed = (PrimaryPhysicalReferenceAdapter<T>) adapter;
        return typed;
    }
}
