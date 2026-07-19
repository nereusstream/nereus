/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import java.util.Objects;

/** Opaque exact provider protection held by one materialization source edge. */
public record MaterializationSourceProtection(
        ReadTargetType targetType,
        String referenceId,
        ObjectProtectionOwner owner,
        long metadataVersion,
        Object providerHandle) {
    public MaterializationSourceProtection {
        Objects.requireNonNull(targetType, "targetType");
        referenceId = requireText(referenceId, "referenceId");
        Objects.requireNonNull(owner, "owner");
        if (metadataVersion < 0) {
            throw new IllegalArgumentException("metadataVersion must be non-negative");
        }
        Objects.requireNonNull(providerHandle, "providerHandle");
    }

    public <T> T requireProviderHandle(Class<T> type) {
        Objects.requireNonNull(type, "type");
        if (!type.isInstance(providerHandle)) {
            throw new IllegalArgumentException(
                    "materialization source protection handle does not match " + targetType);
        }
        return type.cast(providerHandle);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
