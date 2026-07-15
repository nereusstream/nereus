/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.util.Objects;

/** Stable caller-owned identity and owner fields for an idempotent protection acquisition. */
public record ObjectProtectionRequest(
        PhysicalObjectIdentity object,
        ObjectProtectionType type,
        String referenceId,
        ObjectProtectionOwner owner,
        long expiresAtMillis) {
    public ObjectProtectionRequest {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(type, "type");
        new ObjectProtectionIdentity(object.objectKeyHash(), type, referenceId);
        Objects.requireNonNull(owner, "owner");
        if (expiresAtMillis < 0) {
            throw new IllegalArgumentException("expiresAtMillis must be non-negative");
        }
        boolean pending = isPending(type);
        if (pending != (expiresAtMillis > 0)) {
            throw new IllegalArgumentException("only pending protection types may have an expiry");
        }
    }

    public ObjectProtectionIdentity identity() {
        return new ObjectProtectionIdentity(object.objectKeyHash(), type, referenceId);
    }

    public boolean isPending() {
        return isPending(type);
    }

    static boolean isPending(ObjectProtectionType type) {
        return type == ObjectProtectionType.CURSOR_SNAPSHOT_PENDING
                || type == ObjectProtectionType.RECOVERY_CHECKPOINT_PENDING;
    }
}
