/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import java.util.Objects;

/** Exact durable protection version returned only after its root and owner post-checks. */
public record ObjectProtection(
        PhysicalObjectIdentity object,
        ObjectProtectionIdentity identity,
        ObjectProtectionOwner owner,
        long rootLifecycleEpoch,
        long createdAtMillis,
        long expiresAtMillis,
        long metadataVersion,
        Checksum durableValueSha256) {
    public ObjectProtection {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(identity, "identity");
        if (!identity.object().equals(object.objectKeyHash())) {
            throw new IllegalArgumentException("protection identity does not match physical object");
        }
        Objects.requireNonNull(owner, "owner");
        if (rootLifecycleEpoch <= 0 || createdAtMillis < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("protection epoch/version/times are invalid");
        }
        boolean pending = ObjectProtectionRequest.isPending(identity.type());
        if (pending != (expiresAtMillis > createdAtMillis)) {
            throw new IllegalArgumentException("protection expiry does not match its type");
        }
        durableValueSha256 = GcReferenceQuery.requireSha256(
                durableValueSha256, "durableValueSha256");
    }

    public boolean isPending() {
        return ObjectProtectionRequest.isPending(identity.type());
    }
}
