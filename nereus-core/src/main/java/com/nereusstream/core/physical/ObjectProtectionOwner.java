/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.Checksum;
import java.util.Objects;

/** Exact authoritative metadata owner that can re-prove one physical-object reference. */
public record ObjectProtectionOwner(
        String ownerKey,
        long metadataVersion,
        Checksum identitySha256) {
    public ObjectProtectionOwner {
        Objects.requireNonNull(ownerKey, "ownerKey");
        if (ownerKey.isBlank()) {
            throw new IllegalArgumentException("ownerKey cannot be blank");
        }
        if (metadataVersion < 0) {
            throw new IllegalArgumentException("metadataVersion must be non-negative");
        }
        identitySha256 = GcReferenceQuery.requireSha256(identitySha256, "identitySha256");
    }
}
