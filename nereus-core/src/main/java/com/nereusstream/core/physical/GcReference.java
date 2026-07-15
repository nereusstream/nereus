/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.Checksum;

public record GcReference(
        String referenceType,
        String referenceId,
        String ownerKey,
        long ownerMetadataVersion,
        Checksum ownerIdentitySha256) {
    public GcReference {
        referenceType = GcAuthorityToken.requireText(referenceType, "referenceType");
        referenceId = GcAuthorityToken.requireText(referenceId, "referenceId");
        ownerKey = GcAuthorityToken.requireText(ownerKey, "ownerKey");
        if (ownerMetadataVersion < 0) {
            throw new IllegalArgumentException("ownerMetadataVersion must be non-negative");
        }
        ownerIdentitySha256 = GcReferenceQuery.requireSha256(
                ownerIdentitySha256, "ownerIdentitySha256");
    }
}
