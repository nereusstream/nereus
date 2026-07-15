/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.Checksum;
import java.util.Objects;

public record GcAuthorityToken(
        String authorityKey,
        long metadataVersion,
        Checksum identitySha256) {
    public GcAuthorityToken {
        authorityKey = requireText(authorityKey, "authorityKey");
        if (metadataVersion < 0) {
            throw new IllegalArgumentException("metadataVersion must be non-negative");
        }
        identitySha256 = GcReferenceQuery.requireSha256(identitySha256, "identitySha256");
    }

    static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.length() > 4096) {
            throw new IllegalArgumentException(name + " must be non-blank and bounded");
        }
        return value;
    }
}
