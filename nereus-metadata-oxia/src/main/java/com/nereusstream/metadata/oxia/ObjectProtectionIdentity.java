/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Exact protection key identity shared by the metadata store and core protection manager. */
public record ObjectProtectionIdentity(
        ObjectKeyHash object,
        ObjectProtectionType type,
        String referenceId) {
    public ObjectProtectionIdentity {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(referenceId, "referenceId");
        if (referenceId.isBlank() || referenceId.getBytes(StandardCharsets.UTF_8).length > 256) {
            throw new IllegalArgumentException("referenceId must be non-blank and at most 256 UTF-8 bytes");
        }
    }
}
