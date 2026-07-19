/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Exact stored bytes/version wrapper used by allocation and reservation workflow rows. */
public record BookKeeperVersionedValue<T>(String key, T value, long metadataVersion, Checksum durableValueSha256) {
    public BookKeeperVersionedValue {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()) throw new IllegalArgumentException("key cannot be blank");
        Objects.requireNonNull(value, "value");
        if (metadataVersion < 0) throw new IllegalArgumentException("metadataVersion must be non-negative");
        Objects.requireNonNull(durableValueSha256, "durableValueSha256");
        if (durableValueSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("durableValueSha256 must use SHA256");
        }
    }
}
