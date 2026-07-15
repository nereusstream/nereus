/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.ObjectKey;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public record ListedObject(
        ObjectKey key,
        long objectLength,
        Optional<String> etag,
        Optional<Instant> lastModified) {
    public ListedObject {
        Objects.requireNonNull(key, "key");
        if (objectLength < 0) {
            throw new IllegalArgumentException("objectLength must be non-negative");
        }
        etag = Objects.requireNonNull(etag, "etag").map(value -> requireText(value, "etag"));
        lastModified = Objects.requireNonNull(lastModified, "lastModified");
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }
}
