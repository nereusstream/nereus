/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record ListObjectsResult(
        ObjectKeyPrefix prefix,
        List<ListedObject> objects,
        Optional<String> continuationToken) {
    public ListObjectsResult {
        Objects.requireNonNull(prefix, "prefix");
        objects = List.copyOf(Objects.requireNonNull(objects, "objects"));
        if (objects.size() > 1_000 || objects.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("objects exceeds the provider page bound or contains null");
        }
        String previous = null;
        for (ListedObject object : objects) {
            if (!object.key().value().startsWith(prefix.value())
                    || (previous != null && previous.compareTo(object.key().value()) >= 0)) {
                throw new IllegalArgumentException("listed objects must match prefix and be strictly ordered");
            }
            previous = object.key().value();
        }
        continuationToken = Objects.requireNonNull(continuationToken, "continuationToken")
                .map(value -> {
                    if (value.isBlank()) {
                        throw new IllegalArgumentException("continuationToken cannot be blank");
                    }
                    return value;
                });
        if (objects.isEmpty() && continuationToken.isPresent()) {
            throw new IllegalArgumentException("empty list page cannot carry a continuation");
        }
    }
}
