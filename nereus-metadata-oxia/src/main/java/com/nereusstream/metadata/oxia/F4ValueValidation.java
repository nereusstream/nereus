/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

final class F4ValueValidation {
    private F4ValueValidation() {
    }

    static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    static long version(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("metadataVersion must be non-negative");
        }
        return value;
    }

    static Checksum sha256(Checksum value, String name) {
        Objects.requireNonNull(value, name);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return value;
    }

    static <T> List<T> orderedPage(
            List<T> values, Function<T, String> key, int maximum, String name) {
        Objects.requireNonNull(values, name);
        if (values.size() > maximum || values.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(name + " exceeds its bound or contains null");
        }
        String previous = null;
        for (T value : values) {
            String current = text(key.apply(value), name + " key");
            if (previous != null && previous.compareTo(current) >= 0) {
                throw new IllegalArgumentException(name + " must be strictly key ordered");
            }
            previous = current;
        }
        return List.copyOf(values);
    }
}
