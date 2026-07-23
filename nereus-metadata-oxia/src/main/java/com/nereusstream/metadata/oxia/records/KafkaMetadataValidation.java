/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

final class KafkaMetadataValidation {
    static final int SHA256_BYTES = 32;

    private KafkaMetadataValidation() { }

    static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > 65_535) {
            throw new IllegalArgumentException(name + " must be nonblank and bounded");
        }
        return value;
    }

    static String bounded(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.getBytes(StandardCharsets.UTF_8).length > 65_535) {
            throw new IllegalArgumentException(name + " is too large");
        }
        return value;
    }

    static byte[] sha256(byte[] value, String name, boolean allowEmpty) {
        byte[] exact = Objects.requireNonNull(value, name).clone();
        if ((!allowEmpty || exact.length != 0) && exact.length != SHA256_BYTES) {
            throw new IllegalArgumentException(name + " must contain exactly 32 bytes");
        }
        return exact;
    }

    static int bytesHash(byte[] value) {
        return Arrays.hashCode(value);
    }

    static boolean bytesEqual(byte[] left, byte[] right) {
        return Arrays.equals(left, right);
    }

    static <T> List<T> list(List<T> values, int max, String name) {
        List<T> exact = List.copyOf(Objects.requireNonNull(values, name));
        if (exact.size() > max) {
            throw new IllegalArgumentException(name + " exceeds its bound");
        }
        return exact;
    }
}
