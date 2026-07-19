/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.SchemaRef;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

final class BookKeeperRecordValidation {
    static final int VERSION = 1;
    static final int MAX_TEXT_BYTES = 4 * 1024;

    private BookKeeperRecordValidation() { }

    static void version(int value) {
        if (value != VERSION) {
            throw new IllegalArgumentException("schemaVersion must be 1");
        }
    }

    static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(name + " must be non-blank and bounded");
        }
        return value;
    }

    static String optional(String value, String name) {
        Objects.requireNonNull(value, name);
        if ((!value.isEmpty() && value.isBlank()) || value.getBytes(StandardCharsets.UTF_8).length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(name + " must be empty or non-blank and bounded");
        }
        return value;
    }

    static String sha256(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.length() != 64) {
            throw new IllegalArgumentException(name + " must be lowercase SHA-256 hex");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f'))) {
                throw new IllegalArgumentException(name + " must be lowercase SHA-256 hex");
            }
        }
        return value;
    }

    static String optionalSha256(String value, String name) {
        Objects.requireNonNull(value, name);
        return value.isEmpty() ? value : sha256(value, name);
    }

    static long nonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    static int nonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    static long positive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    static int positive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    static long metadataVersion(long value) {
        return nonNegative(value, "metadataVersion");
    }

    static List<SchemaRef> schemaRefs(List<SchemaRef> values) {
        Objects.requireNonNull(values, "schemaRefs");
        if (values.size() > 4_096 || values.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("schemaRefs exceed their bound or contain null");
        }
        return List.copyOf(values);
    }

    static void times(long created, long updated) {
        nonNegative(created, "createdAtMillis");
        if (updated < created) {
            throw new IllegalArgumentException("updatedAtMillis cannot precede creation");
        }
    }
}
