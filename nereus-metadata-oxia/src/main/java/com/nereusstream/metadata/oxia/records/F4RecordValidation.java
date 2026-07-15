/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.SchemaRef;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

final class F4RecordValidation {
    static final int SCHEMA_VERSION = 1;
    static final int MAX_REASON_BYTES = 4 * 1024;
    static final int MAX_REFERENCE_ID_BYTES = 256;
    static final int MAX_TASK_SOURCES = 128;
    static final int MAX_RECOVERY_REFERENCES = 32;

    private F4RecordValidation() {
    }

    static void requireSchemaVersion(int schemaVersion) {
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("schemaVersion must be 1");
        }
    }

    static long requireMetadataVersion(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("metadataVersion must be non-negative");
        }
        return value;
    }

    static long requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    static long requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    static int requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    static String requireText(String value, String name) {
        return requireText(value, name, Integer.MAX_VALUE, false);
    }

    static String requireOptionalText(String value, String name, int maxBytes) {
        return requireText(value, name, maxBytes, true);
    }

    static String requireText(String value, String name, int maxBytes, boolean allowEmpty) {
        Objects.requireNonNull(value, name);
        if ((!allowEmpty && value.isBlank()) || (allowEmpty && !value.isEmpty() && value.isBlank())) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw new IllegalArgumentException(name + " exceeds its UTF-8 byte bound");
        }
        return value;
    }

    static String requireSha256(String value, String name) {
        return requireLowerHex(value, name, 64);
    }

    static String requireOptionalSha256(String value, String name) {
        Objects.requireNonNull(value, name);
        return value.isEmpty() ? value : requireSha256(value, name);
    }

    static String requireCrc32c(String value, String name) {
        return requireLowerHex(value, name, 8);
    }

    static String requireBase32Id(String value, String name) {
        requireText(value, name, 128, false);
        if (value.length() < 26) {
            throw new IllegalArgumentException(name + " must encode at least 128 bits");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z') || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException(name + " must be lowercase base32 without padding");
            }
        }
        return value;
    }

    static List<SchemaRef> canonicalSchemaRefs(List<SchemaRef> schemaRefs) {
        return MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
    }

    static <T> List<T> immutableBoundedList(List<T> values, int maximum, String name) {
        Objects.requireNonNull(values, name);
        if (values.size() > maximum || values.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(name + " exceeds its count bound or contains null");
        }
        return List.copyOf(values);
    }

    static void requireRange(long start, long end, String name) {
        if (start < 0 || end <= start) {
            throw new IllegalArgumentException(name + " must be a non-empty non-negative range");
        }
    }

    static void requireDenseRange(long start, long end, int count, String name) {
        requireRange(start, end, name);
        if (Math.subtractExact(end, start) != count) {
            throw new IllegalArgumentException(name + " count must equal its offset span");
        }
    }

    private static String requireLowerHex(String value, String name, int length) {
        Objects.requireNonNull(value, name);
        if (value.length() != length) {
            throw new IllegalArgumentException(name + " has an invalid length");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f'))) {
                throw new IllegalArgumentException(name + " must be lowercase hexadecimal");
            }
        }
        return value;
    }
}
