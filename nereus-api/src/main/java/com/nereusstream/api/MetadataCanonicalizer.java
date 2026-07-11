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

package com.nereusstream.api;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Canonical ordering and size helpers for API metadata copied into durable records. */
public final class MetadataCanonicalizer {
    private static final Comparator<SchemaRef> SCHEMA_REF_COMPARATOR =
            Comparator.comparing(SchemaRef::namespace, MetadataCanonicalizer::compareUtf8)
                    .thenComparing(SchemaRef::id, MetadataCanonicalizer::compareUtf8)
                    .thenComparingLong(SchemaRef::version);

    private MetadataCanonicalizer() {
    }

    public static Map<String, String> canonicalStringMap(
            Map<String, String> value,
            int maxEncodedBytes,
            String fieldName) {
        Objects.requireNonNull(value, fieldName);
        requirePositiveLimit(maxEncodedBytes, fieldName);

        List<Map.Entry<String, String>> entries = new ArrayList<>(value.entrySet());
        encodedStringMapBytes(entries, maxEncodedBytes, fieldName);
        entries.sort(Map.Entry.comparingByKey(MetadataCanonicalizer::compareUtf8));

        LinkedHashMap<String, String> sorted = new LinkedHashMap<>(entries.size());
        for (Map.Entry<String, String> entry : entries) {
            sorted.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(sorted);
    }

    public static int encodedStringMapBytes(Map<String, String> value) {
        Objects.requireNonNull(value, "value");
        return encodedStringMapBytes(new ArrayList<>(value.entrySet()), Integer.MAX_VALUE, "value");
    }

    public static List<SchemaRef> canonicalSchemaRefs(List<SchemaRef> schemaRefs) {
        Objects.requireNonNull(schemaRefs, "schemaRefs");
        List<SchemaRef> sorted = new ArrayList<>(schemaRefs.size());
        for (SchemaRef schemaRef : schemaRefs) {
            sorted.add(Objects.requireNonNull(schemaRef, "schemaRefs contains null"));
        }
        sorted.sort(SCHEMA_REF_COMPARATOR);

        SchemaRef previous = null;
        for (SchemaRef schemaRef : sorted) {
            if (schemaRef.equals(previous)) {
                throw new IllegalArgumentException("schemaRefs contains duplicate tuple: " + schemaRef);
            }
            previous = schemaRef;
        }
        int encodedBytes = encodedSchemaRefsBytes(sorted);
        if (encodedBytes > ApiLimits.MAX_SCHEMA_REFS_ENCODED_BYTES) {
            throw new IllegalArgumentException("schemaRefs encoded size exceeds "
                    + ApiLimits.MAX_SCHEMA_REFS_ENCODED_BYTES + " bytes");
        }
        return List.copyOf(sorted);
    }

    public static int encodedSchemaRefsBytes(List<SchemaRef> schemaRefs) {
        Objects.requireNonNull(schemaRefs, "schemaRefs");
        long encodedBytes = 0;
        for (SchemaRef schemaRef : schemaRefs) {
            Objects.requireNonNull(schemaRef, "schemaRefs contains null");
            encodedBytes += Integer.BYTES + utf8Length(schemaRef.namespace());
            encodedBytes += Integer.BYTES + utf8Length(schemaRef.id());
            encodedBytes += Long.BYTES;
            if (encodedBytes > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("schemaRefs encoded size exceeds integer range");
            }
        }
        return (int) encodedBytes;
    }

    private static int encodedStringMapBytes(
            List<Map.Entry<String, String>> entries,
            int maxEncodedBytes,
            String fieldName) {
        long encodedBytes = 0;
        for (Map.Entry<String, String> entry : entries) {
            String key = Objects.requireNonNull(entry.getKey(), fieldName + " contains null key");
            String value = Objects.requireNonNull(entry.getValue(), fieldName + " contains null value");
            encodedBytes += Integer.BYTES + utf8Length(key);
            encodedBytes += Integer.BYTES + utf8Length(value);
            if (encodedBytes > maxEncodedBytes) {
                throw new IllegalArgumentException(fieldName + " encoded size exceeds "
                        + maxEncodedBytes + " bytes");
            }
        }
        return (int) encodedBytes;
    }

    private static int compareUtf8(String left, String right) {
        byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
        int limit = Math.min(leftBytes.length, rightBytes.length);
        for (int i = 0; i < limit; i++) {
            int leftByte = leftBytes[i] & 0xff;
            int rightByte = rightBytes[i] & 0xff;
            if (leftByte != rightByte) {
                return Integer.compare(leftByte, rightByte);
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static void requirePositiveLimit(int maxEncodedBytes, String fieldName) {
        if (maxEncodedBytes <= 0) {
            throw new IllegalArgumentException(fieldName + " max encoded bytes must be positive");
        }
    }
}
