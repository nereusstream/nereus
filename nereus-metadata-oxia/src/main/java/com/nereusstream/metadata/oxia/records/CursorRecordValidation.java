/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class CursorRecordValidation {
    static final int POSITION_PROPERTIES_MAX_BYTES = 8 * 1024;
    static final int CURSOR_PROPERTIES_MAX_BYTES = 16 * 1024;
    static final int TRIM_REASON_MAX_BYTES = 1024;

    private static final Comparator<String> UNSIGNED_UTF8 = CursorRecordValidation::compareUtf8;

    private CursorRecordValidation() {
    }

    static String requireString(String value, String fieldName, int maxUtf8Bytes, boolean allowBlank) {
        Objects.requireNonNull(value, fieldName);
        if (!allowBlank && value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        if (value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(fieldName + " cannot contain NUL");
        }
        byte[] bytes = strictUtf8(value, fieldName);
        if (bytes.length > maxUtf8Bytes) {
            throw new IllegalArgumentException(fieldName + " exceeds its UTF-8 byte limit");
        }
        return value;
    }

    static Map<String, Long> canonicalLongMap(Map<String, Long> value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        List<Map.Entry<String, Long>> entries = new ArrayList<>(value.entrySet());
        entries.sort(Map.Entry.comparingByKey(UNSIGNED_UTF8));
        long bytes = 0;
        LinkedHashMap<String, Long> result = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : entries) {
            String key = requireString(entry.getKey(), fieldName + " key", 64 * 1024, true);
            Long item = Objects.requireNonNull(entry.getValue(), fieldName + " contains null value");
            bytes = Math.addExact(bytes, Integer.BYTES + strictUtf8(key, fieldName + " key").length + Long.BYTES);
            if (bytes > POSITION_PROPERTIES_MAX_BYTES) {
                throw new IllegalArgumentException(fieldName + " exceeds the encoded byte limit");
            }
            result.put(key, item);
        }
        return Collections.unmodifiableMap(result);
    }

    static Map<String, String> canonicalStringMap(Map<String, String> value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        List<Map.Entry<String, String>> entries = new ArrayList<>(value.entrySet());
        entries.sort(Map.Entry.comparingByKey(UNSIGNED_UTF8));
        long bytes = 0;
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : entries) {
            String key = requireString(entry.getKey(), fieldName + " key", 64 * 1024, true);
            String item = requireString(entry.getValue(), fieldName + " value", 64 * 1024, true);
            bytes = Math.addExact(bytes,
                    Integer.BYTES + strictUtf8(key, fieldName + " key").length
                            + Integer.BYTES + strictUtf8(item, fieldName + " value").length);
            if (bytes > CURSOR_PROPERTIES_MAX_BYTES) {
                throw new IllegalArgumentException(fieldName + " exceeds the encoded byte limit");
            }
            result.put(key, item);
        }
        return Collections.unmodifiableMap(result);
    }

    static List<CursorAckRangeRecord> canonicalRanges(
            List<CursorAckRangeRecord> ranges, long markDeleteOffset) {
        List<CursorAckRangeRecord> copy = List.copyOf(Objects.requireNonNull(ranges, "ranges"));
        long previousEnd = -1;
        for (CursorAckRangeRecord range : copy) {
            Objects.requireNonNull(range, "ranges contains null");
            if (range.startOffset() < markDeleteOffset) {
                throw new IllegalArgumentException("cursor ack range starts below markDeleteOffset");
            }
            if (previousEnd >= 0 && range.startOffset() <= previousEnd) {
                throw new IllegalArgumentException("cursor ack ranges must be sorted, disjoint, and non-adjacent");
            }
            previousEnd = range.endOffset();
        }
        return copy;
    }

    static List<CursorPartialBatchAckRecord> canonicalPartials(
            List<CursorPartialBatchAckRecord> partials,
            long markDeleteOffset,
            List<CursorAckRangeRecord> ranges) {
        List<CursorPartialBatchAckRecord> copy = List.copyOf(
                Objects.requireNonNull(partials, "partials"));
        long previousOffset = -1;
        int rangeIndex = 0;
        for (CursorPartialBatchAckRecord partial : copy) {
            Objects.requireNonNull(partial, "partials contains null");
            if (partial.entryOffset() < markDeleteOffset || partial.entryOffset() <= previousOffset) {
                throw new IllegalArgumentException("partial ack offsets must be unique, sorted, and not below mark-delete");
            }
            while (rangeIndex < ranges.size()
                    && ranges.get(rangeIndex).endOffset() <= partial.entryOffset()) {
                rangeIndex++;
            }
            if (rangeIndex < ranges.size()) {
                CursorAckRangeRecord range = ranges.get(rangeIndex);
                if (range.startOffset() <= partial.entryOffset()) {
                    throw new IllegalArgumentException("partial ack offset is covered by a whole ack range");
                }
            }
            previousOffset = partial.entryOffset();
        }
        return copy;
    }

    private static byte[] strictUtf8(String value, String fieldName) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(fieldName + " must be valid UTF-8", e);
        }
    }

    private static int compareUtf8(String left, String right) {
        byte[] leftBytes = strictUtf8(Objects.requireNonNull(left, "map key"), "map key");
        byte[] rightBytes = strictUtf8(Objects.requireNonNull(right, "map key"), "map key");
        int limit = Math.min(leftBytes.length, rightBytes.length);
        for (int index = 0; index < limit; index++) {
            int comparison = Integer.compare(leftBytes[index] & 0xff, rightBytes[index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }
}
