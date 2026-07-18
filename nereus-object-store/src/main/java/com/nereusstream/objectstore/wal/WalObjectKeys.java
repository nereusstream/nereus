/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.wal;

import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/** Canonical Phase 1 WAL object-key builder and strict inverse used by F4 inventory. */
public final class WalObjectKeys {
    private static final DateTimeFormatter OBJECT_TIME =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final DateTimeFormatter YEAR = DateTimeFormatter.ofPattern("yyyy");
    private static final DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("MM");
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("dd");

    private WalObjectKeys() {
    }

    public static ObjectKeyPrefix prefix(String cluster) {
        return new ObjectKeyPrefix(KeyComponentCodec.encodeComponent(requireText(cluster, "cluster"))
                + "/wal/");
    }

    public static ObjectKey objectKey(
            String cluster,
            String writerIdHash,
            String writerRunIdHash,
            ObjectId objectId,
            Instant createdAt) {
        String writer = requireStableHash(writerIdHash, "writerIdHash");
        String run = requireCanonicalComponent(writerRunIdHash, "writerRunIdHash");
        Objects.requireNonNull(objectId, "objectId");
        Instant exactCreatedAt = Objects.requireNonNull(createdAt, "createdAt");
        String year = YEAR.withZone(ZoneOffset.UTC).format(exactCreatedAt);
        String month = MONTH.withZone(ZoneOffset.UTC).format(exactCreatedAt);
        String day = DAY.withZone(ZoneOffset.UTC).format(exactCreatedAt);
        validateObjectId(objectId.value(), writer, run, year, month, day);
        return new ObjectKey(prefix(cluster).value()
                + year + "/" + month + "/" + day + "/"
                + writer + "/" + run + "/" + objectId.value() + ".nrs");
    }

    public static ParsedWalObjectKey parse(String cluster, ObjectKey key) {
        Objects.requireNonNull(key, "key");
        String exactPrefix = prefix(cluster).value();
        if (!key.value().startsWith(exactPrefix)) {
            throw new IllegalArgumentException("WAL object key is outside the cluster prefix");
        }
        String[] components = key.value().substring(exactPrefix.length()).split("/", -1);
        if (components.length != 6
                || components[0].length() != 4
                || components[1].length() != 2
                || components[2].length() != 2
                || !components[5].endsWith(".nrs")) {
            throw new IllegalArgumentException("WAL object key path is not canonical");
        }
        requireDate(components[0], components[1], components[2]);
        String writer = requireStableHash(components[3], "writerIdHash");
        String run = requireCanonicalComponent(components[4], "writerRunIdHash");
        String objectIdValue = components[5].substring(0, components[5].length() - ".nrs".length());
        validateObjectId(
                objectIdValue,
                writer,
                run,
                components[0],
                components[1],
                components[2]);
        ObjectId objectId = new ObjectId(objectIdValue);
        String canonical = exactPrefix
                + components[0] + "/" + components[1] + "/" + components[2] + "/"
                + writer + "/" + run + "/" + objectId.value() + ".nrs";
        if (!canonical.equals(key.value())) {
            throw new IllegalArgumentException("WAL object key is not canonical");
        }
        return new ParsedWalObjectKey(
                components[0], components[1], components[2], writer, run, objectId);
    }

    private static void validateObjectId(
            String value,
            String writer,
            String run,
            String year,
            String month,
            String day) {
        String exact = Objects.requireNonNull(value, "objectId");
        String prefix = "wo-";
        if (!exact.startsWith(prefix) || exact.length() < 3 + 14 + 1 + 52 + 1 + 1 + 1 + 19) {
            throw new IllegalArgumentException("WAL object id is not canonical");
        }
        String timestampValue = exact.substring(3, 17);
        String writerValue = exact.substring(18, 70);
        if (exact.charAt(17) != '-'
                || exact.charAt(70) != '-'
                || !writerValue.equals(writer)) {
            throw new IllegalArgumentException("WAL object id writer identity is not canonical");
        }
        String tail = exact.substring(71);
        int sequenceDelimiter = tail.lastIndexOf('-');
        if (sequenceDelimiter <= 0
                || !tail.substring(0, sequenceDelimiter).equals(run)) {
            throw new IllegalArgumentException("WAL object id run identity is not canonical");
        }
        final LocalDateTime timestamp;
        try {
            timestamp = LocalDateTime.parse(timestampValue, OBJECT_TIME);
        } catch (DateTimeParseException failure) {
            throw new IllegalArgumentException("WAL object id timestamp is invalid", failure);
        }
        if (!YEAR.format(timestamp).equals(year)
                || !MONTH.format(timestamp).equals(month)
                || !DAY.format(timestamp).equals(day)) {
            throw new IllegalArgumentException("WAL object id timestamp differs from its date path");
        }
        KeyComponentCodec.decodeNonNegativeLong(tail.substring(sequenceDelimiter + 1));
    }

    private static void requireDate(String year, String month, String day) {
        try {
            LocalDate.of(
                    Integer.parseInt(year),
                    Integer.parseInt(month),
                    Integer.parseInt(day));
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("WAL object key date is invalid", failure);
        }
    }

    private static String requireStableHash(String value, String field) {
        value = requireText(value, field);
        if (value.length() != 52 || !value.matches("[a-z2-7]+")) {
            throw new IllegalArgumentException(field + " must be a canonical SHA-256 base32 component");
        }
        return value;
    }

    private static String requireCanonicalComponent(String value, String field) {
        value = requireText(value, field);
        final String decoded;
        try {
            decoded = KeyComponentCodec.decodeComponent(value);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(field + " must be a canonical key component", failure);
        }
        if (!KeyComponentCodec.encodeComponent(decoded).equals(value)) {
            throw new IllegalArgumentException(field + " must be a canonical key component");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    public record ParsedWalObjectKey(
            String year,
            String month,
            String day,
            String writerIdHash,
            String writerRunIdHash,
            ObjectId objectId) {
        public ParsedWalObjectKey {
            requireDate(year, month, day);
            writerIdHash = requireStableHash(writerIdHash, "writerIdHash");
            writerRunIdHash = requireCanonicalComponent(writerRunIdHash, "writerRunIdHash");
            Objects.requireNonNull(objectId, "objectId");
        }
    }
}
