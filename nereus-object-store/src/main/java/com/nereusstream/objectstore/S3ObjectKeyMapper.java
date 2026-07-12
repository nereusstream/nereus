/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.ObjectKey;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/** Strict one-to-one mapping from protocol-neutral keys into one canonical S3 prefix. */
public final class S3ObjectKeyMapper {
    private static final int MAX_S3_KEY_BYTES = 1_024;
    private final String prefix;

    public S3ObjectKeyMapper(String prefix) {
        this.prefix = requirePrefix(prefix);
    }

    public String map(ObjectKey key) {
        Objects.requireNonNull(key, "key");
        byte[] logicalBytes = strictUtf8(key.value(), "object key");
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(logicalBytes);
        String mapped = prefix + "/objects/v1/" + encoded;
        if (mapped.getBytes(StandardCharsets.UTF_8).length > MAX_S3_KEY_BYTES) {
            throw new IllegalArgumentException("mapped S3 key exceeds 1024 UTF-8 bytes");
        }
        return mapped;
    }

    private static String requirePrefix(String value) {
        Objects.requireNonNull(value, "prefix");
        if (value.isBlank() || value.startsWith("/") || value.endsWith("/")) {
            throw new IllegalArgumentException("prefix is not canonical");
        }
        for (String part : value.split("/", -1)) {
            if (part.isEmpty() || part.equals(".") || part.equals("..")) {
                throw new IllegalArgumentException("prefix is not canonical");
            }
        }
        if (value.chars().anyMatch(character -> character == 0 || Character.isISOControl(character))) {
            throw new IllegalArgumentException("prefix is not canonical");
        }
        strictUtf8(value, "prefix");
        return value;
    }

    private static byte[] strictUtf8(String value, String name) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException error) {
            throw new IllegalArgumentException(name + " must be valid UTF-8", error);
        }
    }
}
