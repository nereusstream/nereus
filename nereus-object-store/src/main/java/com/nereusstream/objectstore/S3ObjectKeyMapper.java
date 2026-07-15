/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.ObjectKey;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Strict one-to-one mapping from protocol-neutral keys into one canonical S3 prefix. */
public final class S3ObjectKeyMapper {
    private static final int MAX_S3_KEY_BYTES = 1_024;
    private static final String BASE64_URL_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
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

    public String mapPrefix(ObjectKeyPrefix logicalPrefix) {
        byte[] bytes = strictUtf8(Objects.requireNonNull(logicalPrefix, "logicalPrefix").value(), "object key prefix");
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        return objectRoot() + encoded.substring(0, (bytes.length * Byte.SIZE) / 6);
    }

    /** Returns disjoint S3 prefixes whose union is exactly the supplied logical byte prefix. */
    public List<String> mapPrefixes(ObjectKeyPrefix logicalPrefix) {
        byte[] bytes = strictUtf8(Objects.requireNonNull(logicalPrefix, "logicalPrefix").value(), "object key prefix");
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        int remainder = bytes.length % 3;
        if (remainder == 0) {
            return List.of(objectRoot() + encoded);
        }
        int variants = remainder == 1 ? 16 : 4;
        String stable = encoded.substring(0, encoded.length() - 1);
        int first = BASE64_URL_ALPHABET.indexOf(encoded.charAt(encoded.length() - 1));
        if (first < 0 || first + variants > BASE64_URL_ALPHABET.length()) {
            throw new IllegalStateException("base64url prefix expansion is invalid");
        }
        List<String> result = new ArrayList<>(variants);
        for (int variant = 0; variant < variants; variant++) {
            result.add(objectRoot() + stable + BASE64_URL_ALPHABET.charAt(first + variant));
        }
        result.sort(Comparator.naturalOrder());
        return List.copyOf(result);
    }

    public ObjectKey unmap(String mappedKey) {
        Objects.requireNonNull(mappedKey, "mappedKey");
        if (!mappedKey.startsWith(objectRoot())) {
            throw new IllegalArgumentException("S3 key is outside the configured Nereus object prefix");
        }
        String encoded = mappedKey.substring(objectRoot().length());
        if (encoded.isEmpty()) {
            throw new IllegalArgumentException("mapped object key has no identity payload");
        }
        byte[] decoded;
        try {
            decoded = Base64.getUrlDecoder().decode(encoded);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("mapped object key is not canonical base64url", failure);
        }
        String logical;
        try {
            logical = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(decoded))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException("mapped object key is not valid UTF-8", failure);
        }
        ObjectKey result = new ObjectKey(logical);
        if (!map(result).equals(mappedKey)) {
            throw new IllegalArgumentException("mapped object key is not canonical");
        }
        return result;
    }

    private String objectRoot() {
        return prefix + "/objects/v1/";
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
