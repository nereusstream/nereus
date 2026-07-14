/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.keys.DeterministicIds;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** The sole exact-name and hash helper for durable F3 cursors. */
public final class CursorNames {
    public static final int MAX_CURSOR_NAME_BYTES = 16 * 1024;

    private static final String NAME_HASH_DOMAIN = "nereus-cursor-v1\0";

    private CursorNames() {
    }

    public static String requireCursorName(String cursorName) {
        Objects.requireNonNull(cursorName, "cursorName");
        if (cursorName.isBlank()) {
            throw new IllegalArgumentException("cursorName cannot be blank");
        }
        if (cursorName.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("cursorName cannot contain NUL");
        }
        int encodedLength;
        try {
            encodedLength = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(cursorName))
                    .remaining();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("cursorName must be valid UTF-8", e);
        }
        if (encodedLength > MAX_CURSOR_NAME_BYTES) {
            throw new IllegalArgumentException("cursorName exceeds the UTF-8 byte limit");
        }
        return cursorName;
    }

    public static String cursorNameHash(String cursorName) {
        return DeterministicIds.stableHashComponent(NAME_HASH_DOMAIN + requireCursorName(cursorName));
    }
}
