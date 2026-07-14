/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.Objects;
import java.util.regex.Pattern;

/** Canonical validator for F3 128-bit random identifiers. */
public final class CursorIds {
    private static final Pattern RANDOM_ID = Pattern.compile("[0-9a-f]{32}");

    private CursorIds() {
    }

    public static String requireRandomId(String value, String fieldName) {
        Objects.requireNonNull(fieldName, "fieldName");
        Objects.requireNonNull(value, fieldName);
        if (!RANDOM_ID.matcher(value).matches()) {
            throw new IllegalArgumentException(fieldName + " must be 32 lowercase hexadecimal characters");
        }
        return value;
    }
}
