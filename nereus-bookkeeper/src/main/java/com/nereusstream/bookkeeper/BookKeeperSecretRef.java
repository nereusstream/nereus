/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Non-secret durable identity of a password resolved only at the composition boundary. */
public record BookKeeperSecretRef(String reference, String identityVersion) {
    public BookKeeperSecretRef {
        reference = text(reference, "reference");
        identityVersion = text(identityVersion, "identityVersion");
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > 16 * 1024) {
            throw new IllegalArgumentException(field + " must be nonblank and bounded");
        }
        return value;
    }

    @Override public String toString() { return "BookKeeperSecretRef[redacted, version=" + identityVersion + "]"; }
}
