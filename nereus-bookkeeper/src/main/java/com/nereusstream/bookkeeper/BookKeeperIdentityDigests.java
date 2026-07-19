/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Non-secret stable identity digests stored instead of process/session secret material. */
public final class BookKeeperIdentityDigests {
    private BookKeeperIdentityDigests() { }

    public static String sha256(String value) {
        Objects.requireNonNull(value, "value");
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
