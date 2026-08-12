/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** Exact fresh-bootstrap BookKeeper INSTANCEID bytes admitted by Registry v1. */
public final class BookKeeperInstanceIdV1 {
    public static final int LENGTH = 36;

    private final String value;
    private final CanonicalBytes bytes;

    private BookKeeperInstanceIdV1(String value) {
        this.value = value;
        this.bytes = CanonicalBytes.copyOf(value.getBytes(StandardCharsets.US_ASCII));
    }

    public static BookKeeperInstanceIdV1 parse(String candidate) {
        Objects.requireNonNull(candidate, "candidate");
        if (candidate.length() != LENGTH || !candidate.chars().allMatch(value -> value < 128)) {
            throw invalid("BookKeeper INSTANCEID must be exactly 36 ASCII bytes");
        }
        final UUID parsed;
        try {
            parsed = UUID.fromString(candidate);
        } catch (IllegalArgumentException failure) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_IDENTITY_INVALID,
                    "BookKeeper INSTANCEID is not a canonical UUID",
                    failure);
        }
        if (!parsed.toString().equals(candidate)
                || (parsed.getMostSignificantBits() == 0 && parsed.getLeastSignificantBits() == 0)) {
            throw invalid("BookKeeper INSTANCEID must be lowercase canonical and non-zero");
        }
        return new BookKeeperInstanceIdV1(candidate);
    }

    public static BookKeeperInstanceIdV1 fromBytes(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length != LENGTH) {
            throw invalid("BookKeeper INSTANCEID must be exactly 36 bytes");
        }
        for (byte value : bytes) {
            if (value < 0) {
                throw invalid("BookKeeper INSTANCEID must be ASCII");
            }
        }
        return parse(new String(bytes, StandardCharsets.US_ASCII));
    }

    public CanonicalBytes bytes() {
        return bytes;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof BookKeeperInstanceIdV1 that && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }

    private static RegistryValidationException invalid(String message) {
        return new RegistryValidationException(RegistryRejectionCodeV1.REGISTRY_IDENTITY_INVALID, message);
    }
}
