/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.util.Objects;

/** Successful V1 capability proof; only capabilitySha256 is persisted in cluster activation metadata. */
public record ObjectStoreDeleteCapabilityProof(
        int protocolVersion,
        String capabilitySha256,
        String probeObjectKeySha256,
        long completedAtMillis) {
    public static final int PROTOCOL_VERSION = 1;

    public ObjectStoreDeleteCapabilityProof {
        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("protocolVersion must be 1");
        }
        capabilitySha256 = requireSha256(capabilitySha256, "capabilitySha256");
        probeObjectKeySha256 = requireSha256(
                probeObjectKeySha256, "probeObjectKeySha256");
        if (completedAtMillis <= 0) {
            throw new IllegalArgumentException("completedAtMillis must be positive");
        }
    }

    private static String requireSha256(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.length() != 64) {
            throw new IllegalArgumentException(name + " must be lowercase SHA-256");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9')
                    || (character >= 'a' && character <= 'f'))) {
                throw new IllegalArgumentException(name + " must be lowercase SHA-256");
            }
        }
        return value;
    }
}
