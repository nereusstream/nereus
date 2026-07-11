/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.ApiLimits;
import java.util.Arrays;
import java.util.Objects;

/** Durable, codec-neutral envelope for a physical read target. */
public record ReadTargetRecord(
        String targetType,
        int targetVersion,
        String payloadEncoding,
        byte[] payload,
        String identityChecksumType,
        String identityChecksumValue) {
    public ReadTargetRecord {
        targetType = requireText(targetType, "targetType");
        payloadEncoding = requireText(payloadEncoding, "payloadEncoding");
        payload = Objects.requireNonNull(payload, "payload").clone();
        identityChecksumType = requireText(identityChecksumType, "identityChecksumType");
        identityChecksumValue = requireText(identityChecksumValue, "identityChecksumValue");
        if (targetVersion <= 0 || payload.length == 0
                || payload.length > ApiLimits.MAX_READ_TARGET_ENCODED_BYTES) {
            throw new IllegalArgumentException("invalid read target version or payload length");
        }
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ReadTargetRecord that
                && targetVersion == that.targetVersion
                && targetType.equals(that.targetType)
                && payloadEncoding.equals(that.payloadEncoding)
                && identityChecksumType.equals(that.identityChecksumType)
                && identityChecksumValue.equals(that.identityChecksumValue)
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(targetType, targetVersion, payloadEncoding,
                identityChecksumType, identityChecksumValue) + Arrays.hashCode(payload);
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }
}
