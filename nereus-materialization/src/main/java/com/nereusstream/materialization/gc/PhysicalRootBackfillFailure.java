/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Redacted deterministic failure retained in a bounded backfill report. */
public record PhysicalRootBackfillFailure(
        String resourceIdentitySha256,
        PhysicalRootBackfillStage stage,
        String errorCode) {
    public PhysicalRootBackfillFailure {
        new Checksum(
                ChecksumType.SHA256,
                Objects.requireNonNull(
                        resourceIdentitySha256, "resourceIdentitySha256"));
        Objects.requireNonNull(stage, "stage");
        errorCode = requireErrorCode(errorCode);
    }

    private static String requireErrorCode(String value) {
        Objects.requireNonNull(value, "errorCode");
        if (value.isEmpty() || value.length() > 128) {
            throw new IllegalArgumentException(
                    "errorCode must contain between one and 128 characters");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'A' && character <= 'Z')
                    || (character >= '0' && character <= '9')
                    || character == '_')) {
                throw new IllegalArgumentException(
                        "errorCode must use uppercase machine-token characters");
            }
        }
        return value;
    }
}
