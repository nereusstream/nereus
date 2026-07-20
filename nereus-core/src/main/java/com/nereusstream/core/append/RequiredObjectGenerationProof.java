/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.Checksum;
import java.util.Objects;

/** Provider-neutral evidence returned only after the exact higher generation passes normal read admission. */
public record RequiredObjectGenerationProof(
        RequiredObjectGenerationRequest request,
        String taskId,
        long generation,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexSha256,
        Checksum targetIdentitySha256) {
    public RequiredObjectGenerationProof {
        Objects.requireNonNull(request, "request");
        taskId = text(taskId, "taskId");
        indexKey = text(indexKey, "indexKey");
        Objects.requireNonNull(indexSha256, "indexSha256");
        Objects.requireNonNull(targetIdentitySha256, "targetIdentitySha256");
        if (generation <= 0 || indexMetadataVersion < 0) {
            throw new IllegalArgumentException("required Object-generation proof identity is invalid");
        }
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
