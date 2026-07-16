/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.ObjectKeyHash;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** One exact metadata-removal fact retained by a durable GC retirement journal. */
public record GcRetirementRemovalRecord(
        int schemaVersion,
        String objectKeyHash,
        String gcAttemptId,
        String removalType,
        String removalKey,
        long removalMetadataVersion,
        String removalDurableValueSha256,
        long metadataVersion) {
    public GcRetirementRemovalRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        new ObjectKeyHash(objectKeyHash);
        gcAttemptId = F4RecordValidation.requireBase32Id(gcAttemptId, "gcAttemptId");
        Objects.requireNonNull(removalType, "removalType");
        if (removalType.length() > 128
                || !removalType.matches("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")) {
            throw new IllegalArgumentException("removalType is not canonical");
        }
        removalKey = boundedKey(removalKey, "removalKey");
        F4RecordValidation.requireMetadataVersion(removalMetadataVersion);
        removalDurableValueSha256 = F4RecordValidation.requireSha256(
                removalDurableValueSha256, "removalDurableValueSha256");
        F4RecordValidation.requireMetadataVersion(metadataVersion);
    }

    public GcRetirementRemovalRecord withMetadataVersion(long version) {
        return new GcRetirementRemovalRecord(
                schemaVersion,
                objectKeyHash,
                gcAttemptId,
                removalType,
                removalKey,
                removalMetadataVersion,
                removalDurableValueSha256,
                version);
    }

    private static String boundedKey(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > 4_096) {
            throw new IllegalArgumentException(name + " must be non-blank and at most 4096 bytes");
        }
        return value;
    }
}
