/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.ObjectKeyHash;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** One exact source protection retained by a durable GC retirement journal. */
public record GcRetirementProtectionRecord(
        int schemaVersion,
        String objectKeyHash,
        String gcAttemptId,
        String protectionKey,
        long protectionMetadataVersion,
        String protectionDurableValueSha256,
        ObjectProtectionRecord protection,
        long metadataVersion) {
    public GcRetirementProtectionRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        new ObjectKeyHash(objectKeyHash);
        gcAttemptId = F4RecordValidation.requireBase32Id(gcAttemptId, "gcAttemptId");
        protectionKey = boundedKey(protectionKey, "protectionKey");
        F4RecordValidation.requireMetadataVersion(protectionMetadataVersion);
        protectionDurableValueSha256 = F4RecordValidation.requireSha256(
                protectionDurableValueSha256, "protectionDurableValueSha256");
        Objects.requireNonNull(protection, "protection");
        if (!protection.objectKeyHash().equals(objectKeyHash)
                || protection.metadataVersion() != protectionMetadataVersion) {
            throw new IllegalArgumentException(
                    "journal protection does not match its object/version identity");
        }
        F4RecordValidation.requireMetadataVersion(metadataVersion);
    }

    public GcRetirementProtectionRecord withMetadataVersion(long version) {
        return new GcRetirementProtectionRecord(
                schemaVersion,
                objectKeyHash,
                gcAttemptId,
                protectionKey,
                protectionMetadataVersion,
                protectionDurableValueSha256,
                protection,
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
