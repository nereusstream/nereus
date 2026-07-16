/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import java.time.Duration;
import java.util.Objects;

/** Exact per-stream append facts checked on the serialized lane before primary WAL preparation. */
public record AppendAdmissionRequest(
        StreamId streamId,
        StorageProfile storageProfile,
        DurabilityLevel durabilityLevel,
        Duration timeout) {
    public AppendAdmissionRequest {
        Objects.requireNonNull(streamId, "streamId");
        storageProfile = Objects.requireNonNull(
                        storageProfile, "storageProfile")
                .canonical();
        Objects.requireNonNull(durabilityLevel, "durabilityLevel");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
