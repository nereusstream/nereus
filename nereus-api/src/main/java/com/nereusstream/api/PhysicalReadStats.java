/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import java.util.Objects;

/** Provider-neutral physical/logical byte accounting for one complete resolved source range. */
public record PhysicalReadStats(
        Checksum targetIdentity,
        long resolvedPayloadBytes,
        long resolvedAuxiliaryBytes,
        long physicalPayloadBytesRead,
        long physicalAuxiliaryBytesRead,
        long returnedPayloadBytes) {
    public PhysicalReadStats {
        Objects.requireNonNull(targetIdentity, "targetIdentity");
        if (targetIdentity.type() != ChecksumType.SHA256
                || resolvedPayloadBytes < 0
                || resolvedAuxiliaryBytes < 0
                || physicalPayloadBytesRead < 0
                || physicalAuxiliaryBytesRead < 0
                || returnedPayloadBytes < 0) {
            throw new IllegalArgumentException("invalid physical read accounting");
        }
    }

    public long physicalBytesRead() {
        return Math.addExact(physicalPayloadBytesRead, physicalAuxiliaryBytesRead);
    }

    public long ioDeltaBytes() {
        return Math.subtractExact(physicalBytesRead(), returnedPayloadBytes);
    }

    public long amplificationBytes() {
        return Math.max(0, ioDeltaBytes());
    }

    public long compressionSavingsBytes() {
        long delta = ioDeltaBytes();
        return delta < 0 ? Math.negateExact(delta) : 0;
    }
}
