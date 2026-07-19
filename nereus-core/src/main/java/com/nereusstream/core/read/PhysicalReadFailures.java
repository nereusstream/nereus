/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.Optional;

/** Stable classification shared by Object and BookKeeper physical readers. */
public final class PhysicalReadFailures {
    private PhysicalReadFailures() { }

    public static Optional<PhysicalReadFailureKind> classify(Throwable failure) {
        if (!(failure instanceof NereusException nereus)) return Optional.empty();
        return Optional.ofNullable(switch (nereus.code()) {
            case OBJECT_NOT_FOUND, PRIMARY_WAL_TARGET_NOT_FOUND -> PhysicalReadFailureKind.NOT_FOUND;
            case OBJECT_CHECKSUM_MISMATCH, PRIMARY_WAL_CHECKSUM_MISMATCH ->
                    PhysicalReadFailureKind.CHECKSUM_MISMATCH;
            case OBJECT_READ_FAILED, PRIMARY_WAL_READ_FAILED, TIMEOUT -> PhysicalReadFailureKind.TRANSIENT_IO;
            case FENCED_APPEND, STORAGE_CLOSED -> PhysicalReadFailureKind.FENCED_OR_CLOSED;
            case METADATA_INVARIANT_VIOLATION -> PhysicalReadFailureKind.METADATA_INVARIANT;
            case UNSUPPORTED_READ_TARGET, UNSUPPORTED_FORMAT -> PhysicalReadFailureKind.UNSUPPORTED_TARGET;
            default -> null;
        });
    }
}
