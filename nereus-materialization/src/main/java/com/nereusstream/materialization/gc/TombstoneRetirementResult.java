/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ObjectKeyHash;
import java.util.Objects;

/** Exact mutation summary for one DELETED-root retirement attempt. */
public record TombstoneRetirementResult(
        ObjectKeyHash object,
        long rootMetadataVersion,
        TombstoneRetirementStatus status,
        boolean referencesRetired,
        boolean manifestRetired,
        boolean rootRetired) {
    public TombstoneRetirementResult {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(status, "status");
        if (rootMetadataVersion < 0) {
            throw new IllegalArgumentException("rootMetadataVersion must be non-negative");
        }
        if (rootRetired != (status == TombstoneRetirementStatus.RETIRED)) {
            throw new IllegalArgumentException("only RETIRED results may report a retired root");
        }
    }

    public static TombstoneRetirementResult simple(
            ObjectKeyHash object,
            long rootMetadataVersion,
            TombstoneRetirementStatus status) {
        return new TombstoneRetirementResult(
                object, rootMetadataVersion, status, false, false, false);
    }
}
