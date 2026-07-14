/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.Objects;

/** One decoded cursor root and its authoritative Oxia version. */
public record VersionedCursorState(CursorStateRecord value, long metadataVersion) {
    public VersionedCursorState {
        Objects.requireNonNull(value, "value");
        if (metadataVersion < 0) {
            throw new IllegalArgumentException("metadataVersion must be non-negative");
        }
        if (value.metadataVersion() != 0) {
            throw new IllegalArgumentException("encoded cursor metadataVersion must remain zero");
        }
    }
}
