/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import java.util.Objects;

/** One decoded retention root and its authoritative Oxia version. */
public record VersionedCursorRetention(CursorRetentionRecord value, long metadataVersion) {
    public VersionedCursorRetention {
        Objects.requireNonNull(value, "value");
        if (metadataVersion < 0) {
            throw new IllegalArgumentException("metadataVersion must be non-negative");
        }
        if (value.metadataVersion() != 0) {
            throw new IllegalArgumentException("encoded retention metadataVersion must remain zero");
        }
    }
}
