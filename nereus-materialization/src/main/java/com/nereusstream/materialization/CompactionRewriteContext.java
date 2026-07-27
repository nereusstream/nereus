/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;
import java.util.OptionalLong;

/** Immutable message-format facts frozen by one ranged compaction plan. */
public record CompactionRewriteContext(
        byte targetMagic,
        Checksum messageFormatSha256,
        boolean allowUncompressedFallback,
        OptionalLong deleteHorizonMillis) {
    public CompactionRewriteContext {
        Objects.requireNonNull(messageFormatSha256, "messageFormatSha256");
        deleteHorizonMillis = Objects.requireNonNull(deleteHorizonMillis, "deleteHorizonMillis");
        if (targetMagic < 0
                || messageFormatSha256.type() != ChecksumType.SHA256
                || (deleteHorizonMillis.isPresent() && deleteHorizonMillis.getAsLong() < 0)) {
            throw new IllegalArgumentException("invalid ranged compaction rewrite context");
        }
    }
}
