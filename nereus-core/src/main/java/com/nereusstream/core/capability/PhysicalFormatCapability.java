/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.core.read.ReadTargetReaderKey;
import java.util.Objects;

/** One exact physical+logical format capability advertised by a runtime. */
public record PhysicalFormatCapability(
        ReadTargetReaderKey readerKey,
        ReadView readView,
        PayloadFormat payloadFormat,
        boolean readable,
        boolean writable) {
    public PhysicalFormatCapability {
        Objects.requireNonNull(readerKey, "readerKey");
        Objects.requireNonNull(readView, "readView");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        if (!readable && !writable) {
            throw new IllegalArgumentException("format capability must enable read or write");
        }
        if (readerKey.logicalFormat().isEmpty()) {
            throw new IllegalArgumentException("format capability requires an exact logical format");
        }
    }

    String canonicalIdentity() {
        return readerKey.targetType().name()
                + "\u0000" + readerKey.targetVersion()
                + "\u0000" + readerKey.objectType().orElseThrow().name()
                + "\u0000" + readerKey.physicalFormat().orElseThrow()
                + "\u0000" + readerKey.logicalFormat().orElseThrow()
                + "\u0000" + readView.name()
                + "\u0000" + payloadFormat.name()
                + "\u0000" + readable
                + "\u0000" + writable;
    }
}
