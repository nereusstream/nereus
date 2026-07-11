/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.api;

import com.nereusstream.api.target.ReadTarget;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A committed logical range and its selected physical read target. */
public record ResolvedRange(
        OffsetRange offsetRange,
        long generation,
        ReadTarget readTarget,
        PayloadFormat payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        Optional<ProjectionRef> projectionRef,
        long commitVersion) {
    public ResolvedRange {
        Objects.requireNonNull(offsetRange, "offsetRange");
        Objects.requireNonNull(readTarget, "readTarget");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        if (offsetRange.isEmpty() || generation < 0 || commitVersion <= 0
                || recordCount <= 0 || entryCount <= 0 || logicalBytes < 0) {
            throw new IllegalArgumentException("invalid resolved range fields");
        }
        if (recordCount != offsetRange.recordCount()) {
            throw new IllegalArgumentException("recordCount must equal range length");
        }
        if (payloadFormat == PayloadFormat.OPAQUE_RECORD_BATCH && entryCount != recordCount) {
            throw new IllegalArgumentException("OPAQUE_RECORD_BATCH entryCount must equal recordCount");
        }
    }
}
