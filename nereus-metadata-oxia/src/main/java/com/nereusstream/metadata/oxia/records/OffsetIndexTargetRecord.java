/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.SchemaRef;
import java.util.List;
import java.util.Objects;

/** Generic generation-zero offset-index value. */
public record OffsetIndexTargetRecord(
        String streamId, long offsetStart, long offsetEnd, long generation, long cumulativeSize,
        ReadTargetRecord readTarget, String payloadFormat, int recordCount, int entryCount, long logicalBytes,
        List<SchemaRef> schemaRefs, String projectionRef,
        long minEventTimeMillis, long maxEventTimeMillis, long commitVersion,
        boolean tombstoned, long metadataVersion) {
    public OffsetIndexTargetRecord {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(readTarget, "readTarget");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        if (streamId.isBlank() || payloadFormat.isBlank() || offsetStart < 0 || offsetEnd <= offsetStart
                || generation != 0 || cumulativeSize < logicalBytes || recordCount <= 0 || entryCount <= 0
                || logicalBytes < 0 || minEventTimeMillis < 0 || maxEventTimeMillis < minEventTimeMillis
                || commitVersion <= 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("generic offset index fields are invalid");
        }
        MetadataRecordValidation.requireDenseLogicalRange(offsetStart, offsetEnd, recordCount, "index range");
    }
}
