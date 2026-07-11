/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ReadTarget;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Canonical in-memory committed append hydrated from either durable record generation. */
public record CommittedAppend(
        StreamId streamId, String commitId, String previousCommitId, ReadTarget readTarget,
        OffsetRange range, long generation, long cumulativeSize, long commitVersion,
        PayloadFormat payloadFormat, int recordCount, int entryCount, long logicalBytes,
        List<SchemaRef> schemaRefs, Optional<ProjectionRef> projectionRef,
        long minEventTimeMillis, long maxEventTimeMillis) {
    public CommittedAppend {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(commitId, "commitId");
        Objects.requireNonNull(previousCommitId, "previousCommitId");
        Objects.requireNonNull(readTarget, "readTarget");
        Objects.requireNonNull(range, "range");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        if (commitId.isBlank() || range.isEmpty() || generation != 0 || cumulativeSize < logicalBytes
                || commitVersion <= 0 || recordCount != range.recordCount() || entryCount <= 0 || logicalBytes < 0) {
            throw new IllegalArgumentException("committed append fields are invalid");
        }
    }
}
