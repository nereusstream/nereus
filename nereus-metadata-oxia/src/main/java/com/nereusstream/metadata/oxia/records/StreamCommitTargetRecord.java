/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.SchemaRef;
import java.util.List;
import java.util.Objects;

/** Generic-target immutable append intent linked by the unchanged stream head. */
public record StreamCommitTargetRecord(
        String streamId, String commitId, String previousCommitId,
        long offsetStart, long offsetEnd, long generation, long cumulativeSize, long commitVersion,
        String writerId, String writerRunIdHash, long writerEpoch, String fencingTokenHash,
        ReadTargetRecord readTarget, String payloadFormat, int recordCount, int entryCount, long logicalBytes,
        List<SchemaRef> schemaRefs, String projectionRef,
        long minEventTimeMillis, long maxEventTimeMillis, long preparedAtMillis, long metadataVersion) {
    public StreamCommitTargetRecord {
        streamId = text(streamId, "streamId");
        commitId = text(commitId, "commitId");
        previousCommitId = Objects.requireNonNull(previousCommitId, "previousCommitId");
        writerId = text(writerId, "writerId");
        writerRunIdHash = text(writerRunIdHash, "writerRunIdHash");
        fencingTokenHash = text(fencingTokenHash, "fencingTokenHash");
        Objects.requireNonNull(readTarget, "readTarget");
        payloadFormat = text(payloadFormat, "payloadFormat");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        if (offsetStart < 0 || offsetEnd <= offsetStart || generation != 0 || cumulativeSize < logicalBytes
                || commitVersion <= 0 || writerEpoch < 0 || recordCount <= 0 || entryCount <= 0 || logicalBytes < 0
                || minEventTimeMillis < 0 || maxEventTimeMillis < minEventTimeMillis
                || preparedAtMillis < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("generic stream commit numeric fields are invalid");
        }
        MetadataRecordValidation.requireDenseLogicalRange(offsetStart, offsetEnd, recordCount, "commit range");
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
