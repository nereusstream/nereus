/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ReadTarget;
import java.util.List;
import java.util.Objects;
public record DurablePrimaryAppend(StreamId streamId, ReadTarget readTarget, PayloadFormat payloadFormat,
        int recordCount, int entryCount, long logicalBytes, List<SchemaRef> schemaRefs,
        long minEventTimeMillis, long maxEventTimeMillis, ProviderCommitEvidence providerCommitEvidence) {
    public DurablePrimaryAppend { Objects.requireNonNull(streamId); Objects.requireNonNull(readTarget);
        Objects.requireNonNull(payloadFormat); schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        Objects.requireNonNull(providerCommitEvidence); if (recordCount <= 0 || entryCount <= 0 || logicalBytes < 0
                || minEventTimeMillis < 0 || maxEventTimeMillis < minEventTimeMillis)
            throw new IllegalArgumentException("invalid durable primary append"); }
}
