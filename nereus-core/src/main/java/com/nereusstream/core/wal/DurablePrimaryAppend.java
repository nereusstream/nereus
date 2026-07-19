/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ReadTarget;
import java.util.List;
import java.util.Objects;
public record DurablePrimaryAppend(StreamId streamId, ReadTarget readTarget,
        PrimaryPhysicalIdentity physicalIdentity, Checksum payloadChecksum, PayloadFormat payloadFormat,
        int recordCount, int entryCount, long logicalBytes, List<SchemaRef> schemaRefs,
        long minEventTimeMillis, long maxEventTimeMillis, ProviderAppendToken providerToken) {
    public DurablePrimaryAppend { Objects.requireNonNull(streamId); Objects.requireNonNull(readTarget);
        Objects.requireNonNull(physicalIdentity); Objects.requireNonNull(payloadChecksum);
        Objects.requireNonNull(payloadFormat); schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        Objects.requireNonNull(providerToken); if (physicalIdentity.targetType() != readTarget.type()
                || recordCount <= 0 || entryCount <= 0 || logicalBytes < 0
                || minEventTimeMillis < 0 || maxEventTimeMillis < minEventTimeMillis)
            throw new IllegalArgumentException("invalid durable primary append"); }

    /** Compatibility alias while Object-WAL call sites migrate to the opaque token name. */
    @Deprecated(forRemoval = true)
    public ProviderAppendToken providerCommitEvidence() { return providerToken; }
}
