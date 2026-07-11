/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Canonical in-memory offset-index entry hydrated from legacy or generic records. */
public record OffsetIndexEntry(
        StreamId streamId, OffsetRange range, long generation, long cumulativeSize, ReadTarget readTarget,
        PayloadFormat payloadFormat, int recordCount, int entryCount, long logicalBytes,
        List<SchemaRef> schemaRefs, Optional<ProjectionRef> projectionRef,
        long commitVersion, boolean tombstoned, long metadataVersion) {
    public OffsetIndexEntry {
        Objects.requireNonNull(streamId); Objects.requireNonNull(range); Objects.requireNonNull(readTarget);
        Objects.requireNonNull(payloadFormat); schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        projectionRef = Objects.requireNonNull(projectionRef);
        if (range.isEmpty() || generation < 0 || cumulativeSize < logicalBytes || recordCount != range.recordCount()
                || entryCount <= 0 || logicalBytes < 0 || commitVersion <= 0 || metadataVersion < 0)
            throw new IllegalArgumentException("offset index entry fields are invalid");
    }

    public long offsetStart() { return range.startOffset(); }
    public long offsetEnd() { return range.endOffset(); }

    public static OffsetIndexEntry fromLegacy(OffsetIndexRecord record) {
        Objects.requireNonNull(record, "record");
        EntryIndexReferenceRecord raw = record.entryIndexRef();
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.valueOf(raw.location()),
                raw.objectId().isEmpty() ? Optional.empty() : Optional.of(new ObjectId(raw.objectId())),
                raw.objectKey().isEmpty() ? Optional.empty() : Optional.of(new ObjectKey(raw.objectKey())),
                raw.inlineData().length == 0 ? Optional.empty() : Optional.of(raw.inlineData()),
                raw.offset(), raw.length(),
                new Checksum(ChecksumType.valueOf(raw.checksumType()), raw.checksumValue()));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1, new ObjectId(record.objectId()), new ObjectKey(record.objectKey()),
                ObjectType.valueOf(record.objectType()), record.physicalFormat(), record.logicalFormat(),
                record.sliceId(), record.objectOffset(), record.objectLength(),
                new Checksum(ChecksumType.valueOf(record.sliceChecksumType()), record.sliceChecksumValue()), index);
        return new OffsetIndexEntry(new StreamId(record.streamId()),
                new OffsetRange(record.offsetStart(), record.offsetEnd()), record.generation(), record.cumulativeSize(),
                target, PayloadFormat.valueOf(record.payloadFormat()), record.recordCount(), record.entryCount(),
                record.logicalBytes(), record.schemaRefs(), ProjectionIdentity.decode(record.projectionRef()),
                record.commitVersion(), record.tombstoned(), record.metadataVersion());
    }
}
