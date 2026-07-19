/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.api;

import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One returned logical batch clipped to a read request. */
public final class ReadBatch {
    private final OffsetRange range;
    private final PayloadFormat payloadFormat;
    private final byte[] payload;
    private final List<SchemaRef> schemaRefs;
    private final Optional<ProjectionRef> projectionRef;
    private final ReadSourceRef source;
    private final long compatibilityObjectOffset;
    private final long compatibilityObjectLength;

    /**
     * Transitional Object-only constructor retained for source compatibility. Physical reader adapters normalize
     * this synthetic compatibility source to the exact resolved target before common correctness code observes it.
     */
    @Deprecated(forRemoval = true)
    public ReadBatch(
            OffsetRange range,
            PayloadFormat payloadFormat,
            byte[] payload,
            List<SchemaRef> schemaRefs,
            EntryIndexRef entryIndexRef,
            Optional<ProjectionRef> projectionRef,
            ObjectId sourceObjectId,
            long sourceObjectOffset,
            long sourceObjectLength) {
        this(range, payloadFormat, payload, schemaRefs, projectionRef,
                legacySource(range, sourceObjectId, sourceObjectOffset, sourceObjectLength, entryIndexRef),
                sourceObjectOffset, sourceObjectLength);
    }

    public ReadBatch(
            OffsetRange range,
            PayloadFormat payloadFormat,
            byte[] payload,
            List<SchemaRef> schemaRefs,
            Optional<ProjectionRef> projectionRef,
            ReadSourceRef source) {
        this(range, payloadFormat, payload, schemaRefs, projectionRef, source, -1, -1);
    }

    public ReadBatch(
            OffsetRange range,
            PayloadFormat payloadFormat,
            byte[] payload,
            List<SchemaRef> schemaRefs,
            Optional<ProjectionRef> projectionRef,
            ReadSourceRef source,
            long compatibilityObjectOffset,
            long compatibilityObjectLength) {
        this.range = Objects.requireNonNull(range, "range");
        this.payloadFormat = Objects.requireNonNull(payloadFormat, "payloadFormat");
        this.payload = Objects.requireNonNull(payload, "payload").clone();
        this.schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        this.projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        this.source = Objects.requireNonNull(source, "source");
        this.compatibilityObjectOffset = compatibilityObjectOffset;
        this.compatibilityObjectLength = compatibilityObjectLength;
        if (range.isEmpty()
                || range.startOffset() < source.resolvedRange().startOffset()
                || range.endOffset() > source.resolvedRange().endOffset()) {
            throw new IllegalArgumentException("batch range must be contained by its resolved source");
        }
        if ((compatibilityObjectOffset < 0) != (compatibilityObjectLength < 0)) {
            throw new IllegalArgumentException("Object compatibility range must be either complete or absent");
        }
        if (compatibilityObjectOffset >= 0) {
            try { Math.addExact(compatibilityObjectOffset, compatibilityObjectLength); }
            catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("Object compatibility range overflows", overflow);
            }
        }
    }

    public OffsetRange range() { return range; }
    public PayloadFormat payloadFormat() { return payloadFormat; }
    public byte[] payload() { return payload.clone(); }
    public List<SchemaRef> schemaRefs() { return schemaRefs; }
    public Optional<ProjectionRef> projectionRef() { return projectionRef; }
    public ReadSourceRef source() { return source; }

    /** Compatibility accessor for Object-only callers; provider-neutral code must use {@link #source()}. */
    @Deprecated(forRemoval = true)
    public EntryIndexRef entryIndexRef() {
        return objectTarget().entryIndexRef();
    }

    /** Compatibility accessor for Object-only callers; provider-neutral code must use {@link #source()}. */
    @Deprecated(forRemoval = true)
    public ObjectId sourceObjectId() {
        return objectTarget().objectId();
    }

    /** Compatibility accessor returning the exact physical subrange of this batch for Object sources. */
    @Deprecated(forRemoval = true)
    public long sourceObjectOffset() {
        ObjectSliceReadTarget target = objectTarget();
        return compatibilityObjectOffset >= 0 ? compatibilityObjectOffset : target.objectOffset();
    }

    /** Compatibility accessor returning the batch payload length for Object sources. */
    @Deprecated(forRemoval = true)
    public long sourceObjectLength() {
        objectTarget();
        return compatibilityObjectLength >= 0 ? compatibilityObjectLength : payload.length;
    }

    private ObjectSliceReadTarget objectTarget() {
        if (source.target() instanceof ObjectSliceReadTarget objectTarget) {
            return objectTarget;
        }
        throw new IllegalStateException("Object-only ReadBatch accessor used for " + source.target().type());
    }

    private static ReadSourceRef legacySource(
            OffsetRange range,
            ObjectId objectId,
            long objectOffset,
            long objectLength,
            EntryIndexRef entryIndexRef) {
        Objects.requireNonNull(objectId, "sourceObjectId");
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        ApiRangeValidation.requireNonNegativeNonOverflowingRange(objectOffset, objectLength, "source object");
        ObjectKey objectKey = entryIndexRef.objectKey().orElseGet(
                () -> new ObjectKey("legacy/" + objectId.value()));
        long compatibilityTargetLength = Math.max(1, objectLength);
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1, objectId, objectKey, ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1", "OPAQUE_SLICE", "legacy-" + objectId.value() + "-" + objectOffset,
                objectOffset, compatibilityTargetLength,
                new Checksum(ChecksumType.CRC32C, "00000000"), entryIndexRef);
        return new ReadSourceRef(range, 0, 1, target, ReadTargetIdentities.sha256(target));
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ReadBatch that
                && range.equals(that.range)
                && payloadFormat == that.payloadFormat
                && Arrays.equals(payload, that.payload)
                && schemaRefs.equals(that.schemaRefs)
                && projectionRef.equals(that.projectionRef)
                && source.equals(that.source);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(range, payloadFormat, schemaRefs, projectionRef, source)
                + Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return "ReadBatch[range=" + range + ", payloadFormat=" + payloadFormat
                + ", payloadBytes=" + payload.length + ", source=" + source + "]";
    }
}
