/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Strictly verified immutable worker output, independent of the generation later assigned to it. */
public record MaterializationOutput(
        String taskId,
        StreamId streamId,
        ReadView view,
        OffsetRange coverage,
        String outputAttemptId,
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectKeyHash objectKeyHash,
        long objectLength,
        Checksum storageCrc32c,
        Checksum contentSha256,
        String etag,
        String physicalFormat,
        String logicalFormat,
        ReadTarget readTarget,
        Checksum targetIdentitySha256,
        EntryIndexRef entryIndexRef,
        int sourceRecordCount,
        int outputRecordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        Checksum sourceSetSha256,
        Optional<ProjectionRef> projectionRef) {
    public MaterializationOutput {
        taskId = requireText(taskId, "taskId");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(coverage, "coverage");
        if (coverage.isEmpty()) {
            throw new IllegalArgumentException("output coverage cannot be empty");
        }
        outputAttemptId = requireBase32(outputAttemptId, "outputAttemptId");
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(objectKeyHash, "objectKeyHash");
        if (!objectKeyHash.equals(ObjectKeyHash.from(objectKey)) || objectLength <= 0) {
            throw new IllegalArgumentException("output object key hash/length is invalid");
        }
        requireChecksum(storageCrc32c, ChecksumType.CRC32C, "storageCrc32c");
        requireChecksum(contentSha256, ChecksumType.SHA256, "contentSha256");
        etag = Objects.requireNonNull(etag, "etag");
        physicalFormat = requireText(physicalFormat, "physicalFormat");
        logicalFormat = requireText(logicalFormat, "logicalFormat");
        try {
            PayloadFormat.valueOf(logicalFormat);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("logicalFormat must identify a PayloadFormat", failure);
        }
        Objects.requireNonNull(readTarget, "readTarget");
        requireChecksum(targetIdentitySha256, ChecksumType.SHA256, "targetIdentitySha256");
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        if (!(readTarget instanceof ObjectSliceReadTarget target)
                || target.objectType() != ObjectType.STREAM_COMPACTED_OBJECT
                || !target.objectId().equals(objectId)
                || !target.objectKey().equals(objectKey)
                || target.objectOffset() != 0
                || target.objectLength() != objectLength
                || !target.physicalFormat().equals(physicalFormat)
                || !target.logicalFormat().equals(logicalFormat)
                || !target.entryIndexRef().equals(entryIndexRef)) {
            throw new IllegalArgumentException("output read target does not match immutable object fields");
        }
        String targetIdentity = ReadTargetCodecRegistry.phase15()
                .encode(readTarget)
                .identityChecksumValue();
        if (!targetIdentitySha256.value().equals(targetIdentity)) {
            throw new IllegalArgumentException("target identity does not match canonical read-target bytes");
        }
        if (sourceRecordCount <= 0 || sourceRecordCount != coverage.recordCount()
                || outputRecordCount < 0 || outputRecordCount > sourceRecordCount
                || entryCount <= 0 || logicalBytes < 0
                || cumulativeSizeAtStart < 0
                || cumulativeSizeAtEnd < cumulativeSizeAtStart) {
            throw new IllegalArgumentException("output accounting is invalid");
        }
        if (view == ReadView.COMMITTED
                && (outputRecordCount != sourceRecordCount
                        || Math.subtractExact(cumulativeSizeAtEnd, cumulativeSizeAtStart) != logicalBytes)) {
            throw new IllegalArgumentException("committed output must be dense and byte-accounting exact");
        }
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        requireChecksum(sourceSetSha256, ChecksumType.SHA256, "sourceSetSha256");
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
    }

    private static void requireChecksum(
            Checksum checksum,
            ChecksumType expected,
            String field) {
        Objects.requireNonNull(checksum, field);
        if (checksum.type() != expected) {
            throw new IllegalArgumentException(field + " must use " + expected);
        }
    }

    private static String requireBase32(String value, String field) {
        value = requireText(value, field);
        if (value.length() < 26 || value.length() > 128) {
            throw new IllegalArgumentException(field + " must encode at least 128 bits");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z')
                    || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException(field + " must be lowercase base32 without padding");
            }
        }
        return value;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
