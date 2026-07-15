/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.SchemaRef;
import java.util.List;
import java.util.Objects;

/** Single-key publication truth for one higher-generation physical range. */
public record GenerationIndexRecord(
        int schemaVersion,
        String streamId,
        int readViewId,
        long offsetStart,
        long offsetEnd,
        long generation,
        String publicationId,
        String taskId,
        GenerationLifecycle lifecycle,
        String sourceSetSha256,
        String policySha256,
        ReadTargetRecord readTarget,
        String targetIdentitySha256,
        String materializationPolicySha256,
        String payloadFormat,
        int sourceRecordCount,
        int outputRecordCount,
        int entryCount,
        long logicalBytes,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        long firstCommitVersion,
        long lastCommitVersion,
        List<SchemaRef> schemaRefs,
        String projectionRef,
        long createdAtMillis,
        long committedAtMillis,
        String stateReason,
        long stateChangedAtMillis,
        long metadataVersion) {
    public GenerationIndexRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        streamId = F4RecordValidation.requireText(streamId, "streamId");
        if (readViewId != 1 && readViewId != 2) {
            throw new IllegalArgumentException("readViewId must identify a known view");
        }
        F4RecordValidation.requireDenseRange(offsetStart, offsetEnd, sourceRecordCount, "generation range");
        F4RecordValidation.requirePositive(generation, "generation");
        publicationId = F4RecordValidation.requireBase32Id(publicationId, "publicationId");
        taskId = F4RecordValidation.requireText(taskId, "taskId", 256, false);
        Objects.requireNonNull(lifecycle, "lifecycle");
        sourceSetSha256 = F4RecordValidation.requireSha256(sourceSetSha256, "sourceSetSha256");
        policySha256 = F4RecordValidation.requireSha256(policySha256, "policySha256");
        Objects.requireNonNull(readTarget, "readTarget");
        targetIdentitySha256 = F4RecordValidation.requireSha256(targetIdentitySha256, "targetIdentitySha256");
        materializationPolicySha256 = F4RecordValidation.requireSha256(
                materializationPolicySha256, "materializationPolicySha256");
        if (!policySha256.equals(materializationPolicySha256)) {
            throw new IllegalArgumentException("generation policy digests must agree");
        }
        payloadFormat = F4RecordValidation.requireText(payloadFormat, "payloadFormat");
        F4RecordValidation.requireNonNegative(outputRecordCount, "outputRecordCount");
        F4RecordValidation.requirePositive(entryCount, "entryCount");
        F4RecordValidation.requireNonNegative(logicalBytes, "logicalBytes");
        F4RecordValidation.requireNonNegative(cumulativeSizeAtStart, "cumulativeSizeAtStart");
        if (cumulativeSizeAtEnd < cumulativeSizeAtStart) {
            throw new IllegalArgumentException("generation cumulative sizes are not ordered");
        }
        F4RecordValidation.requirePositive(firstCommitVersion, "firstCommitVersion");
        if (lastCommitVersion < firstCommitVersion) {
            throw new IllegalArgumentException("generation commit versions are not ordered");
        }
        schemaRefs = F4RecordValidation.canonicalSchemaRefs(schemaRefs);
        projectionRef = F4RecordValidation.requireOptionalText(projectionRef, "projectionRef", 4096);
        F4RecordValidation.requireNonNegative(createdAtMillis, "createdAtMillis");
        F4RecordValidation.requireNonNegative(committedAtMillis, "committedAtMillis");
        stateReason = F4RecordValidation.requireOptionalText(
                stateReason, "stateReason", F4RecordValidation.MAX_REASON_BYTES);
        if (stateChangedAtMillis < createdAtMillis) {
            throw new IllegalArgumentException("state change cannot precede record creation");
        }
        F4RecordValidation.requireMetadataVersion(metadataVersion);
        if (readViewId == 1) {
            if (outputRecordCount != sourceRecordCount
                    || Math.subtractExact(cumulativeSizeAtEnd, cumulativeSizeAtStart) != logicalBytes) {
                throw new IllegalArgumentException("COMMITTED generation must be dense and byte-accounting exact");
            }
        } else if (outputRecordCount > sourceRecordCount) {
            throw new IllegalArgumentException("TOPIC_COMPACTED output cannot exceed source count");
        }
        boolean visible = lifecycle == GenerationLifecycle.COMMITTED
                || lifecycle == GenerationLifecycle.QUARANTINED
                || lifecycle == GenerationLifecycle.DRAINING
                || lifecycle == GenerationLifecycle.RETIRED;
        if (visible != (committedAtMillis >= createdAtMillis && committedAtMillis > 0)) {
            throw new IllegalArgumentException("committed timestamp does not match generation lifecycle");
        }
        boolean reasonRequired = lifecycle == GenerationLifecycle.QUARANTINED
                || lifecycle == GenerationLifecycle.DRAINING
                || lifecycle == GenerationLifecycle.RETIRED
                || lifecycle == GenerationLifecycle.ABORTED;
        if (reasonRequired == stateReason.isEmpty()) {
            throw new IllegalArgumentException("state reason does not match generation lifecycle");
        }
    }

    public GenerationIndexRecord withMetadataVersion(long version) {
        return new GenerationIndexRecord(
                schemaVersion, streamId, readViewId, offsetStart, offsetEnd, generation, publicationId,
                taskId, lifecycle, sourceSetSha256, policySha256, readTarget, targetIdentitySha256,
                materializationPolicySha256, payloadFormat, sourceRecordCount, outputRecordCount,
                entryCount, logicalBytes, cumulativeSizeAtStart, cumulativeSizeAtEnd, firstCommitVersion,
                lastCommitVersion, schemaRefs, projectionRef, createdAtMillis, committedAtMillis,
                stateReason, stateChangedAtMillis, version);
    }
}
