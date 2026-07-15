/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Verified publish-time/size statistics for one immutable committed range. */
public record RangeRetentionStatsRecord(
        int schemaVersion,
        String streamId,
        long offsetStart,
        long offsetEnd,
        long commitVersion,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        long minPublishTimeMillis,
        long maxPublishTimeMillis,
        String sourceIndexKey,
        String sourceIndexIdentitySha256,
        long sourceIndexMetadataVersion,
        String verifierBuild,
        long verifiedAtMillis,
        long metadataVersion) {
    public RangeRetentionStatsRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        streamId = F4RecordValidation.requireText(streamId, "streamId");
        F4RecordValidation.requireRange(offsetStart, offsetEnd, "retention range");
        F4RecordValidation.requirePositive(commitVersion, "commitVersion");
        F4RecordValidation.requireNonNegative(cumulativeSizeAtStart, "cumulativeSizeAtStart");
        if (cumulativeSizeAtEnd < cumulativeSizeAtStart) {
            throw new IllegalArgumentException("cumulative sizes are not ordered");
        }
        F4RecordValidation.requireNonNegative(minPublishTimeMillis, "minPublishTimeMillis");
        if (maxPublishTimeMillis < minPublishTimeMillis) {
            throw new IllegalArgumentException("publish times are not ordered");
        }
        sourceIndexKey = F4RecordValidation.requireText(sourceIndexKey, "sourceIndexKey");
        sourceIndexIdentitySha256 = F4RecordValidation.requireSha256(
                sourceIndexIdentitySha256, "sourceIndexIdentitySha256");
        F4RecordValidation.requireMetadataVersion(sourceIndexMetadataVersion);
        verifierBuild = F4RecordValidation.requireText(verifierBuild, "verifierBuild");
        F4RecordValidation.requireNonNegative(verifiedAtMillis, "verifiedAtMillis");
        F4RecordValidation.requireMetadataVersion(metadataVersion);
    }

    public RangeRetentionStatsRecord withMetadataVersion(long version) {
        return new RangeRetentionStatsRecord(
                schemaVersion, streamId, offsetStart, offsetEnd, commitVersion, cumulativeSizeAtStart,
                cumulativeSizeAtEnd, minPublishTimeMillis, maxPublishTimeMillis, sourceIndexKey,
                sourceIndexIdentitySha256, sourceIndexMetadataVersion, verifierBuild, verifiedAtMillis, version);
    }
}
