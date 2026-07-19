/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PreparedStableAppend;

/** Canonical V1 protection identities shared by live publication and rollout backfill. */
public final class GenerationZeroProtectionIdentities {
    private GenerationZeroProtectionIdentities() {
    }

    static String reachableAppendReferenceId(PreparedStableAppend prepared) {
        if (prepared.request().readTarget() instanceof ObjectSliceReadTarget target) {
            return reachableAppendReferenceId(
                    prepared.request().streamId(),
                    prepared.commitId(),
                    ObjectKeyHash.from(target.objectKey()));
        }
        return reachableAppendReferenceId(
                prepared.request().streamId(),
                prepared.commitId(),
                prepared.primaryTargetIdentitySha256());
    }

    static String visibleGenerationReferenceId(MaterializedGenerationZero materialized) {
        return visibleGenerationReferenceId(
                materialized.committedAppend().streamId(),
                materialized.indexKey(),
                materialized.indexRecordSha256());
    }

    public static String reachableAppendReferenceId(
            com.nereusstream.api.StreamId streamId,
            String commitId,
            ObjectKeyHash objectKeyHash) {
        java.util.Objects.requireNonNull(streamId, "streamId");
        if (java.util.Objects.requireNonNull(commitId, "commitId").isBlank()) {
            throw new IllegalArgumentException("commitId cannot be blank");
        }
        java.util.Objects.requireNonNull(objectKeyHash, "objectKeyHash");
        return "ra1-" + DeterministicIds.stableHashComponent(
                streamId.value() + commitId + objectKeyHash.value());
    }

    public static String reachableAppendReferenceId(
            com.nereusstream.api.StreamId streamId,
            String commitId,
            com.nereusstream.api.Checksum targetIdentitySha256) {
        java.util.Objects.requireNonNull(streamId, "streamId");
        if (java.util.Objects.requireNonNull(commitId, "commitId").isBlank()) {
            throw new IllegalArgumentException("commitId cannot be blank");
        }
        java.util.Objects.requireNonNull(targetIdentitySha256, "targetIdentitySha256");
        if (targetIdentitySha256.type() != com.nereusstream.api.ChecksumType.SHA256) {
            throw new IllegalArgumentException("targetIdentitySha256 must use SHA256");
        }
        return "ra1-" + DeterministicIds.stableHashComponent(
                streamId.value() + commitId + targetIdentitySha256.value());
    }

    public static String visibleGenerationReferenceId(
            com.nereusstream.api.StreamId streamId,
            String indexKey,
            com.nereusstream.api.Checksum indexRecordSha256) {
        java.util.Objects.requireNonNull(streamId, "streamId");
        if (java.util.Objects.requireNonNull(indexKey, "indexKey").isBlank()) {
            throw new IllegalArgumentException("indexKey cannot be blank");
        }
        java.util.Objects.requireNonNull(indexRecordSha256, "indexRecordSha256");
        if (indexRecordSha256.type()
                != com.nereusstream.api.ChecksumType.SHA256) {
            throw new IllegalArgumentException(
                    "indexRecordSha256 must use SHA256");
        }
        return "vg0-" + DeterministicIds.stableHashComponent(
                streamId.value()
                        + indexKey
                        + indexRecordSha256.value());
    }

    static com.nereusstream.api.Checksum targetIdentity(CommittedAppend append) {
        return ReadTargetIdentities.sha256(append.readTarget());
    }

    @Deprecated(forRemoval = true)
    static ObjectKeyHash objectKeyHash(CommittedAppend append) {
        if (!(append.readTarget() instanceof ObjectSliceReadTarget target)) {
            throw new IllegalArgumentException("generation-zero append target must be an object slice");
        }
        return ObjectKeyHash.from(target.objectKey());
    }
}
