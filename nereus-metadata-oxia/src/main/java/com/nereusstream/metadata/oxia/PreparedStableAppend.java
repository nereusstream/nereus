/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.util.Objects;

/** Exact immutable generic commit intent prepared without advancing the stream head. */
public record PreparedStableAppend(
        CommitAppendRequest request,
        String commitId,
        String commitKey,
        long commitMetadataVersion,
        Checksum commitRecordSha256,
        Checksum primaryTargetIdentitySha256,
        boolean replayWasReachable) {
    public PreparedStableAppend {
        Objects.requireNonNull(request, "request");
        commitId = F4ValueValidation.text(commitId, "commitId");
        commitKey = F4ValueValidation.text(commitKey, "commitKey");
        F4ValueValidation.version(commitMetadataVersion);
        commitRecordSha256 = F4ValueValidation.sha256(commitRecordSha256, "commitRecordSha256");
        primaryTargetIdentitySha256 = F4ValueValidation.sha256(
                primaryTargetIdentitySha256,
                "primaryTargetIdentitySha256");
        if (!commitId.equals(request.commitId())) {
            throw new IllegalArgumentException("commitId does not match the stable append request");
        }
        if (!ReadTargetIdentities.sha256(request.readTarget()).equals(primaryTargetIdentitySha256)) {
            throw new IllegalArgumentException("prepared stable append target identity does not match its request");
        }
    }

    /** Source-compatible Object-WAL constructor retained while callers migrate to canonical target identities. */
    @Deprecated(forRemoval = true)
    public PreparedStableAppend(
            CommitAppendRequest request,
            String commitId,
            String commitKey,
            long commitMetadataVersion,
            Checksum commitRecordSha256,
            ObjectKeyHash objectKeyHash,
            boolean replayWasReachable) {
        this(
                request,
                commitId,
                commitKey,
                commitMetadataVersion,
                commitRecordSha256,
                ReadTargetIdentities.sha256(request.readTarget()),
                replayWasReachable);
        if (!(request.readTarget() instanceof ObjectSliceReadTarget target)
                || target.objectType() != ObjectType.MULTI_STREAM_WAL_OBJECT
                || !ObjectKeyHash.from(target.objectKey()).equals(objectKeyHash)) {
            throw new IllegalArgumentException("legacy prepared append requires the exact Object WAL identity");
        }
    }

    /** Object-only compatibility accessor. Provider-neutral code uses {@link #primaryTargetIdentitySha256()}. */
    @Deprecated(forRemoval = true)
    public ObjectKeyHash objectKeyHash() {
        if (!(request.readTarget() instanceof ObjectSliceReadTarget target)
                || target.objectType() != ObjectType.MULTI_STREAM_WAL_OBJECT) {
            throw new IllegalStateException("prepared append target is not an Object WAL slice");
        }
        return ObjectKeyHash.from(target.objectKey());
    }
}
