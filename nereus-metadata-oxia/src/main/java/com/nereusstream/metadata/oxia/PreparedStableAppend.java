/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
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
        ObjectKeyHash objectKeyHash,
        boolean replayWasReachable) {
    public PreparedStableAppend {
        Objects.requireNonNull(request, "request");
        commitId = F4ValueValidation.text(commitId, "commitId");
        commitKey = F4ValueValidation.text(commitKey, "commitKey");
        F4ValueValidation.version(commitMetadataVersion);
        commitRecordSha256 = F4ValueValidation.sha256(commitRecordSha256, "commitRecordSha256");
        Objects.requireNonNull(objectKeyHash, "objectKeyHash");
        if (!commitId.equals(request.commitId())) {
            throw new IllegalArgumentException("commitId does not match the stable append request");
        }
        if (!(request.readTarget() instanceof ObjectSliceReadTarget target)
                || target.objectType() != ObjectType.MULTI_STREAM_WAL_OBJECT
                || !ObjectKeyHash.from(target.objectKey()).equals(objectKeyHash)) {
            throw new IllegalArgumentException("prepared stable append requires the exact Object WAL identity");
        }
    }
}
