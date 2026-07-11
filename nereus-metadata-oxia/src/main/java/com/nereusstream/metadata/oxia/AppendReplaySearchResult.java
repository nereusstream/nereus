/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;
import java.util.Objects;
import java.util.Optional;
public record AppendReplaySearchResult(AppendReplayStatus status,
        Optional<ReachableCommittedAppend> committedAppend, Optional<AppendReplayCursor> continuation,
        int scannedRecords) {
    public AppendReplaySearchResult { Objects.requireNonNull(status); committedAppend = Objects.requireNonNull(committedAppend);
        continuation = Objects.requireNonNull(continuation); if (scannedRecords < 0) throw new IllegalArgumentException("negative scanned records");
        if ((status == AppendReplayStatus.FOUND) != committedAppend.isPresent()
                || (status == AppendReplayStatus.CONTINUE) != continuation.isPresent())
            throw new IllegalArgumentException("illegal append replay result shape"); }
}
