/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import com.nereusstream.metadata.oxia.AppendReplayStatus;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import java.util.Objects;
import java.util.Optional;

/** Terminal bounded replay proof from either the live tail or the current NRC1 root. */
public record AppendReplayResolution(
        AppendReplayStatus status,
        Optional<ReachableCommittedAppend> committedAppend,
        Optional<AppendReplayEvidenceSource> evidenceSource,
        int scannedLiveCommits) {
    public AppendReplayResolution {
        Objects.requireNonNull(status, "status");
        committedAppend = Objects.requireNonNull(committedAppend, "committedAppend");
        evidenceSource = Objects.requireNonNull(evidenceSource, "evidenceSource");
        if (status == AppendReplayStatus.CONTINUE
                || scannedLiveCommits < 0
                || (status == AppendReplayStatus.FOUND) != committedAppend.isPresent()
                || committedAppend.isPresent() != evidenceSource.isPresent()) {
            throw new IllegalArgumentException("append replay resolution shape is invalid");
        }
    }

    public static AppendReplayResolution found(
            ReachableCommittedAppend append,
            AppendReplayEvidenceSource source,
            int scannedLiveCommits) {
        return new AppendReplayResolution(
                AppendReplayStatus.FOUND,
                Optional.of(Objects.requireNonNull(append, "append")),
                Optional.of(Objects.requireNonNull(source, "source")),
                scannedLiveCommits);
    }

    public static AppendReplayResolution notCommitted(int scannedLiveCommits) {
        return new AppendReplayResolution(
                AppendReplayStatus.PROVEN_NOT_COMMITTED,
                Optional.empty(),
                Optional.empty(),
                scannedLiveCommits);
    }
}
