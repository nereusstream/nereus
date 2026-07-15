/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import java.util.Objects;
import java.util.Optional;

/** One coordinator outcome: conservative skip or fully reconciled publication. */
public record RecoveryCheckpointRunResult(
        RecoveryCheckpointBuildStatus status,
        Optional<PublishedRecoveryCheckpoint> publication) {
    public RecoveryCheckpointRunResult {
        Objects.requireNonNull(status, "status");
        publication = Objects.requireNonNull(publication, "publication");
        if ((status == RecoveryCheckpointBuildStatus.READY) != publication.isPresent()) {
            throw new IllegalArgumentException(
                    "only a published READY checkpoint may carry publication facts");
        }
    }

    public static RecoveryCheckpointRunResult skipped(
            RecoveryCheckpointBuildStatus status) {
        if (status == RecoveryCheckpointBuildStatus.READY) {
            throw new IllegalArgumentException("READY requires a published checkpoint");
        }
        return new RecoveryCheckpointRunResult(status, Optional.empty());
    }

    public static RecoveryCheckpointRunResult published(
            PublishedRecoveryCheckpoint publication) {
        return new RecoveryCheckpointRunResult(
                RecoveryCheckpointBuildStatus.READY,
                Optional.of(publication));
    }
}
