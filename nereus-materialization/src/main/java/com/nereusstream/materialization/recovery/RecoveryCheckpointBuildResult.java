/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import java.util.Objects;
import java.util.Optional;

/** Ready plan or an explicit conservative reason why no root mutation is legal. */
public record RecoveryCheckpointBuildResult(
        RecoveryCheckpointBuildStatus status,
        Optional<RecoveryCheckpointPlan> plan) {
    public RecoveryCheckpointBuildResult {
        Objects.requireNonNull(status, "status");
        plan = Objects.requireNonNull(plan, "plan");
        if ((status == RecoveryCheckpointBuildStatus.READY) != plan.isPresent()) {
            throw new IllegalArgumentException("only READY recovery builds may carry a plan");
        }
    }

    public static RecoveryCheckpointBuildResult ready(RecoveryCheckpointPlan plan) {
        return new RecoveryCheckpointBuildResult(
                RecoveryCheckpointBuildStatus.READY, Optional.of(plan));
    }

    public static RecoveryCheckpointBuildResult skipped(RecoveryCheckpointBuildStatus status) {
        if (status == RecoveryCheckpointBuildStatus.READY) {
            throw new IllegalArgumentException("READY requires a recovery checkpoint plan");
        }
        return new RecoveryCheckpointBuildResult(status, Optional.empty());
    }
}
