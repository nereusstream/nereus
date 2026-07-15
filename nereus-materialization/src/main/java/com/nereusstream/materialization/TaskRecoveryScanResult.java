/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.Map;
import java.util.Objects;

/** Immutable accounting for one complete per-stream task-prefix recovery scan. */
public record TaskRecoveryScanResult(
        int scannedTasks,
        Map<MaterializationTaskRecoveryAction, Integer> actions) {
    public TaskRecoveryScanResult {
        if (scannedTasks < 0) {
            throw new IllegalArgumentException("scannedTasks must be non-negative");
        }
        actions = Map.copyOf(Objects.requireNonNull(actions, "actions"));
        int total = actions.values().stream().mapToInt(Integer::intValue).sum();
        if (actions.values().stream().anyMatch(count -> count < 0) || total != scannedTasks) {
            throw new IllegalArgumentException("recovery action counts must equal scannedTasks");
        }
    }
}
