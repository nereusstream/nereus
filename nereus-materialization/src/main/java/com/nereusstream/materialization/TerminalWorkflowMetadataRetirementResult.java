/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

/** Accounting for one bounded per-stream terminal workflow-metadata retirement pass. */
public record TerminalWorkflowMetadataRetirementResult(
        int tasksScanned,
        int tasksEligible,
        int tasksRetired,
        int protectionsReleased,
        int retentionStatsScanned,
        int retentionStatsRetired,
        int checkpointsScanned,
        int checkpointsRetired) {
    public TerminalWorkflowMetadataRetirementResult {
        if (tasksScanned < 0
                || tasksEligible < 0
                || tasksRetired < 0
                || protectionsReleased < 0
                || retentionStatsScanned < 0
                || retentionStatsRetired < 0
                || checkpointsScanned < 0
                || checkpointsRetired < 0
                || tasksEligible > tasksScanned
                || tasksRetired > tasksEligible
                || retentionStatsRetired > retentionStatsScanned
                || checkpointsRetired > checkpointsScanned) {
            throw new IllegalArgumentException(
                    "terminal workflow-metadata retirement accounting is invalid");
        }
    }

    public static TerminalWorkflowMetadataRetirementResult empty() {
        return new TerminalWorkflowMetadataRetirementResult(0, 0, 0, 0, 0, 0, 0, 0);
    }
}
