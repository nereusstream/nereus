/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

/** Accounting for one complete 64-shard registered-stream materialization pass. */
public record RegisteredMaterializationScanResult(
        int shardsScanned,
        int registrationsScanned,
        int registrationsAdmitted,
        int registrationsSkipped,
        int existingTasksRecovered,
        int plannedTasksConverged,
        int workflowMetadataRetired) {
    public RegisteredMaterializationScanResult {
        if (shardsScanned < 0
                || registrationsScanned < 0
                || registrationsAdmitted < 0
                || registrationsSkipped < 0
                || existingTasksRecovered < 0
                || plannedTasksConverged < 0
                || workflowMetadataRetired < 0
                || registrationsAdmitted + registrationsSkipped != registrationsScanned) {
            throw new IllegalArgumentException("registered materialization scan accounting is invalid");
        }
    }
}
