/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

/** Non-destructive result of proving one candidate recovery-checkpoint prefix. */
public enum RecoveryCheckpointBuildStatus {
    READY,
    NO_LIVE_TAIL,
    TAIL_SCAN_LIMIT,
    NO_ELIGIBLE_PREFIX,
    MERGE_REQUIRED,
    REFERENCE_LIMIT
}
