/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Outcome of one bounded stream-registration retirement attempt. */
public enum StreamRegistrationRetirementStatus {
    DISABLED,
    DRY_RUN,
    ALREADY_ABSENT,
    RETIRED,
    STREAM_NOT_DELETED,
    PROJECTION_LIVE,
    EXTERNAL_REFERENCE,
    TASK_NOT_TERMINAL,
    INDEX_STILL_LIVE,
    RECOVERY_TAIL_PRESENT,
    AUDIT_GRACE_PENDING,
    LIMIT_EXCEEDED,
    VERSION_CHANGED
}
