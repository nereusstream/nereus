/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

public enum PhysicalGcMarkStatus {
    DISABLED,
    DRY_RUN,
    NOT_YET_ELIGIBLE,
    DEADLINE_OVERFLOW,
    DOMAIN_BLOCKED,
    PLAN_CHANGED,
    ROOT_CHANGED,
    MARKED
}
