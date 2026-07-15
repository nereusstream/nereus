/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

public enum PhysicalGcAdvanceStatus {
    DISABLED,
    DRY_RUN,
    WAITING_FOR_GRACE,
    WAITING_FOR_READERS,
    PLAN_DRIFT_UNMARKED,
    ROOT_CHANGED,
    DELETE_INTENT
}
