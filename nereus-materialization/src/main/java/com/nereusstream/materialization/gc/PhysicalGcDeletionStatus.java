/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

public enum PhysicalGcDeletionStatus {
    DISABLED,
    DRY_RUN,
    ROOT_CHANGED,
    DELETED,
    ALREADY_DELETED
}
