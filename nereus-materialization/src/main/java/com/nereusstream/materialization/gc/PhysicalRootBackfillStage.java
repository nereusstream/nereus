/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Stable machine stage attached to a bounded physical-root backfill failure. */
public enum PhysicalRootBackfillStage {
    REGISTRY_SCAN,
    PROJECTION_READ,
    HEAD_COMMIT_SCAN,
    GENERATION_ZERO_SCAN,
    CURSOR_INVENTORY_SCAN,
    OBJECT_HEAD,
    ROOT_WRITE,
    PROTECTION_WRITE,
    FINAL_REVALIDATION
}
