/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import java.util.Objects;

/** Ordered outcome of one metadata-first root, registration, then inventory pass. */
public record PhysicalGcLifecyclePassResult(
        PhysicalObjectRootScanResult roots,
        StreamRegistrationRetirementScanResult registrations,
        ObjectInventoryScanResult inventory) {
    public PhysicalGcLifecyclePassResult {
        Objects.requireNonNull(roots, "roots");
        Objects.requireNonNull(registrations, "registrations");
        Objects.requireNonNull(inventory, "inventory");
    }
}
