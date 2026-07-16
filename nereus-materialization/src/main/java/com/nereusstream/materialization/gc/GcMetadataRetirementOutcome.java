/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Exact terminal observation for one journaled metadata-retirement action. */
public enum GcMetadataRetirementOutcome {
    RETIRED,
    ALREADY_ABSENT
}
