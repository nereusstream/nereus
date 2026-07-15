/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Durable root wrapper from which a process-local candidate was reconstructed. */
public enum GcCandidateRootState {
    ACTIVE_DISCOVERY,
    MARKED_RECOVERY
}
