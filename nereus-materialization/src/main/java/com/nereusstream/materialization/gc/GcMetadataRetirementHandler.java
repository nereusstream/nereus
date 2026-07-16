/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.materialization.MaterializationDeadline;
import java.util.concurrent.CompletableFuture;

/** Type-owned exact read/transition/delete implementation for one journaled metadata key family. */
public interface GcMetadataRetirementHandler {
    String removalType();

    CompletableFuture<GcMetadataRetirementOutcome> retire(
            GcMetadataRetirementContext context,
            GcPlannedMetadataRemoval removal,
            MaterializationDeadline deadline);
}
