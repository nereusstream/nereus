/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Establishes exact permanent physical-reference vetoes around generation-zero visibility cuts. */
public interface GenerationZeroPhysicalReferencePublisher {
    CompletableFuture<ProtectedStableAppend> protectBeforeHead(
            PreparedStableAppend append,
            Duration timeout);

    CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
            MaterializedGenerationZero append,
            Duration timeout);
}
