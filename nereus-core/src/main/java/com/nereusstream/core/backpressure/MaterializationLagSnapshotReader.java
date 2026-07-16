/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.backpressure;

import com.nereusstream.api.StreamId;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Supplies exact lag evidence without making the append gate a metadata correctness owner. */
@FunctionalInterface
public interface MaterializationLagSnapshotReader {
    CompletableFuture<MaterializationLagSnapshot> measure(
            StreamId streamId,
            Duration timeout);
}
