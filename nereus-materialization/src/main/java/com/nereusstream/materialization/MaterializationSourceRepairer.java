/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.StreamId;
import java.util.concurrent.CompletableFuture;

/** Repairs primary generation facts before planning secondary materialization work. */
@FunctionalInterface
public interface MaterializationSourceRepairer {
    CompletableFuture<Void> repair(StreamId streamId);

    static MaterializationSourceRepairer noOp() {
        return ignored -> CompletableFuture.completedFuture(null);
    }
}
