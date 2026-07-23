/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/** Streaming NCP2 writer. */
public interface RangedCompactedObjectWriter {
    CompletableFuture<RangedCompactedObjectWriteResult> write(
            RangedCompactedObjectWriteRequest request,
            Flow.Publisher<RangedCompactedObjectRow> rows);
}
