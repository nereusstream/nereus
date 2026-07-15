/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/** Streaming protocol-neutral NCP1/NTC1 writer. */
public interface CompactedObjectWriter {
    CompletableFuture<CompactedObjectWriteResult> write(
            CompactedObjectWriteRequest request,
            Flow.Publisher<CompactedObjectRow> rows);
}
