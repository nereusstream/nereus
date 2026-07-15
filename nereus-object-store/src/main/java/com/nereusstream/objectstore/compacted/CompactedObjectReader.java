/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.util.concurrent.CompletableFuture;

/** Range-oriented strict NCP1/NTC1 reader. */
public interface CompactedObjectReader {
    CompletableFuture<CompactedObjectReadResult> read(CompactedObjectReadRequest request);
}
