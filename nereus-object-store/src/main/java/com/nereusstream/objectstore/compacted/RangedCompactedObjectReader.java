/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.util.concurrent.CompletableFuture;

/** Strict NCP2 ranged reader. */
public interface RangedCompactedObjectReader {
    CompletableFuture<RangedCompactedObjectReadResult> read(RangedCompactedObjectReadRequest request);
}
