/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.objectstore.ObjectStore;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Closed NCP2 reader facade over the shared bounded Parquet transport. */
public final class ParquetRangedCompactedObjectReader implements RangedCompactedObjectReader {
    private final ParquetV2ReaderSupport support;

    public ParquetRangedCompactedObjectReader(ObjectStore objectStore, Executor readerExecutor) {
        support = new ParquetV2ReaderSupport(objectStore, readerExecutor);
    }

    @Override
    public CompletableFuture<RangedCompactedObjectReadResult> read(RangedCompactedObjectReadRequest request) {
        return support.readNcp2(Objects.requireNonNull(request, "request"));
    }
}
