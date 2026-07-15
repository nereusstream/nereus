/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Exact-format physical reader used after generation resolution. */
public interface ReadTargetReader extends AutoCloseable {
    ReadTargetReaderKey key();

    long reservationBytes(ResolvedRange range);

    CompletableFuture<WalReadResult> readWithStats(
            StreamId streamId,
            long startOffset,
            List<ResolvedRange> ranges,
            ReadOptions options);

    @Override
    default void close() {
        // Most adapters borrow a process-owned reader. Stateful implementations may override.
    }
}
