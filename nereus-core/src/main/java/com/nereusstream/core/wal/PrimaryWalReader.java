/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.util.List;
import java.util.concurrent.CompletableFuture;
public interface PrimaryWalReader {
    ReadTargetType targetType();
    long reservationBytes(ResolvedRange range);
    CompletableFuture<WalReadResult> readWithStats(long startOffset, List<ResolvedRange> ranges, ReadOptions options);
}
