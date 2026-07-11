/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.target.ReadTargetType;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
public interface PrimaryWalAppender<P extends PreparedPrimaryAppend> {
    ReadTargetType targetType();
    P prepare(PrimaryAppendRequest request);
    CompletableFuture<DurablePrimaryAppend> persist(P prepared, Duration timeout);
    CompletableFuture<Void> validateBeforeHeadCommit(DurablePrimaryAppend append, AppendSession session, Duration timeout);
}
