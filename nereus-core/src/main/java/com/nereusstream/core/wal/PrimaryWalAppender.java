/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.target.ReadTargetType;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
public interface PrimaryWalAppender<P extends PreparedPrimaryAppend> {
    ReadTargetType targetType();
    Class<P> preparedClass();
    P prepare(PrimaryAppendRequest request);
    CompletableFuture<DurablePrimaryAppend> persist(P prepared, Duration timeout);
    default CompletableFuture<Void> publishDurableMetadata(
            DurablePrimaryAppend append,
            AppendSession session,
            Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }
    CompletableFuture<Void> validateBeforeHeadCommit(DurablePrimaryAppend append, AppendSession session, Duration timeout);
}
