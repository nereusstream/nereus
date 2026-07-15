/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.StableAppendResult;
import java.util.concurrent.CompletableFuture;
public interface StableAppendCommitter {
    CompletableFuture<PreparedStableAppend> prepare(CommitAppendRequest request);
    CompletableFuture<StableAppendResult> commit(ProtectedStableAppend append);
}
