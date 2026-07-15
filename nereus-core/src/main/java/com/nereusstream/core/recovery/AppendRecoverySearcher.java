/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import com.nereusstream.metadata.oxia.CommitAppendRequest;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Bounded terminal exact-append search used by retained-attempt recovery. */
@FunctionalInterface
public interface AppendRecoverySearcher {
    CompletableFuture<AppendReplayResolution> search(
            CommitAppendRequest request,
            int maximumLiveCommits,
            int pageSize,
            Duration timeout);
}
