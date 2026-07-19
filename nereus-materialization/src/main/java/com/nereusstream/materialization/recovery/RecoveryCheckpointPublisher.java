/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.api.StreamId;
import java.util.concurrent.CompletableFuture;

/** Process-shared recovery-checkpoint publication boundary used by registered-stream orchestration. */
@FunctionalInterface
public interface RecoveryCheckpointPublisher {
    CompletableFuture<RecoveryCheckpointRunResult> checkpoint(StreamId streamId);
}
