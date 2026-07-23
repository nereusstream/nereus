/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.checkpoint;

import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import java.util.concurrent.CompletableFuture;

/** Reloads current stream-head facts and proves a captured commit remains reachable. */
public interface KafkaCheckpointSourceValidator {
    CompletableFuture<KafkaCheckpointSourceState> loadCurrent();

    CompletableFuture<Boolean> isSourceCommitReachable(
            KafkaCheckpointHeader captured,
            KafkaCheckpointSourceState current);
}
