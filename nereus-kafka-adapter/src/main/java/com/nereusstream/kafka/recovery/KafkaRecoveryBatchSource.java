/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Reads exact committed batch bytes for the frozen half-open recovery range. */
@FunctionalInterface
public interface KafkaRecoveryBatchSource {
    CompletableFuture<List<KafkaReplayBatch>> readCommitted(
            long startOffset, long endOffset, Duration timeout);
}
