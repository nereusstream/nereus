/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Reads one bounded exact COMMITTED page within the frozen half-open recovery range. */
@FunctionalInterface
public interface KafkaRecoveryBatchSource {
    CompletableFuture<KafkaRecoveryBatchPage> readCommittedPage(
            long startOffset, long endOffset, Duration timeout);
}
