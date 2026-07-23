/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import java.util.concurrent.CompletableFuture;

/** Installs fresh recovered state under the partition lock without enabling writes; the coordinator revalidates next. */
@FunctionalInterface
public interface KafkaRecoveryPublisher<S> {
    CompletableFuture<Void> publish(KafkaRecoveredPartition<S> recovered);
}
