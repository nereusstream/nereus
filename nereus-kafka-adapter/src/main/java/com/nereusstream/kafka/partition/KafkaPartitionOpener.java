/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import java.util.concurrent.CompletableFuture;

/** Acquires authority, performs fresh recovery, and returns an unopened-to-callers leader storage instance. */
@FunctionalInterface
public interface KafkaPartitionOpener {
    CompletableFuture<KafkaPartitionStorage> open(KafkaLeaderAuthority authority);
}
