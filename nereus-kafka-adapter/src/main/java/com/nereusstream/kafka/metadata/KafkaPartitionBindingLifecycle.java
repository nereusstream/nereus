/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.metadata;

import java.util.concurrent.CompletableFuture;

/** Durable binding lifecycle used by the product-owned Kafka partition storage manager. */
public interface KafkaPartitionBindingLifecycle {
    CompletableFuture<KafkaPartitionBinding> ensureBinding(KafkaBindingRequest request);

    CompletableFuture<Void> delete(KafkaPartitionDeleteRequest request);
}
