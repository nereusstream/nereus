/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.util.concurrent.CompletableFuture;

/** Strict Kafka-specific NTC2 sparse reader. */
public interface KafkaTopicCompactedObjectReader {
    CompletableFuture<KafkaTopicCompactedObjectReadResult> read(KafkaTopicCompactedObjectReadRequest request);
}
