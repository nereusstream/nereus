/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/** Streaming Kafka-specific NTC2 writer. */
public interface KafkaTopicCompactedObjectWriter {
    CompletableFuture<RangedCompactedObjectWriteResult> write(
            KafkaTopicCompactedObjectWriteRequest request,
            Flow.Publisher<KafkaTopicCompactedObjectRow> rows);
}
