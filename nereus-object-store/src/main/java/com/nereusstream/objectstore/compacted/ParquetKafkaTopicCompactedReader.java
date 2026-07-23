/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.objectstore.ObjectStore;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Closed Kafka-specific NTC2 reader facade over the shared bounded Parquet transport. */
public final class ParquetKafkaTopicCompactedReader implements KafkaTopicCompactedObjectReader {
    private final ParquetV2ReaderSupport support;

    public ParquetKafkaTopicCompactedReader(ObjectStore objectStore, Executor readerExecutor) {
        support = new ParquetV2ReaderSupport(objectStore, readerExecutor);
    }

    @Override
    public CompletableFuture<KafkaTopicCompactedObjectReadResult> read(
            KafkaTopicCompactedObjectReadRequest request) {
        return support.readNtc2(Objects.requireNonNull(request, "request"));
    }
}
