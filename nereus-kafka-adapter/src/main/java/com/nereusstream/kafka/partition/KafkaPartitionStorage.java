/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.partition;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.retention.KafkaPartitionMaintenance;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Nereus-side partition log boundary consumed by the Kafka fork.
 */
public interface KafkaPartitionStorage extends AutoCloseable {
    KafkaPartitionIdentity identity();

    int leaderEpoch();

    StorageProfile storageProfile();

    KafkaPartitionState state();

    KafkaStableSnapshot stableSnapshot();

    /**
     * Optional checkpoint-before-trim maintenance state machine for production native Kafka storage.
     */
    default Optional<KafkaPartitionMaintenance> maintenance() {
        return Optional.empty();
    }

    /**
     * Publish Kafka-derived HW/LSO after the stock state machine has consumed the exact stable append.
     *
     * <p>The durable end may advance before these derived offsets. Implementations keep the previous visibility bounds
     * and do not dispatch the next same-partition append or publish its stable event until this method confirms the
     * matching stable end.
     */
    default KafkaStableSnapshot publishDerivedOffsets(
            long expectedStableEndOffset, long highWatermark, long lastStableOffset) {
        throw new UnsupportedOperationException("Kafka partition storage does not support derived-offset publication");
    }

    /**
     * Publishes a product-confirmed durable trim into the partition snapshot before the Kafka fork
     * exposes the matching local log start.
     */
    default KafkaStableSnapshot publishDurableLogStart(long durableLogStartOffset) {
        throw new UnsupportedOperationException(
                "Kafka partition storage does not support durable log-start publication");
    }

    CompletableFuture<KafkaStableAppendResult> append(ByteBuffer validatedRecords, KafkaAppendContext context);

    CompletableFuture<KafkaStorageReadResult> read(KafkaStorageReadRequest request);

    /**
     * Probes the activated mandatory compacted prefix without falling back to COMMITTED bytes.
     *
     * <p>Kafka invokes this before electing an internal-topic coordinator. Implementations without
     * binding-rooted compacted-view support must fail closed.
     */
    default CompletableFuture<Void> probeMandatoryCompactedRead(Duration timeout) {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                false,
                "Kafka partition storage cannot probe a mandatory compacted read"));
    }

    KafkaPartitionEventSubscription subscribe(KafkaPartitionEventListener listener);

    CompletableFuture<Void> resign();

    @Override
    void close();
}
