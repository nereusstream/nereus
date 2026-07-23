/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import java.util.concurrent.CompletionStage;

/** Kafka-fork seam that supplies one current, internally consistent KRaft storage view. */
@FunctionalInterface
public interface KafkaStorageClusterSnapshotProvider {
    CompletionStage<KafkaStorageClusterSnapshot> currentSnapshot();
}
