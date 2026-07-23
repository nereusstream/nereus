/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import java.util.Objects;

/** One fresh Kafka-fork state codec and its exact short-critical-section publisher. */
public record KafkaRecoveryState<S>(
        KafkaRecoveryStateCodec<S> codec,
        KafkaRecoveryPublisher<S> publisher) {
    public KafkaRecoveryState {
        Objects.requireNonNull(codec, "codec");
        Objects.requireNonNull(publisher, "publisher");
    }
}
