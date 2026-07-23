/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

/** Fail-closed NKC1 structural or integrity error. */
public final class KafkaCheckpointFormatException extends IllegalArgumentException {
    public KafkaCheckpointFormatException(String message) {
        super(message);
    }

    public KafkaCheckpointFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
