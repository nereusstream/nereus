/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import java.nio.ByteBuffer;

/** Blocking Kafka log append work executed outside request-handler threads. */
@FunctionalInterface
public interface KafkaAppendTask<T> {
    T execute(ByteBuffer ownedRecords) throws Exception;
}
