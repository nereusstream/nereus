/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.materialization;

/**
 * Authority shape admitted by a materialization runtime.
 */
public enum MaterializationStreamAuthorityMode {
    /**
     * Existing Pulsar/F4 path: every source and output is rooted in one live projection.
     */
    PROJECTION_REQUIRED,

    /**
     * Native protocol path: source and output projection hints stay empty.
     */
    DIRECT_STREAM,

    /**
     * Kafka topic-compaction path: object-materialization profiles use the normal direct-stream
     * registration, while {@code BOOKKEEPER_WAL_ONLY} is rooted directly in the live L0 stream
     * authority and must not acquire an F4 materialization registration.
     */
    KAFKA_TOPIC_COMPACTION
}
