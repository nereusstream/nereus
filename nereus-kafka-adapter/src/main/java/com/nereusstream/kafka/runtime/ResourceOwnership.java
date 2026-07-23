/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

/** Whether the Kafka runtime must close a supplied dependency. */
public enum ResourceOwnership {
    OWNED,
    BORROWED
}
