/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Durable retry/terminal classification; messages never drive transitions. */
public enum TaskFailureClass {
    NONE(0), RETRYABLE_METADATA(1), RETRYABLE_OBJECT_STORE(2), RETRYABLE_RESOURCE_LIMIT(3),
    SOURCE_CHANGED(4), SOURCE_RETIRED(5), UNSUPPORTED_MAPPING(6), OUTPUT_INVARIANT(7),
    CORRUPT_SOURCE(8), CLOSED(9);

    private final int wireId;

    TaskFailureClass(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static TaskFailureClass fromWireId(int wireId) {
        for (TaskFailureClass value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown task failure class wire id: " + wireId);
    }
}
