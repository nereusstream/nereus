/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Closed lifecycle for one durable materialization task. */
public enum TaskLifecycle {
    PLANNED(1), CLAIMED(2), OUTPUT_READY(3), PUBLISHING(4), PUBLISHED(5),
    RETRY_WAIT(6), CANCELLED(7), TERMINAL_FAILED(8);

    private final int wireId;

    TaskLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static TaskLifecycle fromWireId(int wireId) {
        for (TaskLifecycle value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown task lifecycle wire id: " + wireId);
    }
}
