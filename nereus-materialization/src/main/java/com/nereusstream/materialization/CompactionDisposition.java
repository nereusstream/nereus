/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

/** Closed NTC1 row disposition registry; wire ids are durable and never enum ordinals. */
public enum CompactionDisposition {
    VALUE(1),
    TOMBSTONE(2);

    private final int wireId;

    CompactionDisposition(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static CompactionDisposition fromWireId(int wireId) {
        for (CompactionDisposition disposition : values()) {
            if (disposition.wireId == wireId) {
                return disposition;
            }
        }
        throw new IllegalArgumentException("unknown compaction disposition wire id: " + wireId);
    }
}
