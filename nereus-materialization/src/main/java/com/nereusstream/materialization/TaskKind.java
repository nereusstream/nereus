/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ReadView;

/** Closed durable materialization task family. */
public enum TaskKind {
    LOSSLESS_REWRITE(1),
    TOPIC_KEY_COMPACTION(2);

    private final int wireId;

    TaskKind(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    /** V1 always derives semantic compaction from the lossless committed source domain. */
    public ReadView sourceView() {
        return ReadView.COMMITTED;
    }

    public static TaskKind fromWireId(int wireId) {
        for (TaskKind kind : values()) {
            if (kind.wireId == wireId) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown task kind wire id: " + wireId);
    }
}
