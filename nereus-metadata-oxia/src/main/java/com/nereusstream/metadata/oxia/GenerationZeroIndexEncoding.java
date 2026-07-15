/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

/** Closed generation-zero durable record encoding family. */
public enum GenerationZeroIndexEncoding {
    LEGACY_OFFSET_INDEX_RECORD(1),
    GENERIC_OFFSET_INDEX_TARGET_RECORD(2);

    private final int wireId;

    GenerationZeroIndexEncoding(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }
}
