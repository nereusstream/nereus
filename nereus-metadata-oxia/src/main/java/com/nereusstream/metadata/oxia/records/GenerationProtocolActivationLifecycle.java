/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Durable cluster-wide generation protocol activation lifecycle. */
public enum GenerationProtocolActivationLifecycle {
    PREPARED(1),
    ACTIVE(2);

    private final int wireId;

    GenerationProtocolActivationLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static GenerationProtocolActivationLifecycle fromWireId(int wireId) {
        return switch (wireId) {
            case 1 -> PREPARED;
            case 2 -> ACTIVE;
            default -> throw new IllegalArgumentException(
                    "unknown generation activation lifecycle wire id: " + wireId);
        };
    }
}
