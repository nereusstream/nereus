/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

/** Accepted first-satisfied plan close reasons in exact wire-code order. */
public enum Nwg1CloseReasonV1 {
    OBJECT_BODY_CAP(1),
    DIRECTORY_CAP(2),
    APPEND_UNIT_CAP(3),
    FRAME_CAP(4),
    EARLIEST_REQUEST_DEADLINE(5),
    HANDOFF(6),
    RUN_STOP(7),
    POLICY_CHANGE(8),
    RESOURCE_PRESSURE(9),
    EXPLICIT_FLUSH(10),
    TARGET_BYTES(11),
    LINGER_EXPIRED(12);

    private final int code;

    Nwg1CloseReasonV1(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static Nwg1CloseReasonV1 fromCode(int code) {
        for (Nwg1CloseReasonV1 value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new Nwg1ValidationException(
                Nwg1RejectionV1.UNKNOWN_CODE,
                Nwg1ValidationStageV1.HEADER_GRAMMAR,
                Nwg1IsolationScopeV1.SHARED_OBJECT,
                "unknown actual close reason");
    }
}
