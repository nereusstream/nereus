/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.util.Objects;

/** Typed NWG1 validation failure; messages are intentionally not wire authority. */
public final class Nwg1ValidationException extends IllegalArgumentException {
    private final Nwg1RejectionV1 rejection;
    private final Nwg1ValidationStageV1 stage;
    private final Nwg1IsolationScopeV1 scope;

    public Nwg1ValidationException(
            Nwg1RejectionV1 rejection, Nwg1ValidationStageV1 stage, Nwg1IsolationScopeV1 scope, String message) {
        super(message);
        this.rejection = Objects.requireNonNull(rejection, "rejection");
        this.stage = Objects.requireNonNull(stage, "stage");
        this.scope = Objects.requireNonNull(scope, "scope");
    }

    public Nwg1ValidationException(
            Nwg1RejectionV1 rejection,
            Nwg1ValidationStageV1 stage,
            Nwg1IsolationScopeV1 scope,
            String message,
            Throwable cause) {
        super(message, cause);
        this.rejection = Objects.requireNonNull(rejection, "rejection");
        this.stage = Objects.requireNonNull(stage, "stage");
        this.scope = Objects.requireNonNull(scope, "scope");
    }

    public Nwg1RejectionV1 rejection() {
        return rejection;
    }

    public Nwg1ValidationStageV1 stage() {
        return stage;
    }

    public Nwg1IsolationScopeV1 scope() {
        return scope;
    }
}
