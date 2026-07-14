/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Objects;

/** Authoritative cursor state returned after a mutation or idempotence proof. */
public record CursorMutationResult(CursorMutationOutcome outcome, CursorState state) {
    public CursorMutationResult {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(state, "state");
    }
}
