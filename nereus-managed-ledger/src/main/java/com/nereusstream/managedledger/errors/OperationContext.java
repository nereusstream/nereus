/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.errors;

import com.nereusstream.api.StreamState;
import java.util.Objects;
import java.util.Optional;

public record OperationContext(
        String operation,
        boolean factoryOperation,
        boolean directRead,
        Optional<StreamState> observedStreamState) {
    public OperationContext {
        Objects.requireNonNull(operation, "operation");
        if (operation.isBlank()) {
            throw new IllegalArgumentException("operation cannot be blank");
        }
        observedStreamState = Objects.requireNonNull(observedStreamState, "observedStreamState");
    }

    public static OperationContext ledger(String operation) {
        return new OperationContext(operation, false, false, Optional.empty());
    }
}
