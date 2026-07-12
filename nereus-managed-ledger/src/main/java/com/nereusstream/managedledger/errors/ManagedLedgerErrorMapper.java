/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.errors;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamState;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/** Stable translation from provider-neutral L0 errors to locked Pulsar managed-ledger channels. */
public final class ManagedLedgerErrorMapper {
    public static final String UNSUPPORTED_PREFIX = "NEREUS_UNSUPPORTED_OPERATION:";

    public ManagedLedgerException map(Throwable error, OperationContext context) {
        Objects.requireNonNull(context, "context");
        Throwable cause = unwrap(Objects.requireNonNull(error, "error"));
        if (cause instanceof ManagedLedgerException managedLedger) {
            return managedLedger;
        }
        if (!(cause instanceof NereusException nereus)) {
            return new ManagedLedgerException("Nereus operation failed: " + context.operation(), cause);
        }
        ErrorCode code = nereus.code();
        if (code == ErrorCode.STREAM_NOT_FOUND || code == ErrorCode.OBJECT_NOT_FOUND) {
            return new ManagedLedgerException.ManagedLedgerNotFoundException(nereus.getMessage());
        }
        if (code == ErrorCode.BACKPRESSURE_REJECTED) {
            return new ManagedLedgerException.TooManyRequestsException(nereus.getMessage());
        }
        if (code == ErrorCode.STORAGE_CLOSED) {
            return context.factoryOperation()
                    ? new ManagedLedgerException.ManagedLedgerFactoryClosedException(nereus)
                    : new ManagedLedgerException.ManagedLedgerAlreadyClosedException(nereus.getMessage());
        }
        if (code == ErrorCode.FENCED_APPEND || code == ErrorCode.APPEND_SESSION_EXPIRED) {
            return new ManagedLedgerException.ManagedLedgerFencedException(nereus);
        }
        if (code == ErrorCode.STREAM_NOT_ACTIVE) {
            StreamState observed = context.observedStreamState().orElse(null);
            if (observed == StreamState.SEALED) {
                return new ManagedLedgerException.ManagedLedgerTerminatedException(nereus.getMessage());
            }
            if (observed == StreamState.DELETING || observed == StreamState.DELETED) {
                return new ManagedLedgerException.ManagedLedgerAlreadyClosedException(nereus.getMessage());
            }
        }
        if (code == ErrorCode.OFFSET_TRIMMED
                || (code == ErrorCode.OFFSET_NOT_AVAILABLE && context.directRead())) {
            return new ManagedLedgerException.InvalidCursorPositionException(nereus.getMessage());
        }
        if (code == ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND
                || code == ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH
                || code == ErrorCode.OBJECT_CHECKSUM_MISMATCH) {
            ManagedLedgerException.NonRecoverableLedgerException mapped =
                    new ManagedLedgerException.NonRecoverableLedgerException(nereus.getMessage());
            mapped.initCause(nereus);
            return mapped;
        }
        if (code == ErrorCode.UNSUPPORTED_READ_TARGET
                || code == ErrorCode.UNSUPPORTED_STORAGE_PROFILE
                || code == ErrorCode.UNSUPPORTED_DURABILITY_LEVEL
                || code == ErrorCode.UNSUPPORTED_FORMAT) {
            return unsupported(context.operation());
        }
        return new ManagedLedgerException(
                "Nereus " + context.operation() + " failed [" + code + ", retriable=" + nereus.retriable() + "]",
                nereus);
    }

    public ManagedLedgerException unsupported(String operation) {
        Objects.requireNonNull(operation, "operation");
        return new ManagedLedgerException(UNSUPPORTED_PREFIX + operation);
    }

    public UnsupportedOperationException unsupportedRuntime(String operation) {
        Objects.requireNonNull(operation, "operation");
        return new UnsupportedOperationException(UNSUPPORTED_PREFIX + operation);
    }

    public <T> CompletableFuture<T> failedFuture(ManagedLedgerException error) {
        return CompletableFuture.failedFuture(Objects.requireNonNull(error, "error"));
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
