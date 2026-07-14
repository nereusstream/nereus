/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/** Broker-ownership admission evidence retained by one writable managed-ledger instance. */
public final class NereusManagedLedgerOwnershipGuard {
    private final Supplier<CompletableFuture<Boolean>> checker;
    private final Duration timeout;
    private final boolean trustedDirect;

    private NereusManagedLedgerOwnershipGuard(
            Supplier<CompletableFuture<Boolean>> checker,
            Duration timeout,
            boolean trustedDirect) {
        this.checker = checker;
        this.timeout = requirePositive(timeout);
        this.trustedDirect = trustedDirect;
    }

    public static NereusManagedLedgerOwnershipGuard checked(
            Supplier<CompletableFuture<Boolean>> checker,
            Duration timeout) {
        return new NereusManagedLedgerOwnershipGuard(
                Objects.requireNonNull(checker, "checker"), timeout, false);
    }

    public static NereusManagedLedgerOwnershipGuard trustedDirect(Duration timeout) {
        return new NereusManagedLedgerOwnershipGuard(null, timeout, true);
    }

    public CompletableFuture<Void> requireOwned(String operation) {
        String exactOperation = requireOperation(operation);
        if (trustedDirect) {
            return CompletableFuture.completedFuture(null);
        }

        final CompletableFuture<Boolean> supplied;
        try {
            supplied = Objects.requireNonNull(checker.get(), "ownership checker returned null future");
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(fenced(exactOperation, error));
        }

        CompletableFuture<Boolean> bounded = supplied.thenApply(Boolean.TRUE::equals)
                .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
        return bounded.handle((owned, error) -> {
            if (error == null && Boolean.TRUE.equals(owned)) {
                return null;
            }
            Throwable cause = error == null
                    ? new IllegalStateException("ownership checker returned false or null")
                    : unwrap(error);
            throw new CompletionException(fenced(exactOperation, cause));
        });
    }

    public boolean isTrustedDirect() {
        return trustedDirect;
    }

    private static ManagedLedgerException.ManagedLedgerFencedException fenced(
            String operation,
            Throwable cause) {
        ManagedLedgerException.ManagedLedgerFencedException fenced =
                new ManagedLedgerException.ManagedLedgerFencedException(
                        "managed-ledger ownership check failed during " + operation);
        fenced.initCause(cause);
        return fenced;
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = Objects.requireNonNull(error, "error");
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String requireOperation(String operation) {
        Objects.requireNonNull(operation, "operation");
        if (operation.isBlank()) {
            throw new IllegalArgumentException("operation cannot be blank");
        }
        return operation;
    }

    private static Duration requirePositive(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        return timeout;
    }
}
