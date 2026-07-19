/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Applies one selected mutation, then loses only its response for deterministic recovery contracts. */
public final class ResponseLossPartitionedOxiaBackend implements PartitionedOxiaClient.Backend {
    private final InMemoryPartitionedOxiaBackend delegate = new InMemoryPartitionedOxiaBackend();
    private Operation armed;

    public synchronized void loseNextResponse(Operation operation) {
        if (armed != null) {
            throw new IllegalStateException("one response-loss injection is already armed");
        }
        armed = operation;
    }

    @Override
    public CompletableFuture<Optional<PartitionedOxiaClient.VersionedValue>> get(
            String key, PartitionKey partitionKey) {
        return delegate.get(key, partitionKey);
    }

    @Override
    public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfAbsent(
            String key, byte[] value, PartitionKey partitionKey) {
        return loseAfterApply(Operation.PUT_IF_ABSENT, delegate.putIfAbsent(key, value, partitionKey));
    }

    @Override
    public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfVersion(
            String key, byte[] value, long expectedVersion, PartitionKey partitionKey) {
        return loseAfterApply(
                Operation.PUT_IF_VERSION,
                delegate.putIfVersion(key, value, expectedVersion, partitionKey));
    }

    @Override
    public CompletableFuture<Void> deleteIfVersion(
            String key, long expectedVersion, PartitionKey partitionKey) {
        return loseAfterApply(
                Operation.DELETE_IF_VERSION,
                delegate.deleteIfVersion(key, expectedVersion, partitionKey));
    }

    @Override
    public CompletableFuture<List<String>> list(
            String fromInclusive, String toExclusive, PartitionKey partitionKey) {
        return delegate.list(fromInclusive, toExclusive, partitionKey);
    }

    @Override
    public CompletableFuture<List<PartitionedOxiaClient.VersionedValue>> rangeScan(
            String fromInclusive, String toExclusive, int limit, PartitionKey partitionKey) {
        return delegate.rangeScan(fromInclusive, toExclusive, limit, partitionKey);
    }

    @Override
    public WatchRegistration watchPrefix(
            String prefix, PartitionKey partitionKey, Runnable invalidationCallback) {
        return delegate.watchPrefix(prefix, partitionKey, invalidationCallback);
    }

    private synchronized <T> CompletableFuture<T> loseAfterApply(
            Operation operation, CompletableFuture<T> applied) {
        if (armed != operation) {
            return applied;
        }
        armed = null;
        return applied.thenCompose(ignored -> CompletableFuture.failedFuture(
                new InjectedResponseLossException(operation + " response was lost after apply")));
    }

    public enum Operation {
        PUT_IF_ABSENT,
        PUT_IF_VERSION,
        DELETE_IF_VERSION
    }

    public static final class InjectedResponseLossException extends RuntimeException {
        private InjectedResponseLossException(String message) {
            super(message);
        }
    }
}
