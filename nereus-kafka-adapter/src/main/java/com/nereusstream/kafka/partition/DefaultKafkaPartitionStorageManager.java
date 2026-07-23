/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.metadata.KafkaBindingRequest;
import com.nereusstream.kafka.metadata.KafkaPartitionBinding;
import com.nereusstream.kafka.metadata.KafkaPartitionBindingLifecycle;
import com.nereusstream.kafka.metadata.KafkaPartitionDeleteRequest;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Binding-first partition manager; authority acquisition and fresh recovery remain owned by its opener. */
public final class DefaultKafkaPartitionStorageManager implements KafkaPartitionStorageManager {
    private final Object guard = new Object();
    private final KafkaPartitionBindingLifecycle bindings;
    private final KafkaPartitionLeaderManager leaders;
    private final Clock clock;
    private final String operationOwnerId;
    private final long operationOwnerEpoch;
    private final Duration operationTtl;
    private final Map<KafkaPartitionIdentity, OpenIntent> intents = new HashMap<>();
    private boolean closed;

    public DefaultKafkaPartitionStorageManager(
            KafkaPartitionBindingLifecycle bindings,
            KafkaPartitionOpener opener,
            Clock clock,
            String operationOwnerId,
            long operationOwnerEpoch,
            Duration operationTtl) {
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.leaders = new KafkaPartitionLeaderManager(Objects.requireNonNull(opener, "opener"));
        this.clock = Objects.requireNonNull(clock, "clock");
        this.operationOwnerId = Objects.requireNonNull(operationOwnerId, "operationOwnerId");
        this.operationTtl = positive(Objects.requireNonNull(operationTtl, "operationTtl"), "operationTtl");
        if (operationOwnerId.isBlank() || operationOwnerEpoch <= 0) {
            throw new IllegalArgumentException("Kafka partition manager owner must be nonblank and positive");
        }
        this.operationOwnerEpoch = operationOwnerEpoch;
    }

    @Override
    public CompletableFuture<KafkaPartitionStorage> openLeader(
            KafkaPartitionLeaderOpenRequest request) {
        Objects.requireNonNull(request, "request");
        KafkaStorageProfilePolicy policy = KafkaStorageProfilePolicy.forProfile(request.storageProfile());
        long deadline = deadline(request.timeout());
        OpenIntent intent = new OpenIntent(request, policy, deadline);
        OpenIntent superseded = null;
        synchronized (guard) {
            if (closed) return failedClosed();
            OpenIntent current = intents.get(request.identity());
            if (current != null) {
                KafkaLeaderAuthority.AuthorityRelation relation =
                        request.authority().relationTo(current.request.authority());
                if (relation == KafkaLeaderAuthority.AuthorityRelation.EXACT) {
                    if (!current.policy.equals(policy)) {
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_INVARIANT_VIOLATION,
                                false,
                                "Kafka leader open conflicts with the process-current storage profile"));
                    }
                    return current.result;
                }
                if (relation != KafkaLeaderAuthority.AuthorityRelation.DOMINATES) {
                    return CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.FENCED_APPEND,
                            false,
                            "Kafka leader open is stale or conflicts with the process-current term"));
                }
                superseded = current;
            }
            intents.put(request.identity(), intent);
        }
        if (superseded != null) {
            superseded.result.fail(new NereusException(
                    ErrorCode.FENCED_APPEND, false, "Kafka leader open was superseded by a newer term"));
        }
        beginOpen(intent);
        return intent.result;
    }

    private void beginOpen(OpenIntent intent) {
        KafkaBindingRequest bindingRequest = new KafkaBindingRequest(
                intent.request.identity(),
                intent.policy.storageProfile(),
                intent.request.metadataOffset(),
                operationOwnerId,
                operationOwnerEpoch,
                operationTtl);
        CompletableFuture<KafkaPartitionStorage> operation;
        try {
            operation = Objects.requireNonNull(
                    bindings.ensureBinding(bindingRequest), "Kafka binding future")
                    .thenCompose(binding -> openBound(intent, binding));
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }
        operation.whenComplete((storage, failure) -> completeOpen(intent, storage, failure));
    }

    private CompletableFuture<KafkaPartitionStorage> openBound(
            OpenIntent intent, KafkaPartitionBinding binding) {
        Duration remaining = remaining(intent.deadline);
        KafkaPartitionOpenPlan plan;
        try {
            plan = new KafkaPartitionOpenPlan(
                    intent.request.authority(), binding, intent.policy, remaining);
        } catch (IllegalArgumentException failure) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "Kafka binding does not match the requested leader open",
                    failure));
        }
        synchronized (guard) {
            if (closed || intents.get(intent.request.identity()) != intent) {
                return CompletableFuture.failedFuture(new NereusException(
                        closed ? ErrorCode.STORAGE_CLOSED : ErrorCode.FENCED_APPEND,
                        false,
                        closed
                                ? "Kafka partition storage manager is closed"
                                : "Kafka leader open was superseded before authority acquisition"));
            }
            return leaders.open(plan);
        }
    }

    private void completeOpen(
            OpenIntent intent, KafkaPartitionStorage storage, Throwable failure) {
        Throwable cause = failure == null ? null : unwrap(failure);
        synchronized (guard) {
            if (intents.get(intent.request.identity()) == intent && cause != null) {
                intents.remove(intent.request.identity());
            }
        }
        if (cause == null) intent.result.succeed(storage);
        else intent.result.fail(cause);
    }

    @Override
    public CompletableFuture<Void> resign(
            KafkaPartitionIdentity identity, int observedLeaderEpoch, Duration timeout) {
        Objects.requireNonNull(identity, "identity");
        positive(Objects.requireNonNull(timeout, "timeout"), "timeout");
        OpenIntent removed = removeIntent(identity, observedLeaderEpoch);
        if (removed != null) {
            removed.result.fail(new NereusException(
                    ErrorCode.FENCED_APPEND, false, "Kafka leader open was resigned before completion"));
        }
        return protectedResult(leaders.resign(identity, observedLeaderEpoch));
    }

    @Override
    public CompletableFuture<Void> delete(
            KafkaPartitionIdentity identity, long metadataOffset, Duration timeout) {
        Objects.requireNonNull(identity, "identity");
        Duration exactTimeout = positive(Objects.requireNonNull(timeout, "timeout"), "timeout");
        if (metadataOffset < 0) throw new IllegalArgumentException("metadataOffset must be non-negative");
        OpenIntent removed;
        synchronized (guard) {
            if (closed) return failedClosedVoid();
            removed = intents.remove(identity);
        }
        if (removed != null) {
            removed.result.fail(new NereusException(
                    ErrorCode.FENCED_APPEND, false, "Kafka leader open was removed by partition deletion"));
        }
        CompletableFuture<Void> operation = leaders.resign(identity, Integer.MAX_VALUE)
                .thenCompose(ignored -> {
                    synchronized (guard) {
                        if (closed) return failedClosedVoid();
                    }
                    return bindings.delete(new KafkaPartitionDeleteRequest(
                            identity,
                            metadataOffset,
                            operationOwnerId,
                            operationOwnerEpoch,
                            operationTtl,
                            exactTimeout));
                });
        return protectedResult(operation);
    }

    @Override
    public Optional<KafkaPartitionStorage> current(KafkaPartitionIdentity identity) {
        return leaders.current(Objects.requireNonNull(identity, "identity"));
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        List<OpenIntent> removed;
        synchronized (guard) {
            if (closed) return CompletableFuture.completedFuture(null);
            closed = true;
            removed = new ArrayList<>(intents.values());
            intents.clear();
        }
        for (OpenIntent intent : removed) {
            intent.result.fail(new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "Kafka partition storage manager is closed"));
        }
        return protectedResult(leaders.shutdown());
    }

    @Override
    public void close() {
        shutdown();
    }

    private long deadline(Duration timeout) {
        try {
            return Math.addExact(clock.millis(), positive(timeout, "timeout").toMillis());
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("Kafka partition open deadline overflows", failure);
        }
    }

    private Duration remaining(long deadline) {
        long millis;
        try {
            millis = Math.subtractExact(deadline, clock.millis());
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("Kafka partition open deadline underflows", failure);
        }
        if (millis <= 0) {
            throw new NereusException(ErrorCode.TIMEOUT, true, "Kafka partition binding/open deadline expired");
        }
        return Duration.ofMillis(millis);
    }

    private OpenIntent removeIntent(
            KafkaPartitionIdentity identity, int observedLeaderEpoch) {
        if (observedLeaderEpoch < 0) {
            throw new IllegalArgumentException("observedLeaderEpoch must be non-negative");
        }
        synchronized (guard) {
            OpenIntent current = intents.get(identity);
            if (current == null || current.request.leaderEpoch() > observedLeaderEpoch) return null;
            intents.remove(identity);
            return current;
        }
    }

    private static Duration positive(Duration value, String name) {
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private static <T> CompletableFuture<T> protectedResult(CompletableFuture<T> operation) {
        ManagerFuture<T> result = new ManagerFuture<>();
        operation.whenComplete((value, failure) -> {
            if (failure == null) result.succeed(value);
            else result.fail(unwrap(failure));
        });
        return result;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> CompletableFuture<T> failedClosed() {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.STORAGE_CLOSED, false, "Kafka partition storage manager is closed"));
    }

    private static CompletableFuture<Void> failedClosedVoid() {
        return failedClosed();
    }

    private static final class ManagerFuture<T> extends CompletableFuture<T> {
        private boolean succeed(T value) {
            return super.complete(value);
        }

        private boolean fail(Throwable failure) {
            return super.completeExceptionally(failure);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean complete(T value) {
            return false;
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            return false;
        }

        @Override
        public void obtrudeValue(T value) {
            throw new UnsupportedOperationException("Kafka partition manager completion is operation-owned");
        }

        @Override
        public void obtrudeException(Throwable failure) {
            throw new UnsupportedOperationException("Kafka partition manager completion is operation-owned");
        }
    }

    private static final class OpenIntent {
        private final KafkaPartitionLeaderOpenRequest request;
        private final KafkaStorageProfilePolicy policy;
        private final long deadline;
        private final ManagerFuture<KafkaPartitionStorage> result = new ManagerFuture<>();

        private OpenIntent(
                KafkaPartitionLeaderOpenRequest request,
                KafkaStorageProfilePolicy policy,
                long deadline) {
            this.request = request;
            this.policy = policy;
            this.deadline = deadline;
        }
    }
}
