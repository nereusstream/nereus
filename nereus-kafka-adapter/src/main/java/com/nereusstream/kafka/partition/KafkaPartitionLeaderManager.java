/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.partition;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Process-owned authority arbiter for Kafka partition leader instances. Durable authority acquisition remains inside
 * the opener; this class prevents stale asynchronous opens and resign callbacks from becoming process-current.
 */
public final class KafkaPartitionLeaderManager implements AutoCloseable {
    private final Object guard = new Object();
    private final KafkaPartitionOpener opener;
    private final Map<KafkaPartitionIdentity, Slot> slots = new HashMap<>();
    private boolean closed;

    public KafkaPartitionLeaderManager(KafkaPartitionOpener opener) {
        this.opener = Objects.requireNonNull(opener, "opener");
    }

    public CompletableFuture<KafkaPartitionStorage> open(KafkaPartitionOpenPlan plan) {
        Objects.requireNonNull(plan, "plan");
        KafkaLeaderAuthority authority = plan.authority();
        KafkaPartitionStorage superseded = null;
        OpenAttempt attempt;
        synchronized (guard) {
            if (closed) return failedClosed();
            Slot slot = slots.computeIfAbsent(authority.identity(), ignored -> new Slot());
            if (slot.desired != null) {
                KafkaLeaderAuthority.AuthorityRelation relation = authority.relationTo(slot.desired.authority());
                if (relation == KafkaLeaderAuthority.AuthorityRelation.EXACT) {
                    if (!plan.compatibleWith(slot.desired)) {
                        return CompletableFuture.failedFuture(invariant(
                                "Kafka leader open conflicts with the process-current binding or profile"));
                    }
                    if (slot.opening != null) return slot.opening.result;
                    if (slot.installed != null) {
                        return CompletableFuture.completedFuture(slot.installed.storage);
                    }
                } else if (relation != KafkaLeaderAuthority.AuthorityRelation.DOMINATES) {
                    return CompletableFuture.failedFuture(fenced(
                            "Kafka leader authority is stale or conflicts with the process-current term"));
                }
            }
            slot.desired = plan;
            if (slot.installed != null) {
                superseded = slot.installed.storage;
                slot.installed = null;
            }
            attempt = new OpenAttempt(plan);
            slot.opening = attempt;
        }
        if (superseded != null) safeResign(superseded);
        beginOpen(attempt);
        return attempt.result;
    }

    /** A stale follower/resign notification is a no-op and can never close a newer installed leader. */
    public CompletableFuture<Void> resign(KafkaLeaderAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        KafkaPartitionStorage storage = null;
        synchronized (guard) {
            Slot slot = slots.get(authority.identity());
            if (slot == null || !authority.equals(slot.desired.authority())) {
                return CompletableFuture.completedFuture(null);
            }
            slots.remove(authority.identity());
            if (slot.installed != null) storage = slot.installed.storage;
        }
        return storage == null ? CompletableFuture.completedFuture(null) : strictResign(storage);
    }

    /** Resigns the current local term when the observed KRaft epoch is current or newer. */
    public CompletableFuture<Void> resign(
            KafkaPartitionIdentity identity, int observedLeaderEpoch) {
        Objects.requireNonNull(identity, "identity");
        if (observedLeaderEpoch < 0) {
            throw new IllegalArgumentException("observedLeaderEpoch must be non-negative");
        }
        KafkaPartitionStorage storage = null;
        synchronized (guard) {
            Slot slot = slots.get(identity);
            if (slot == null || slot.desired.authority().leaderEpoch() > observedLeaderEpoch) {
                return CompletableFuture.completedFuture(null);
            }
            slots.remove(identity);
            if (slot.installed != null) storage = slot.installed.storage;
        }
        return storage == null ? CompletableFuture.completedFuture(null) : strictResign(storage);
    }

    public Optional<KafkaPartitionStorage> current(KafkaPartitionIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        synchronized (guard) {
            Slot slot = slots.get(identity);
            return slot == null || slot.installed == null
                    ? Optional.empty()
                    : Optional.of(slot.installed.storage);
        }
    }

    public int installedPartitions() {
        synchronized (guard) {
            return Math.toIntExact(slots.values().stream()
                    .filter(slot -> slot.installed != null)
                    .count());
        }
    }

    /** Stops new opens and asynchronously resigns every installed instance. */
    public CompletableFuture<Void> shutdown() {
        List<KafkaPartitionStorage> installed = new ArrayList<>();
        synchronized (guard) {
            if (closed) return CompletableFuture.completedFuture(null);
            closed = true;
            for (Slot slot : slots.values()) {
                if (slot.installed != null) installed.add(slot.installed.storage);
            }
            slots.clear();
        }
        CompletableFuture<?>[] resigns = installed.stream()
                .map(KafkaPartitionLeaderManager::strictResign)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(resigns);
    }

    @Override
    public void close() {
        shutdown();
    }

    private void beginOpen(OpenAttempt attempt) {
        CompletableFuture<KafkaPartitionStorage> opening;
        try {
            opening = Objects.requireNonNull(opener.open(attempt.plan), "Kafka partition open future");
        } catch (Throwable failure) {
            opening = CompletableFuture.failedFuture(failure);
        }
        opening.whenComplete((storage, failure) -> completeOpen(attempt, storage, failure));
    }

    private void completeOpen(
            OpenAttempt attempt,
            KafkaPartitionStorage storage,
            Throwable suppliedFailure) {
        Throwable failure = suppliedFailure == null ? null : unwrap(suppliedFailure);
        if (failure == null) {
            try {
                validateOpened(attempt.plan, storage);
            } catch (Throwable invalid) {
                failure = invalid;
            }
        }
        boolean install = false;
        synchronized (guard) {
            Slot slot = slots.get(attempt.plan.authority().identity());
            if (failure == null
                    && !closed
                    && slot != null
                    && slot.opening == attempt
                    && attempt.plan == slot.desired) {
                slot.opening = null;
                slot.installed = new Installed(attempt.plan, storage);
                install = true;
            } else if (slot != null && slot.opening == attempt) {
                slot.opening = null;
                if (slot.installed == null && attempt.plan == slot.desired) {
                    slots.remove(attempt.plan.authority().identity());
                }
            }
        }
        if (install) {
            attempt.result.succeed(storage);
            return;
        }
        if (storage != null) safeResign(storage);
        attempt.result.fail(failure == null
                ? fenced("Kafka partition open was superseded before installation")
                : failure);
    }

    private static void validateOpened(
            KafkaPartitionOpenPlan plan, KafkaPartitionStorage storage) {
        Objects.requireNonNull(storage, "Kafka partition storage");
        if (!storage.identity().equals(plan.authority().identity())
                || storage.leaderEpoch() != plan.authority().leaderEpoch()
                || storage.storageProfile() != plan.profilePolicy().storageProfile()
                || storage.state() != KafkaPartitionState.LEADER_WRITABLE) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "Kafka opener returned storage that does not match the recovered leader authority");
        }
    }

    private static CompletableFuture<Void> safeResign(KafkaPartitionStorage storage) {
        return strictResign(storage).exceptionally(ignored -> null);
    }

    private static CompletableFuture<Void> strictResign(KafkaPartitionStorage storage) {
        try {
            return Objects.requireNonNull(storage.resign(), "Kafka partition resign future");
        } catch (Throwable failure) {
            try {
                storage.close();
            } catch (Throwable alsoIgnored) {
                // Durable authority fencing is the correctness owner if local cleanup fails.
            }
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException fenced(String message) {
        return new NereusException(ErrorCode.FENCED_APPEND, false, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static CompletableFuture<KafkaPartitionStorage> failedClosed() {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.STORAGE_CLOSED, false, "Kafka partition leader manager is closed"));
    }

    private static final class Slot {
        private KafkaPartitionOpenPlan desired;
        private OpenAttempt opening;
        private Installed installed;
    }

    private static final class OpenAttempt {
        private final KafkaPartitionOpenPlan plan;
        private final OpenFuture result = new OpenFuture();

        private OpenAttempt(KafkaPartitionOpenPlan plan) {
            this.plan = plan;
        }
    }

    private static final class OpenFuture extends CompletableFuture<KafkaPartitionStorage> {
        private boolean succeed(KafkaPartitionStorage storage) {
            return super.complete(storage);
        }

        private boolean fail(Throwable failure) {
            return super.completeExceptionally(failure);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean complete(KafkaPartitionStorage storage) {
            return false;
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            return false;
        }

        @Override
        public void obtrudeValue(KafkaPartitionStorage storage) {
            throw new UnsupportedOperationException("Kafka partition open completion is manager-owned");
        }

        @Override
        public void obtrudeException(Throwable failure) {
            throw new UnsupportedOperationException("Kafka partition open completion is manager-owned");
        }
    }

    private record Installed(
            KafkaPartitionOpenPlan plan,
            KafkaPartitionStorage storage) {}
}
