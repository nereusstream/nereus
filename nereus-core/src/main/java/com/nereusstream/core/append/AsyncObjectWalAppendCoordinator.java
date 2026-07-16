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

package com.nereusstream.core.append;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import com.nereusstream.metadata.oxia.StableAppendResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Applies the Phase 4 acknowledgement cut after a protected stable Object-WAL commit.
 *
 * <p>{@link DurabilityLevel#WAL_DURABLE} returns from the stable-head boundary and starts an independently bounded,
 * idempotent generation-zero repair. Strict durability remains on the exact materialize-and-protect path.
 */
public final class AsyncObjectWalAppendCoordinator {
    private final GenerationZeroIndexMaterializer materializer;
    private final GenerationZeroPhysicalReferencePublisher physicalReferences;
    private final Duration backgroundRepairTimeout;
    private final Executor backgroundExecutor;
    private final ConcurrentMap<String, CompletableFuture<CommittedAppend>> backgroundRepairs =
            new ConcurrentHashMap<>();
    private final AtomicBoolean backgroundAdmissionClosed = new AtomicBoolean();
    private final AtomicLong backgroundRepairFailures = new AtomicLong();

    public AsyncObjectWalAppendCoordinator(
            GenerationZeroIndexMaterializer materializer,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            Duration backgroundRepairTimeout,
            Executor backgroundExecutor) {
        this.materializer = Objects.requireNonNull(materializer, "materializer");
        this.physicalReferences = Objects.requireNonNull(physicalReferences, "physicalReferences");
        this.backgroundRepairTimeout = requirePositive(
                backgroundRepairTimeout, "backgroundRepairTimeout");
        this.backgroundExecutor = Objects.requireNonNull(backgroundExecutor, "backgroundExecutor");
    }

    public CompletableFuture<CommittedAppend> completeAfterStableCommit(
            StableAppendResult stable,
            DurabilityLevel durability,
            Duration strictTimeout) {
        StableAppendResult exact = Objects.requireNonNull(stable, "stable");
        Objects.requireNonNull(durability, "durability");
        if (durability == DurabilityLevel.WAL_DURABLE) {
            startBackgroundRepair(exact.reachableAppend());
            return CompletableFuture.completedFuture(
                    exact.reachableAppend().committedAppend());
        }
        if (durability == DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED) {
            return repairAndProtect(
                    exact.reachableAppend(),
                    requirePositive(strictTimeout, "strictTimeout"));
        }
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.UNSUPPORTED_DURABILITY_LEVEL,
                false,
                "unknown Object-WAL durability boundary",
                AppendOutcome.KNOWN_COMMITTED));
    }

    private void startBackgroundRepair(ReachableCommittedAppend append) {
        if (backgroundAdmissionClosed.get()) {
            return;
        }
        String commitId = append.committedAppend().commitId();
        CompletableFuture<CommittedAppend> repair;
        try {
            repair = backgroundRepairs.computeIfAbsent(
                    commitId,
                    ignored -> CompletableFuture.completedFuture(append)
                            .thenComposeAsync(
                                    reachable ->
                                            repairAndProtect(
                                                    reachable,
                                                    backgroundRepairTimeout),
                                    backgroundExecutor));
        } catch (Throwable failure) {
            backgroundRepairFailures.incrementAndGet();
            return;
        }
        repair.whenComplete((ignored, failure) -> {
            if (failure != null) {
                backgroundRepairFailures.incrementAndGet();
            }
            backgroundRepairs.remove(commitId, repair);
        });
    }

    private CompletableFuture<CommittedAppend> repairAndProtect(
            ReachableCommittedAppend append,
            Duration timeout) {
        AppendDeadline deadline = new AppendDeadline(timeout);
        return deadline.bound(
                        () -> materializer.materialize(append),
                        AppendOutcome.KNOWN_COMMITTED,
                        "materialize generation-zero index")
                .thenCompose(materialized -> deadline.bound(
                        () -> physicalReferences.protectVisibleIndex(
                                materialized, deadline.remaining()),
                        AppendOutcome.KNOWN_COMMITTED,
                        "protect visible generation-zero index"))
                .thenApply(protectedIndex -> requireExactProtectedResult(
                        append, protectedIndex.materialized()));
    }

    private static CommittedAppend requireExactProtectedResult(
            ReachableCommittedAppend expected,
            MaterializedGenerationZero materialized) {
        if (!materialized.committedAppend().equals(expected.committedAppend())) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "generation-zero protection returned another committed append",
                    AppendOutcome.KNOWN_COMMITTED);
        }
        return materialized.committedAppend();
    }

    /** Stops new detached repairs after all append callers admitted by the owner have drained. */
    public void stopBackgroundAdmission() {
        backgroundAdmissionClosed.set(true);
    }

    /**
     * Waits best-effort for the detached repair set that existed at the time of the call.
     *
     * <p>Timeout or repair failure does not invalidate an already acknowledged stable append; the durable scanner can
     * resume the same idempotent work after restart.
     */
    public void awaitBackgroundRepairs(Duration timeout) {
        Duration exactTimeout = requirePositive(timeout, "timeout");
        CompletableFuture<?>[] snapshot =
                new ArrayList<>(backgroundRepairs.values()).toArray(CompletableFuture[]::new);
        if (snapshot.length == 0) {
            return;
        }
        try {
            CompletableFuture.allOf(snapshot)
                    .handle((ignored, failure) -> null)
                    .get(exactTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException | TimeoutException ignored) {
            // Stable WAL acknowledgements are recovered by the durable generation-zero scanner.
        }
    }

    int inFlightBackgroundRepairCount() {
        return backgroundRepairs.size();
    }

    long backgroundRepairFailureCount() {
        return backgroundRepairFailures.get();
    }

    private static Duration requirePositive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
