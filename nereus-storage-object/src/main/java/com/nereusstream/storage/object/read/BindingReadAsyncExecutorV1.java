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

package com.nereusstream.storage.object.read;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Event-loop adapter that holds one outer generation lease through real async source completion.
 *
 * <p>The supplied executor must serialize this Binding and must always execute its tasks on one
 * owner thread. Cancellation only closes new source use; it never treats a transport cancel request
 * as proof of provider termination and therefore never force-clears the lease.
 */
public final class BindingReadAsyncExecutorV1 {
    @FunctionalInterface
    public interface CapturedOperation<T> {
        CompletionStage<T> start(BindingReadAuthorityV1 authority);
    }

    public static final class AdmissionException extends IllegalStateException {
        private final BindingReadHazardPoolV1.CaptureOutcome outcome;

        private AdmissionException(BindingReadHazardPoolV1.CaptureOutcome outcome) {
            super("M4 read admission failed before source I/O: " + outcome);
            this.outcome = outcome;
        }

        public BindingReadHazardPoolV1.CaptureOutcome outcome() {
            return outcome;
        }
    }

    private final Executor ownerExecutor;
    private final ArrayDeque<BindingReadBatchContextV1> reusableBatchContexts;

    public BindingReadAsyncExecutorV1(Executor ownerExecutor, int maximumInFlightReads) {
        this(ownerExecutor, maximumInFlightReads, BindingReadBatchContextV1::new);
    }

    BindingReadAsyncExecutorV1(
            Executor ownerExecutor, int maximumInFlightReads, Supplier<BindingReadBatchContextV1> batchContextFactory) {
        this.ownerExecutor = Objects.requireNonNull(ownerExecutor, "ownerExecutor");
        Objects.requireNonNull(batchContextFactory, "batchContextFactory");
        if (maximumInFlightReads <= 0 || maximumInFlightReads > 65_536) {
            throw new IllegalArgumentException("reusable read-context capacity is outside the admitted bound");
        }
        reusableBatchContexts = new ArrayDeque<>(maximumInFlightReads);
        for (int index = 0; index < maximumInFlightReads; index++) {
            reusableBatchContexts.addLast(
                    Objects.requireNonNull(batchContextFactory.get(), "reusable read-batch context"));
        }
    }

    public <T> CompletableFuture<T> execute(
            AtomicReference<BindingReadAuthorityV1> currentAuthority,
            BindingReadHazardPoolV1 hazardPool,
            CapturedOperation<T> operation) {
        return execute(currentAuthority, hazardPool, operation, () -> {});
    }

    public <T> CompletableFuture<T> execute(
            AtomicReference<BindingReadAuthorityV1> currentAuthority,
            BindingReadHazardPoolV1 hazardPool,
            CapturedOperation<T> operation,
            Runnable afterTerminalDrain) {
        Objects.requireNonNull(currentAuthority, "currentAuthority");
        Objects.requireNonNull(hazardPool, "hazardPool");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(afterTerminalDrain, "afterTerminalDrain");
        CompletableFuture<T> result = new CompletableFuture<>();
        State<T> state = new State<>(currentAuthority, hazardPool, operation, afterTerminalDrain, result);
        result.whenComplete((ignored, failure) -> {
            if (result.isCancelled()) {
                ownerExecutor.execute(state::requestCancellation);
            }
        });
        ownerExecutor.execute(state::start);
        return result;
    }

    private final class State<T> {
        private final AtomicReference<BindingReadAuthorityV1> currentAuthority;
        private final BindingReadHazardPoolV1 hazardPool;
        private final CapturedOperation<T> operation;
        private final Runnable afterTerminalDrain;
        private final CompletableFuture<T> result;
        private BindingReadBatchContextV1 batch;
        private boolean terminal;

        private State(
                AtomicReference<BindingReadAuthorityV1> currentAuthority,
                BindingReadHazardPoolV1 hazardPool,
                CapturedOperation<T> operation,
                Runnable afterTerminalDrain,
                CompletableFuture<T> result) {
            this.currentAuthority = currentAuthority;
            this.hazardPool = hazardPool;
            this.operation = operation;
            this.afterTerminalDrain = afterTerminalDrain;
            this.result = result;
        }

        private void start() {
            if (result.isCancelled()) {
                terminal = true;
                return;
            }
            batch = reusableBatchContexts.pollFirst();
            if (batch == null) {
                terminal = true;
                result.completeExceptionally(new AdmissionException(BindingReadHazardPoolV1.CaptureOutcome.EXHAUSTED));
                return;
            }
            BindingReadHazardPoolV1.CaptureOutcome capture = hazardPool.tryCapture(currentAuthority, batch);
            if (capture != BindingReadHazardPoolV1.CaptureOutcome.CAPTURED) {
                terminal = true;
                releaseReusableBatch();
                result.completeExceptionally(new AdmissionException(capture));
                return;
            }
            if (!batch.beginAttempt(1)) {
                batch.closeNewSourceUse();
                terminal = true;
                if (!batch.terminalClearExactLease()) {
                    throw new IllegalStateException("unused M4 async lease did not clear");
                }
                releaseReusableBatch();
                result.completeExceptionally(new IllegalStateException("M4 source attempt did not start"));
                return;
            }
            final CompletionStage<T> source;
            try {
                source = Objects.requireNonNull(operation.start(batch.authority()), "source operation stage");
            } catch (Throwable failure) {
                finish(null, failure);
                return;
            }
            source.whenComplete((value, failure) -> ownerExecutor.execute(() -> finish(value, failure)));
        }

        private void requestCancellation() {
            if (!terminal && batch != null && batch.active()) {
                batch.closeNewSourceUse();
            }
        }

        private void finish(T value, Throwable failure) {
            if (terminal) {
                return;
            }
            terminal = true;
            boolean drained = batch.endAttempt(1);
            batch.closeNewSourceUse();
            if (!drained || !batch.terminalClearExactLease()) {
                batch.quarantine();
                if (!result.isDone()) {
                    result.completeExceptionally(
                            new IllegalStateException("M4 async source termination could not clear its exact lease"));
                }
                return;
            }
            releaseReusableBatch();
            try {
                afterTerminalDrain.run();
            } catch (Throwable reconciliationFailure) {
                if (!result.isDone()) {
                    result.completeExceptionally(reconciliationFailure);
                }
                return;
            }
            if (result.isDone()) {
                return;
            }
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(unwrap(failure));
            }
        }

        private void releaseReusableBatch() {
            BindingReadBatchContextV1 released = batch;
            batch = null;
            if (released != null) {
                reusableBatchContexts.addLast(released);
            }
        }
    }

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException completion && completion.getCause() != null
                ? completion.getCause()
                : failure;
    }
}
