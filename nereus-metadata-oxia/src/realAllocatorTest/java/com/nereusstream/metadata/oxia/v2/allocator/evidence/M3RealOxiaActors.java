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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.EventOutcome;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.OxiaOperationKind;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Four independently closeable real Oxia sessions with latency and raw-operation instrumentation. */
final class M3RealOxiaActors implements AutoCloseable {
    private final String serviceAddress;
    private final List<Actor> actors = new ArrayList<>(M3AllocatorWorkloadPlan.BROKER_ACTORS);

    M3RealOxiaActors(String serviceAddress) throws Exception {
        this.serviceAddress = requireServiceAddress(serviceAddress);
        try {
            for (int actorId = 0; actorId < M3AllocatorWorkloadPlan.BROKER_ACTORS; actorId++) {
                actors.add(new Actor(actorId, this.serviceAddress));
            }
        } catch (Exception | Error failure) {
            try {
                close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    Actor actor(int actorId) {
        if (actorId < 0 || actorId >= actors.size()) {
            throw new IllegalArgumentException("allocator evidence actor ID is outside [0,4)");
        }
        return actors.get(actorId);
    }

    List<Actor> actors() {
        return List.copyOf(actors);
    }

    void setControlledLatencyMillis(int latencyMillis) {
        if (!M3AllocatorWorkloadPlan.METADATA_LATENCY_P99_MILLIS.contains(latencyMillis) && latencyMillis != 0) {
            throw new IllegalArgumentException("controlled Oxia latency differs from ADR 0094");
        }
        actors.forEach(actor -> actor.client.setControlledLatencyMillis(latencyMillis));
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        for (int index = actors.size() - 1; index >= 0; index--) {
            try {
                actors.get(index).close();
            } catch (Exception closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    static final class Actor implements AutoCloseable {
        private final int actorId;
        private final String serviceAddress;
        private final InstrumentedClient client;

        private Actor(int actorId, String serviceAddress) throws Exception {
            this.actorId = actorId;
            this.serviceAddress = serviceAddress;
            client = new InstrumentedClient(actorId, open(serviceAddress));
        }

        int actorId() {
            return actorId;
        }

        InstrumentedClient client() {
            return client;
        }

        void closeSessionWithWorkInFlight() throws Exception {
            client.closeSession();
        }

        void reopenFreshSession() throws Exception {
            client.reopen(open(serviceAddress));
        }

        CrashBarrier armCrashAfterNextMutationApplied() {
            return client.armCrashAfterNextMutationApplied();
        }

        @Override
        public void close() throws Exception {
            client.close();
        }

        private static AsyncOxiaClient open(String serviceAddress) throws Exception {
            return OxiaClientBuilder.create(serviceAddress)
                    .namespace("default")
                    .asyncClient()
                    .get(30, TimeUnit.SECONDS);
        }
    }

    static final class InstrumentedClient implements OxiaConditionalClient, AutoCloseable {
        private final int actorId;
        private final AtomicReference<Session> session = new AtomicReference<>();
        private final ScheduledExecutorService delayScheduler;
        private final AtomicInteger controlledLatencyMillis = new AtomicInteger();
        private final AtomicBoolean loseNextMutationResponse = new AtomicBoolean();
        private final AtomicReference<CrashBarrier> crashBarrier = new AtomicReference<>();
        private final ConcurrentMap<String, OperationBinding> bindings = new ConcurrentHashMap<>();

        private InstrumentedClient(int actorId, AsyncOxiaClient rawClient) {
            this.actorId = actorId;
            delayScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "m3-allocator-oxia-delay-" + actorId);
                thread.setDaemon(true);
                return thread;
            });
            session.set(new Session(rawClient));
        }

        OperationBinding bind(
                String key, M3AllocatorRequestTelemetry.RequestTrace trace, OxiaOperationKind mutationKind) {
            if (trace == null) {
                return null;
            }
            OperationBinding binding = new OperationBinding(trace, mutationKind);
            if (bindings.putIfAbsent(key, binding) != null) {
                throw new IllegalStateException("allocator evidence overlaps one exact Oxia authority key");
            }
            return binding;
        }

        void unbind(String key, OperationBinding binding) {
            if (binding != null && !bindings.remove(key, binding)) {
                throw new IllegalStateException("allocator evidence Oxia key binding drifted");
            }
        }

        void setControlledLatencyMillis(int latencyMillis) {
            controlledLatencyMillis.set(latencyMillis);
        }

        void loseNextMutationResponse() {
            if (!loseNextMutationResponse.compareAndSet(false, true)) {
                throw new IllegalStateException("an Oxia response-loss cut is already armed");
            }
        }

        CrashBarrier armCrashAfterNextMutationApplied() {
            CrashBarrier barrier = new CrashBarrier();
            if (!crashBarrier.compareAndSet(null, barrier)) {
                throw new IllegalStateException("an Oxia session-crash barrier is already armed");
            }
            return barrier;
        }

        @Override
        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            OperationBinding binding = bindings.get(key);
            M3AllocatorRequestTelemetry.OxiaOperation operation = binding == null
                    ? null
                    : binding.trace.startOxia(
                            OxiaOperationKind.EXACT_READ, key.getBytes(StandardCharsets.UTF_8).length);
            CompletionStage<Optional<AuthorityRecord>> dispatched;
            try {
                dispatched = requireSession().delegate.read(key);
            } catch (RuntimeException failure) {
                dispatched = CompletableFuture.failedFuture(failure);
            }
            return delayed(dispatched, false).whenComplete((record, failure) -> {
                if (binding != null) {
                    long responseBytes = record == null || record.isEmpty()
                            ? 0
                            : record.orElseThrow().storedBytes().length();
                    binding.trace.endOxia(
                            operation, failure == null ? EventOutcome.SUCCESS : EventOutcome.FAILED, responseBytes);
                    WriteProof proof = binding.writeProof.get();
                    if (proof != null && binding.rereadRecorded.compareAndSet(false, true)) {
                        binding.trace.sameKeyReread(proof.operation, proof.writeToken, proof.canonicalBytes);
                    }
                }
            });
        }

        @Override
        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes storedBytes) {
            return mutation(key, storedBytes, delegate -> delegate.createIfAbsent(key, storedBytes));
        }

        @Override
        public CompletionStage<Void> compareAndSet(
                String key, CanonicalBytes storedBytes, long expectedVersionId) {
            return mutation(key, storedBytes, delegate -> delegate.compareAndSet(key, storedBytes, expectedVersionId));
        }

        private CompletionStage<Void> mutation(String key, CanonicalBytes storedBytes, MutationDispatch dispatch) {
            OperationBinding binding = bindings.get(key);
            M3AllocatorRequestTelemetry.OxiaOperation operation = binding == null
                    ? null
                    : binding.trace.startOxia(binding.mutationKind, storedBytes.length());
            if (binding != null && binding.trace.faultCut() != null) {
                long token = binding.trace.metadataWriteDispatched(operation, storedBytes.length());
                binding.writeProof.set(new WriteProof(operation, token, storedBytes.length()));
            }
            boolean loseResponse = loseNextMutationResponse.compareAndSet(true, false);
            CrashBarrier barrier = crashBarrier.getAndSet(null);
            CompletionStage<Void> dispatched;
            try {
                dispatched = dispatch.run(requireSession().delegate);
            } catch (RuntimeException failure) {
                dispatched = CompletableFuture.failedFuture(failure);
            }
            CompletionStage<Void> held = barrier == null ? dispatched : barrier.hold(dispatched);
            return delayed(held, loseResponse).whenComplete((ignored, failure) -> {
                if (binding != null) {
                    binding.trace.endOxia(
                            operation, failure == null ? EventOutcome.SUCCESS : EventOutcome.FAILED, 0);
                }
            });
        }

        private <T> CompletionStage<T> delayed(CompletionStage<T> source, boolean loseResponse) {
            CompletableFuture<T> output = new CompletableFuture<>();
            source.whenComplete((value, failure) -> delayScheduler.schedule(
                    () -> {
                        if (failure != null) {
                            output.completeExceptionally(failure);
                        } else if (loseResponse) {
                            output.completeExceptionally(new IOException("injected response loss after real apply"));
                        } else {
                            output.complete(value);
                        }
                    },
                    controlledLatencyMillis.get(),
                    TimeUnit.MILLISECONDS));
            return output;
        }

        void closeSession() throws Exception {
            Session current = session.getAndSet(null);
            if (current == null) {
                throw new IllegalStateException("allocator Oxia actor session is already closed");
            }
            current.raw.close();
        }

        void reopen(AsyncOxiaClient freshClient) {
            Session fresh = new Session(freshClient);
            if (!session.compareAndSet(null, fresh)) {
                try {
                    fresh.raw.close();
                } catch (Exception closeFailure) {
                    // The caller still observes that the session was not in the crashed state.
                }
                throw new IllegalStateException("allocator Oxia actor session was not closed before reopen");
            }
        }

        private Session requireSession() {
            Session current = session.get();
            if (current == null) {
                throw new IllegalStateException("allocator Oxia actor session is closed");
            }
            return current;
        }

        @Override
        public void close() throws Exception {
            Exception failure = null;
            Session current = session.getAndSet(null);
            if (current != null) {
                try {
                    current.raw.close();
                } catch (Exception closeFailure) {
                    failure = closeFailure;
                }
            }
            delayScheduler.shutdownNow();
            if (!delayScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                IllegalStateException timeout =
                        new IllegalStateException("allocator Oxia delay scheduler did not stop");
                if (failure == null) {
                    failure = timeout;
                } else {
                    failure.addSuppressed(timeout);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        static final class OperationBinding {
            private final M3AllocatorRequestTelemetry.RequestTrace trace;
            private final OxiaOperationKind mutationKind;
            private final AtomicReference<WriteProof> writeProof = new AtomicReference<>();
            private final AtomicBoolean rereadRecorded = new AtomicBoolean();

            private OperationBinding(
                    M3AllocatorRequestTelemetry.RequestTrace trace, OxiaOperationKind mutationKind) {
                this.trace = Objects.requireNonNull(trace, "trace");
                this.mutationKind = Objects.requireNonNull(mutationKind, "mutationKind");
            }

            WriteProof writeProof() {
                return writeProof.get();
            }
        }

        private record Session(AsyncOxiaClient raw, OxiaConditionalClient delegate) {
            private Session(AsyncOxiaClient raw) {
                this(Objects.requireNonNull(raw, "raw"), new AsyncOxiaConditionalClient(raw));
            }
        }

        record WriteProof(
                M3AllocatorRequestTelemetry.OxiaOperation operation, long writeToken, long canonicalBytes) {}

        @FunctionalInterface
        private interface MutationDispatch {
            CompletionStage<Void> run(OxiaConditionalClient delegate);
        }
    }

    static final class CrashBarrier {
        private final CountDownLatch applied = new CountDownLatch(1);
        private final CompletableFuture<Void> release = new CompletableFuture<>();

        void awaitApplied() throws InterruptedException {
            if (!applied.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Oxia crash-cut mutation did not apply within 30 seconds");
            }
        }

        void releaseResponse() {
            release.complete(null);
        }

        private CompletionStage<Void> hold(CompletionStage<Void> source) {
            CompletableFuture<Void> output = new CompletableFuture<>();
            source.whenComplete((ignored, failure) -> {
                if (failure != null) {
                    output.completeExceptionally(failure);
                    applied.countDown();
                    return;
                }
                applied.countDown();
                release.whenComplete((released, releaseFailure) -> {
                    if (releaseFailure == null) {
                        output.complete(null);
                    } else {
                        output.completeExceptionally(releaseFailure);
                    }
                });
            });
            return output;
        }
    }

    private static String requireServiceAddress(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("real Oxia service address is required");
        }
        return value;
    }
}
