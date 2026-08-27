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

    void requirePopulationConstructionLatencyDisabled() {
        requirePopulationConstructionLatencyDisabled(
                actors.stream().map(actor -> actor.client).toList());
    }

    static void requirePopulationConstructionLatencyDisabled(List<InstrumentedClient> clients) {
        Objects.requireNonNull(clients, "clients");
        if (clients.isEmpty()
                || clients.stream().anyMatch(client ->
                        Objects.requireNonNull(client, "client").controlledLatencyMillis() != 0)) {
            throw new IllegalStateException(
                    "allocator population construction inherited measured metadata latency");
        }
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
        private final AtomicReference<OperationDiagnostics> operationDiagnostics = new AtomicReference<>();

        private InstrumentedClient(int actorId, AsyncOxiaClient rawClient) {
            this(actorId, new Session(rawClient));
        }

        InstrumentedClient(int actorId, OxiaConditionalClient delegate) {
            this(actorId, new Session(() -> {}, Objects.requireNonNull(delegate, "delegate")));
        }

        private InstrumentedClient(int actorId, Session initialSession) {
            this.actorId = actorId;
            delayScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "m3-allocator-oxia-delay-" + actorId);
                thread.setDaemon(true);
                return thread;
            });
            session.set(Objects.requireNonNull(initialSession, "initialSession"));
        }

        OperationBinding binding(
                String key, M3AllocatorRequestTelemetry.RequestTrace trace, OxiaOperationKind mutationKind) {
            if (trace == null) {
                return null;
            }
            return new OperationBinding(key, trace, mutationKind);
        }

        OxiaConditionalClient bound(OperationBinding binding) {
            return binding == null ? this : new BoundClient(this, binding);
        }

        void setControlledLatencyMillis(int latencyMillis) {
            controlledLatencyMillis.set(latencyMillis);
        }

        int controlledLatencyMillis() {
            return controlledLatencyMillis.get();
        }

        void beginDiagnosticCapture() {
            if (!operationDiagnostics.compareAndSet(null, new OperationDiagnostics())) {
                throw new IllegalStateException("allocator Oxia operation diagnostic capture is already active");
            }
        }

        OperationDiagnosticSnapshot endDiagnosticCapture() {
            OperationDiagnostics diagnostics = operationDiagnostics.getAndSet(null);
            if (diagnostics == null) {
                throw new IllegalStateException("allocator Oxia operation diagnostic capture is not active");
            }
            return diagnostics.snapshot();
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
            return read(null, key);
        }

        private CompletionStage<Optional<AuthorityRecord>> read(OperationBinding binding, String key) {
            requireBindingKey(binding, key);
            OperationCapture capture = startDiagnosticOperation("READ");
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
            return delayed(dispatched, false, capture).whenComplete((record, failure) -> {
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
            return mutation(
                    null,
                    "CREATE_IF_ABSENT",
                    key,
                    storedBytes,
                    delegate -> delegate.createIfAbsent(key, storedBytes));
        }

        @Override
        public CompletionStage<Void> compareAndSet(
                String key, CanonicalBytes storedBytes, long expectedVersionId) {
            return mutation(
                    null,
                    "COMPARE_AND_SET",
                    key,
                    storedBytes,
                    delegate -> delegate.compareAndSet(key, storedBytes, expectedVersionId));
        }

        private CompletionStage<Void> mutation(
                OperationBinding binding,
                String diagnosticKind,
                String key,
                CanonicalBytes storedBytes,
                MutationDispatch dispatch) {
            requireBindingKey(binding, key);
            OperationCapture capture = startDiagnosticOperation(diagnosticKind);
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
            return delayed(held, loseResponse, capture).whenComplete((ignored, failure) -> {
                if (binding != null) {
                    binding.trace.endOxia(
                            operation, failure == null ? EventOutcome.SUCCESS : EventOutcome.FAILED, 0);
                }
            });
        }

        private static void requireBindingKey(OperationBinding binding, String key) {
            Objects.requireNonNull(key, "key");
            if (binding != null && !binding.key.equals(key)) {
                throw new IllegalStateException("allocator evidence binding crossed an exact Oxia authority key");
            }
        }

        private OperationCapture startDiagnosticOperation(String kind) {
            OperationDiagnostics diagnostics = operationDiagnostics.get();
            return diagnostics == null ? null : diagnostics.start(kind, System.nanoTime());
        }

        private <T> CompletionStage<T> delayed(
                CompletionStage<T> source, boolean loseResponse, OperationCapture capture) {
            CompletableFuture<T> output = new CompletableFuture<>();
            source.whenComplete((value, failure) -> {
                long sourceCompletedNanos = System.nanoTime();
                int delayMillis = controlledLatencyMillis.get();
                long targetNanos = Math.addExact(
                        sourceCompletedNanos, TimeUnit.MILLISECONDS.toNanos(delayMillis));
                delayScheduler.schedule(
                        () -> {
                            long schedulerFiredNanos = System.nanoTime();
                            if (failure != null) {
                                output.completeExceptionally(failure);
                            } else if (loseResponse) {
                                output.completeExceptionally(
                                        new IOException("injected response loss after real apply"));
                            } else {
                                output.complete(value);
                            }
                            if (capture != null) {
                                capture.finish(
                                        sourceCompletedNanos,
                                        delayMillis,
                                        targetNanos,
                                        schedulerFiredNanos,
                                        System.nanoTime(),
                                        failure != null || loseResponse);
                            }
                        },
                        delayMillis,
                        TimeUnit.MILLISECONDS);
            });
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
            private final String key;
            private final M3AllocatorRequestTelemetry.RequestTrace trace;
            private final OxiaOperationKind mutationKind;
            private final AtomicReference<WriteProof> writeProof = new AtomicReference<>();
            private final AtomicBoolean rereadRecorded = new AtomicBoolean();

            private OperationBinding(
                    String key,
                    M3AllocatorRequestTelemetry.RequestTrace trace,
                    OxiaOperationKind mutationKind) {
                this.key = Objects.requireNonNull(key, "key");
                this.trace = Objects.requireNonNull(trace, "trace");
                this.mutationKind = Objects.requireNonNull(mutationKind, "mutationKind");
            }

            WriteProof writeProof() {
                return writeProof.get();
            }
        }

        private record BoundClient(InstrumentedClient client, OperationBinding binding)
                implements OxiaConditionalClient {
            private BoundClient {
                Objects.requireNonNull(client, "client");
                Objects.requireNonNull(binding, "binding");
            }

            @Override
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return client.read(binding, key);
            }

            @Override
            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes storedBytes) {
                return client.mutation(
                        binding,
                        "CREATE_IF_ABSENT",
                        key,
                        storedBytes,
                        delegate -> delegate.createIfAbsent(key, storedBytes));
            }

            @Override
            public CompletionStage<Void> compareAndSet(
                    String key, CanonicalBytes storedBytes, long expectedVersionId) {
                return client.mutation(
                        binding,
                        "COMPARE_AND_SET",
                        key,
                        storedBytes,
                        delegate -> delegate.compareAndSet(key, storedBytes, expectedVersionId));
            }
        }

        private record Session(AutoCloseable raw, OxiaConditionalClient delegate) {
            private Session(AsyncOxiaClient raw) {
                this(Objects.requireNonNull(raw, "raw"), new AsyncOxiaConditionalClient(raw));
            }
        }

        record WriteProof(
                M3AllocatorRequestTelemetry.OxiaOperation operation, long writeToken, long canonicalBytes) {}

        record OperationSample(
                String kind,
                long realRttMicros,
                int injectedLatencyMillis,
                long schedulerLagMicros,
                long callbackLagMicros,
                boolean failed) {
            OperationSample {
                Objects.requireNonNull(kind, "kind");
                if (realRttMicros < 0
                        || injectedLatencyMillis < 0
                        || schedulerLagMicros < 0
                        || callbackLagMicros < 0) {
                    throw new IllegalArgumentException("allocator Oxia operation diagnostic sample is negative");
                }
            }
        }

        record OperationDiagnosticSnapshot(List<OperationSample> samples, int outstandingMaximum) {
            OperationDiagnosticSnapshot {
                samples = List.copyOf(samples);
                if (samples.isEmpty() || outstandingMaximum <= 0) {
                    throw new IllegalArgumentException("allocator Oxia operation diagnostic snapshot is empty");
                }
            }
        }

        private static final class OperationDiagnostics {
            private final List<OperationSample> samples = new ArrayList<>();
            private int outstanding;
            private int outstandingMaximum;

            private synchronized OperationCapture start(String kind, long dispatchedNanos) {
                outstanding++;
                outstandingMaximum = Math.max(outstandingMaximum, outstanding);
                return new OperationCapture(this, kind, dispatchedNanos);
            }

            private synchronized void finish(OperationSample sample) {
                samples.add(sample);
                outstanding--;
                if (outstanding < 0) {
                    throw new IllegalStateException("allocator Oxia diagnostic outstanding became negative");
                }
            }

            private synchronized OperationDiagnosticSnapshot snapshot() {
                if (outstanding != 0) {
                    throw new IllegalStateException("allocator Oxia diagnostic has in-flight operations");
                }
                return new OperationDiagnosticSnapshot(samples, outstandingMaximum);
            }
        }

        private record OperationCapture(OperationDiagnostics owner, String kind, long dispatchedNanos) {
            private OperationCapture {
                Objects.requireNonNull(owner, "owner");
                Objects.requireNonNull(kind, "kind");
            }

            private void finish(
                    long sourceCompletedNanos,
                    int injectedLatencyMillis,
                    long targetNanos,
                    long schedulerFiredNanos,
                    long callbackReturnedNanos,
                    boolean failed) {
                owner.finish(new OperationSample(
                        kind,
                        TimeUnit.NANOSECONDS.toMicros(Math.max(0, sourceCompletedNanos - dispatchedNanos)),
                        injectedLatencyMillis,
                        TimeUnit.NANOSECONDS.toMicros(Math.max(0, schedulerFiredNanos - targetNanos)),
                        TimeUnit.NANOSECONDS.toMicros(Math.max(0, callbackReturnedNanos - schedulerFiredNanos)),
                        failed));
            }
        }

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
