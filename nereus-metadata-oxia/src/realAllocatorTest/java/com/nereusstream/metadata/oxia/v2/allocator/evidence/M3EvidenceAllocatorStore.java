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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.EventOutcome;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.OxiaOperationKind;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.oxia.v2.allocator.OxiaVirtualLedgerAllocatorKeys;
import com.nereusstream.metadata.oxia.v2.allocator.OxiaVirtualLedgerAllocatorStore;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.spi.allocator.PulsarVirtualLedgerAllocatorStore;
import com.nereusstream.metadata.spi.allocator.VersionedAllocatorCellStateV1;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.allocator.VersionedVirtualLedgerCandidateNodeV1;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Raw-evidence decorator around the exact production Oxia adapter. It never implements allocator transitions itself;
 * it only binds real client calls to the currently executing request.
 */
final class M3EvidenceAllocatorStore implements PulsarVirtualLedgerAllocatorStore {
    private final OxiaVirtualLedgerAllocatorKeys keys;
    private final M3RealOxiaActors.InstrumentedClient client;
    private final OxiaVirtualLedgerAllocatorStore delegate;
    private final TraceRegistry traces;

    M3EvidenceAllocatorStore(
            String root, M3RealOxiaActors.InstrumentedClient client, TraceRegistry traces) {
        keys = new OxiaVirtualLedgerAllocatorKeys(root);
        this.client = Objects.requireNonNull(client, "client");
        this.traces = Objects.requireNonNull(traces, "traces");
        delegate = new OxiaVirtualLedgerAllocatorStore(
                () -> {},
                keys,
                client,
                new ConditionalMutationEngine(client, new MutationFailureClassifier()));
    }

    @Override
    public CompletionStage<Optional<VersionedAllocatorCellStateV1>> readCell(
            Sha256Digest namespaceId, Sha256Digest sliceAssignmentId) {
        String key = keys.cellKey(namespaceId, sliceAssignmentId);
        return read(key, traces.cellTrace(), () -> delegate.readCell(namespaceId, sliceAssignmentId));
    }

    @Override
    public CompletionStage<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
            VirtualLedgerCellAllocatorStateV1 candidate) {
        String key = keys.cellKey(candidate.ledgerIdCompatibilityNamespaceId(), candidate.sliceAssignmentId());
        return mutation(
                key,
                traces.cellTrace(),
                OxiaOperationKind.CELL_CREATE,
                () -> delegate.createCell(candidate),
                result -> outcome(result.outcome()));
    }

    @Override
    public CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> compareAndSetCell(
            VersionedAllocatorCellStateV1 exactPredecessor, VirtualLedgerCellAllocatorStateV1 candidate) {
        String key = keys.cellKey(
                exactPredecessor.value().ledgerIdCompatibilityNamespaceId(),
                exactPredecessor.value().sliceAssignmentId());
        return mutation(
                key,
                traces.cellTrace(),
                traces.cellMutationKind(),
                () -> delegate.compareAndSetCell(exactPredecessor, candidate),
                result -> outcome(result.outcome()));
    }

    @Override
    public CompletionStage<Optional<VersionedManagedLedgerAllocatorHeadV1>> readHead(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerIncarnationIdV1 managedLedgerIncarnation) {
        String key = keys.headKey(namespaceId, sliceAssignmentId, managedLedgerIncarnation);
        return read(
                key,
                traces.trace(managedLedgerIncarnation),
                () -> delegate.readHead(namespaceId, sliceAssignmentId, managedLedgerIncarnation));
    }

    @Override
    public CompletionStage<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerAllocatorHeadV1 candidate) {
        String key = keys.headKey(namespaceId, sliceAssignmentId, candidate.managedLedgerIncarnation());
        return mutation(
                key,
                traces.trace(candidate.managedLedgerIncarnation()),
                OxiaOperationKind.HEAD_CREATE,
                () -> delegate.createHead(namespaceId, sliceAssignmentId, candidate),
                result -> outcome(result.outcome()));
    }

    @Override
    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> compareAndSetHead(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            VersionedManagedLedgerAllocatorHeadV1 exactPredecessor,
            ManagedLedgerAllocatorHeadV1 candidate) {
        ManagedLedgerIncarnationIdV1 incarnation = exactPredecessor.value().managedLedgerIncarnation();
        String key = keys.headKey(namespaceId, sliceAssignmentId, incarnation);
        return mutation(
                key,
                traces.trace(incarnation),
                traces.headMutationKind(incarnation),
                () -> delegate.compareAndSetHead(namespaceId, sliceAssignmentId, exactPredecessor, candidate),
                result -> outcome(result.outcome()));
    }

    @Override
    public CompletionStage<Optional<VersionedVirtualLedgerCandidateNodeV1>> readNode(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
            long ledgerId) {
        String key = keys.nodeKey(namespaceId, sliceAssignmentId, managedLedgerIncarnation, ledgerId);
        return read(
                key,
                traces.trace(managedLedgerIncarnation),
                () -> delegate.readNode(namespaceId, sliceAssignmentId, managedLedgerIncarnation, ledgerId));
    }

    @Override
    public CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createNode(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            VirtualLedgerCandidateNodeV1 candidate) {
        String key = keys.nodeKey(
                namespaceId, sliceAssignmentId, candidate.managedLedgerIncarnation(), candidate.ledgerId());
        return mutation(
                key,
                traces.trace(candidate.managedLedgerIncarnation()),
                OxiaOperationKind.NODE_CREATE,
                () -> delegate.createNode(namespaceId, sliceAssignmentId, candidate),
                result -> outcome(result.outcome()));
    }

    private <T> CompletionStage<T> read(
            String key, M3AllocatorRequestTelemetry.RequestTrace trace, Supplier<CompletionStage<T>> operation) {
        M3RealOxiaActors.InstrumentedClient.OperationBinding binding =
                client.bind(key, trace, OxiaOperationKind.EXACT_READ);
        CompletionStage<T> stage;
        try {
            stage = Objects.requireNonNull(operation.get(), "allocator evidence read stage");
        } catch (RuntimeException failure) {
            client.unbind(key, binding);
            throw failure;
        }
        return stage.whenComplete((ignored, failure) -> client.unbind(key, binding));
    }

    private <T> CompletionStage<T> mutation(
            String key,
            M3AllocatorRequestTelemetry.RequestTrace trace,
            OxiaOperationKind kind,
            Supplier<CompletionStage<T>> operation,
            Function<T, EventOutcome> terminalOutcome) {
        M3RealOxiaActors.InstrumentedClient.OperationBinding binding = client.bind(key, trace, kind);
        CompletionStage<T> stage;
        try {
            stage = Objects.requireNonNull(operation.get(), "allocator evidence mutation stage");
        } catch (RuntimeException failure) {
            client.unbind(key, binding);
            throw failure;
        }
        return stage.whenComplete((result, failure) -> {
            try {
                if (binding != null && trace.faultCut() != null) {
                    M3RealOxiaActors.InstrumentedClient.WriteProof proof = binding.writeProof();
                    if (proof == null) {
                        throw new IllegalStateException("fault mutation did not dispatch an exact Oxia write");
                    }
                    if (failure != null) {
                        trace.unexpectedError();
                    } else {
                        trace.typedTerminal(
                                proof.operation(),
                                proof.writeToken(),
                                proof.canonicalBytes(),
                                terminalOutcome.apply(result));
                    }
                }
            } finally {
                client.unbind(key, binding);
            }
        });
    }

    private static EventOutcome outcome(CreateMutationOutcome outcome) {
        return switch (outcome) {
            case CREATED -> EventOutcome.APPLIED_EXACT;
            case EXISTING_EXACT -> EventOutcome.PREDECESSOR_UNCHANGED;
            case DEFINITIVE_CONFLICT -> EventOutcome.CONFLICT;
            case INDETERMINATE -> throw new IllegalStateException("allocator create remained indeterminate");
        };
    }

    private static EventOutcome outcome(ConditionalCasOutcome outcome) {
        return switch (outcome) {
            case APPLIED_EXACT -> EventOutcome.APPLIED_EXACT;
            case PREDECESSOR_UNCHANGED -> EventOutcome.PREDECESSOR_UNCHANGED;
            case DEFINITIVE_CONFLICT -> EventOutcome.CONFLICT;
            case INDETERMINATE -> throw new IllegalStateException("allocator CAS remained indeterminate");
        };
    }

    static final class TraceRegistry {
        private final ConcurrentMap<ManagedLedgerIncarnationIdV1, M3AllocatorRequestTelemetry.RequestTrace>
                traceByIncarnation = new ConcurrentHashMap<>();
        private final ConcurrentMap<ManagedLedgerIncarnationIdV1, OxiaOperationKind> headMutationKinds =
                new ConcurrentHashMap<>();
        private final AtomicReference<M3AllocatorRequestTelemetry.RequestTrace> cellTrace = new AtomicReference<>();
        private final AtomicReference<OxiaOperationKind> cellMutationKind = new AtomicReference<>();

        void bindHead(
                ManagedLedgerIncarnationIdV1 incarnation,
                M3AllocatorRequestTelemetry.RequestTrace trace,
                OxiaOperationKind headMutationKind) {
            if (traceByIncarnation.putIfAbsent(incarnation, trace) != null
                    || headMutationKinds.putIfAbsent(incarnation, headMutationKind) != null) {
                throw new IllegalStateException("allocator evidence overlaps one ManagedLedger trace");
            }
        }

        void setHeadMutationKind(ManagedLedgerIncarnationIdV1 incarnation, OxiaOperationKind kind) {
            if (traceByIncarnation.containsKey(incarnation)) {
                headMutationKinds.put(incarnation, kind);
            }
        }

        void unbindHead(ManagedLedgerIncarnationIdV1 incarnation) {
            traceByIncarnation.remove(incarnation);
            headMutationKinds.remove(incarnation);
        }

        void bindCell(M3AllocatorRequestTelemetry.RequestTrace trace, OxiaOperationKind kind) {
            if (!cellTrace.compareAndSet(null, trace) || !cellMutationKind.compareAndSet(null, kind)) {
                cellTrace.compareAndSet(trace, null);
                throw new IllegalStateException("allocator evidence overlaps the Cell-wide trace");
            }
        }

        void setCellMutationKind(OxiaOperationKind kind) {
            if (cellTrace.get() == null) {
                throw new IllegalStateException("allocator evidence Cell trace is not bound");
            }
            cellMutationKind.set(Objects.requireNonNull(kind, "kind"));
        }

        void unbindCell(M3AllocatorRequestTelemetry.RequestTrace trace) {
            if (!cellTrace.compareAndSet(trace, null)) {
                throw new IllegalStateException("allocator evidence Cell trace drifted");
            }
            cellMutationKind.set(null);
        }

        private M3AllocatorRequestTelemetry.RequestTrace trace(ManagedLedgerIncarnationIdV1 incarnation) {
            return traceByIncarnation.get(incarnation);
        }

        private OxiaOperationKind headMutationKind(ManagedLedgerIncarnationIdV1 incarnation) {
            return headMutationKinds.getOrDefault(incarnation, OxiaOperationKind.HEAD_PUBLISH_CAS);
        }

        private M3AllocatorRequestTelemetry.RequestTrace cellTrace() {
            return cellTrace.get();
        }

        private OxiaOperationKind cellMutationKind() {
            return cellMutationKind.get() == null ? OxiaOperationKind.CELL_RESERVE_CAS : cellMutationKind.get();
        }
    }
}
