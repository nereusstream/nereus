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

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceLifecycleV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceViewV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.oxia.v2.allocator.OxiaVirtualLedgerAllocatorKeys;
import com.nereusstream.metadata.oxia.v2.allocator.OxiaVirtualLedgerAllocatorStore;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Request;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Result;
import com.nereusstream.metadata.spi.allocator.ProductionVirtualLedgerAllocator;
import com.nereusstream.metadata.spi.allocator.PulsarVirtualLedgerAllocatorStore;
import com.nereusstream.metadata.spi.allocator.VersionedAllocatorCellStateV1;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.allocator.VersionedVirtualLedgerCandidateNodeV1;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** Short, non-promotable real-Oxia prerequisite for the V2 formal campaign. */
class M3V2RealOxiaDiagnosticTest {
    @Test
    void strictWorkflowUsesRealOxia() throws Exception {
        try (Fixture fixture = Fixture.open("strict", AllocatorEvidenceCandidateV1.strict())) {
            VersionedManagedLedgerAllocatorHeadV1 head = fixture.createHead(0, "strict-ledger");

            Result result = fixture.allocate(0, head, "strict-request");

            assertThat(result.exactCell().value().reservation()).isEmpty();
            assertThat(result.exactHead().value().visibleChainHead())
                    .isEqualTo(result.exactNode().value().pointer());
            assertThat(result.exactCell().value().nextSliceLedgerId())
                    .isEqualTo(fixture.initialSliceLedgerId() + 1);
        }
    }

    @Test
    void installedRangeReusesGrant() throws Exception {
        try (Fixture fixture = Fixture.open("installed-range", AllocatorEvidenceCandidateV1.range(16))) {
            VersionedManagedLedgerAllocatorHeadV1 head = fixture.createHead(0, "installed-range-ledger");
            Result first = fixture.allocate(0, head, "range-first");
            long consumedPrefix = first.exactCell().value().nextSliceLedgerId();

            Result second = fixture.allocate(1, first.exactHead(), "range-second");

            assertThat(first.exactHead().value().grantId()).isPositive();
            assertThat(second.exactHead().value().grantId())
                    .isEqualTo(first.exactHead().value().grantId());
            assertThat(second.exactCell().value().nextSliceLedgerId()).isEqualTo(consumedPrefix);
            assertThat(second.exactNode().value().ledgerId())
                    .isEqualTo(first.exactNode().value().ledgerId() + 1);
        }
    }

    @Test
    void rangeRenewalUsesCellCas() throws Exception {
        try (Fixture fixture = Fixture.open("range-renewal", AllocatorEvidenceCandidateV1.range(16))) {
            VersionedManagedLedgerAllocatorHeadV1 head = fixture.createHead(0, "range-renewal-ledger");
            List<Long> ledgerIds = new ArrayList<>();
            Result result = null;
            for (int index = 0; index < 17; index++) {
                result = fixture.allocate(index & 3, head, "renewal-" + index);
                head = result.exactHead();
                ledgerIds.add(result.exactNode().value().ledgerId());
            }

            assertThat(new HashSet<>(ledgerIds)).hasSize(17);
            assertThat(result).isNotNull();
            assertThat(result.exactCell().value().nextSliceLedgerId())
                    .isEqualTo(fixture.initialSliceLedgerId() + 32);
            assertThat(result.exactHead().value().grantId()).isEqualTo(2);
        }
    }

    @Test
    void conflictStormUsesFourIndependentCoordinators() throws Exception {
        try (Fixture fixture = Fixture.openConflictStorm("conflict-storm", AllocatorEvidenceCandidateV1.strict())) {
            List<VersionedManagedLedgerAllocatorHeadV1> heads = new ArrayList<>();
            for (int actorId = 0; actorId < 4; actorId++) {
                heads.add(fixture.createHead(actorId, "storm-ledger-" + actorId));
            }
            List<CompletableFuture<Result>> futures = new ArrayList<>();
            for (int actorId = 0; actorId < 4; actorId++) {
                futures.add(fixture.allocateAsync(actorId, heads.get(actorId), "storm-" + actorId));
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
            List<Result> results = futures.stream().map(CompletableFuture::join).toList();

            assertThat(results).hasSize(4);
            assertThat(results.stream().map(result -> result.exactNode().value().ledgerId()))
                    .doesNotHaveDuplicates();
            assertThat(results.stream().mapToInt(Result::reconcileRetries).sum())
                    .isPositive();
            VersionedAllocatorCellStateV1 terminal = fixture.readCell();
            assertThat(terminal.value().nextSliceLedgerId())
                    .isEqualTo(fixture.initialSliceLedgerId() + 4);
            assertThat(terminal.value().reservation()).isEmpty();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final String root;
        private final Sha256Digest namespace;
        private final VirtualLedgerSliceAssignmentV1 assignment;
        private final VersionedVirtualLedgerSliceViewV1 view;
        private final M3RealOxiaActors actors;
        private final List<PulsarVirtualLedgerAllocatorStore> stores;
        private final List<ProductionVirtualLedgerAllocator> allocators;
        private final List<BoundedVirtualLedgerAllocatorWorkflowV2> workflows;
        private final long initialSliceLedgerId;

        private Fixture(String scenario, AllocatorEvidenceCandidateV1 candidate, boolean conflictBarrier)
                throws Exception {
            String commit = requireProperty("nereus.m3.allocator.v2.nereusCommit");
            String runId = requireProperty("nereus.m3.allocator.v2.diagnosticRunId");
            root = "/nereus/m3/allocator-v2-diagnostic/" + commit + "/" + runId + "/" + scenario;
            namespace = digest(root + ":namespace");
            assignment = VirtualLedgerSliceAssignmentV1.create(
                    new DeploymentId(new Id128(1, 2)),
                    new ReservationDomainId(new Id128(3, 4)),
                    new PulsarCellId(new Id128(5, 6)),
                    namespace,
                    VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                    VirtualLedgerSliceLifecycleV1.ACTIVE);
            view = new VersionedVirtualLedgerSliceViewV1(
                    new VirtualLedgerSliceViewV1(namespace, 1, assignment),
                    version(1),
                    digest(root + ":registry"));
            initialSliceLedgerId = assignment.startInclusive();
            actors = new M3RealOxiaActors(requireProperty("nereus.m3.allocator.v2.oxiaServiceAddress"));
            CellCasBarrier barrier = conflictBarrier ? new CellCasBarrier(4) : null;
            List<PulsarVirtualLedgerAllocatorStore> actorStores = new ArrayList<>();
            List<ProductionVirtualLedgerAllocator> actorAllocators = new ArrayList<>();
            List<BoundedVirtualLedgerAllocatorWorkflowV2> actorWorkflows = new ArrayList<>();
            for (int actorId = 0; actorId < 4; actorId++) {
                PulsarVirtualLedgerAllocatorStore store = productionStore(root, actors.actor(actorId));
                if (barrier != null) {
                    store = new FirstCellCasBarrierStore(store, barrier);
                }
                ProductionVirtualLedgerAllocator allocator =
                        ProductionVirtualLedgerAllocator.forEvidenceCandidate(candidate, store);
                actorStores.add(store);
                actorAllocators.add(allocator);
                actorWorkflows.add(allocator.boundedWorkflow(
                        BoundedVirtualLedgerAllocatorWorkflowV2.Bounds.formal(), Fixture::boundedBackoff));
            }
            stores = List.copyOf(actorStores);
            allocators = List.copyOf(actorAllocators);
            workflows = List.copyOf(actorWorkflows);
            exactCreate(allocators.get(0).createCell(view));
        }

        private static Fixture open(String scenario, AllocatorEvidenceCandidateV1 candidate) throws Exception {
            return new Fixture(scenario, candidate, false);
        }

        private static Fixture openConflictStorm(String scenario, AllocatorEvidenceCandidateV1 candidate)
                throws Exception {
            return new Fixture(scenario, candidate, true);
        }

        private VersionedManagedLedgerAllocatorHeadV1 createHead(int actorId, String identity) {
            VersionedAllocatorCellStateV1 cell = readCell();
            return exactCreate(allocators.get(actorId)
                    .createHead(cell, view, new ManagedLedgerIncarnationIdV1(digest(root + ":" + identity)), 1));
        }

        private Result allocate(int actorId, VersionedManagedLedgerAllocatorHeadV1 head, String identity) {
            return allocateAsync(actorId, head, identity).join();
        }

        private CompletableFuture<Result> allocateAsync(
                int actorId, VersionedManagedLedgerAllocatorHeadV1 head, String identity) {
            Request request = new Request(
                    digest(root + ":request:" + identity),
                    digest(root + ":descriptor:" + identity),
                    view,
                    head);
            return workflows.get(actorId).allocate(request).toCompletableFuture();
        }

        private VersionedAllocatorCellStateV1 readCell() {
            return stores.get(0)
                    .readCell(namespace, assignment.sliceAssignmentId())
                    .toCompletableFuture()
                    .join()
                    .orElseThrow();
        }

        private long initialSliceLedgerId() {
            return initialSliceLedgerId;
        }

        @Override
        public void close() throws Exception {
            actors.close();
        }

        private static CompletionStage<Void> boundedBackoff(
                Sha256Digest requestId,
                int retryNumber,
                BoundedVirtualLedgerAllocatorWorkflowV2.RetryReason reason) {
            return CompletableFuture.runAsync(
                    () -> {}, CompletableFuture.delayedExecutor(5, TimeUnit.MILLISECONDS));
        }
    }

    private static PulsarVirtualLedgerAllocatorStore productionStore(
            String root, M3RealOxiaActors.Actor actor) {
        OxiaVirtualLedgerAllocatorKeys keys = new OxiaVirtualLedgerAllocatorKeys(root);
        return new OxiaVirtualLedgerAllocatorStore(
                () -> {},
                keys,
                actor.client(),
                new ConditionalMutationEngine(actor.client(), new MutationFailureClassifier()));
    }

    private static String requireProperty(String name) {
        String value = System.getProperty(name, "UNSET");
        if (value.isBlank() || value.equals("UNSET") || value.contains("/")) {
            throw new IllegalArgumentException("allocator V2 diagnostic property is absent or unsafe: " + name);
        }
        return value;
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static MetadataVersion version(long value) {
        return new MetadataVersion(CanonicalBytes.copyOf(ByteBuffer.allocate(8).putLong(value).array()));
    }

    private static <T> T exactCreate(CompletionStage<CreateMutationResult<T>> stage) {
        return stage.toCompletableFuture().join().exactSnapshot().orElseThrow();
    }

    private static final class CellCasBarrier {
        private final int parties;
        private final List<PendingCellCas> pending = new ArrayList<>();

        private CellCasBarrier(int parties) {
            this.parties = parties;
        }

        private synchronized CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> submit(
                Supplier<CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>>> dispatch) {
            CompletableFuture<ConditionalCasResult<VersionedAllocatorCellStateV1>> future = new CompletableFuture<>();
            pending.add(new PendingCellCas(dispatch, future));
            if (pending.size() == parties) {
                List<PendingCellCas> exact = List.copyOf(pending);
                pending.clear();
                exact.forEach(item -> item.dispatch().get().whenComplete((result, failure) -> {
                    if (failure == null) {
                        item.future().complete(result);
                    } else {
                        item.future().completeExceptionally(failure);
                    }
                }));
            }
            return future;
        }
    }

    private record PendingCellCas(
            Supplier<CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>>> dispatch,
            CompletableFuture<ConditionalCasResult<VersionedAllocatorCellStateV1>> future) {}

    private static final class FirstCellCasBarrierStore implements PulsarVirtualLedgerAllocatorStore {
        private final PulsarVirtualLedgerAllocatorStore delegate;
        private final CellCasBarrier barrier;
        private final AtomicBoolean firstCellCas = new AtomicBoolean(true);

        private FirstCellCasBarrierStore(PulsarVirtualLedgerAllocatorStore delegate, CellCasBarrier barrier) {
            this.delegate = delegate;
            this.barrier = barrier;
        }

        @Override
        public CompletionStage<Optional<VersionedAllocatorCellStateV1>> readCell(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId) {
            return delegate.readCell(namespaceId, sliceAssignmentId);
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
                VirtualLedgerCellAllocatorStateV1 candidate) {
            return delegate.createCell(candidate);
        }

        @Override
        public CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> compareAndSetCell(
                VersionedAllocatorCellStateV1 predecessor, VirtualLedgerCellAllocatorStateV1 candidate) {
            return firstCellCas.compareAndSet(true, false)
                    ? barrier.submit(() -> delegate.compareAndSetCell(predecessor, candidate))
                    : delegate.compareAndSetCell(predecessor, candidate);
        }

        @Override
        public CompletionStage<Optional<VersionedManagedLedgerAllocatorHeadV1>> readHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation) {
            return delegate.readHead(namespaceId, sliceAssignmentId, managedLedgerIncarnation);
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerAllocatorHeadV1 candidate) {
            return delegate.createHead(namespaceId, sliceAssignmentId, candidate);
        }

        @Override
        public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> compareAndSetHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                VersionedManagedLedgerAllocatorHeadV1 predecessor,
                ManagedLedgerAllocatorHeadV1 candidate) {
            return delegate.compareAndSetHead(namespaceId, sliceAssignmentId, predecessor, candidate);
        }

        @Override
        public CompletionStage<Optional<VersionedVirtualLedgerCandidateNodeV1>> readNode(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
                long ledgerId) {
            return delegate.readNode(namespaceId, sliceAssignmentId, managedLedgerIncarnation, ledgerId);
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createNode(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                VirtualLedgerCandidateNodeV1 candidate) {
            return delegate.createNode(namespaceId, sliceAssignmentId, candidate);
        }
    }
}
