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
package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaBindingLifecycleMetadataStoreV2;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationOutcomeV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationResultV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardOutcomeV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardedMutationRequestV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Original C-ticketed Native control mutations. This actor grants no all-writer or delete qualification. */
final class PulsarNativeControlWriterV2 {
    /** Concrete physical inputs of source publication or fallback retirement; the original C Mutation owns dispatch. */
    interface PhysicalWriter {
        Sha256Digest root();

        long writerDescriptorBytes();

        CompletableFuture<Void> admit();

        CompletableFuture<Void> reconcile();

        M5TargetDeleteMultiWriterGuardV2 writerGuard();

        java.util.Map<
                        com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2,
                        M5TargetDeleteMultiWriterGuardV2.Context>
                writerContexts(Sha256Digest mutationRoot, ReferenceKindV1 kind);
    }

    private final PulsarNativeBindingV1 binding;
    private final PulsarNativeReadCellBudgetV1 budget;
    private final CanonicalBytes nativeOwner;
    private final PulsarNativeReadCellBudgetV1.Reservation lifetime;
    private final OxiaBindingLifecycleMetadataStoreV2 route;
    private final M5ClosedWriterRegistryV1 registry;
    private final M5ReferenceMutationGuardV1 guard;
    private final Set<Mutation> pending = new HashSet<>();
    private final Set<ReadHold> readHolds = new HashSet<>();
    private long pendingBytes;
    private boolean closed;
    private CompletableFuture<Void> cleanup;
    private CompletableFuture<Void> closing;

    PulsarNativeControlWriterV2(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            CanonicalBytes nativeOwner,
            PulsarNativeReadCellBudgetV1.Reservation lifetime,
            OxiaBindingLifecycleMetadataStoreV2 route,
            M5ClosedWriterRegistryV1 registry,
            M5ReferenceMutationGuardV1 guard) {
        this.binding = binding;
        this.budget = budget;
        this.nativeOwner = nativeOwner;
        this.lifetime = lifetime;
        this.route = route;
        this.registry = registry;
        this.guard = guard;
    }

    CompletableFuture<ControlMutationOutcome> mutate(
            ReferenceKindV1 kind, String key, Optional<CanonicalBytes> expected, CanonicalBytes candidate) {
        return mutate(kind, key, expected, candidate, Optional.empty());
    }

    CompletableFuture<ControlMutationOutcome> mutate(
            ReferenceKindV1 kind,
            String key,
            Optional<CanonicalBytes> expected,
            CanonicalBytes candidate,
            Optional<PhysicalWriter> physicalAdmission) {
        return mutate(kind, key, expected, candidate, physicalAdmission, route);
    }

    CompletableFuture<ControlMutationOutcome> createSourceMap(
            PulsarNativeM4SourceMapV1 map,
            String key,
            Optional<PhysicalWriter> physicalAdmission,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1 nativeMaps) {
        if (!key.equals(binding.m4Control().sourceMapKey(map.selectedViewSha256()))) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native source-map route differs"));
        }
        return mutate(
                ReferenceKindV1.MANIFEST_SELECTED, key, Optional.empty(), map.encode(), physicalAdmission, nativeMaps);
    }

    private CompletableFuture<ControlMutationOutcome> mutate(
            ReferenceKindV1 kind,
            String key,
            Optional<CanonicalBytes> expected,
            CanonicalBytes candidate,
            Optional<PhysicalWriter> physicalAdmission,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1 metadata) {
        final Mutation original;
        synchronized (this) {
            long bytes = 4096L
                    + candidate.length()
                    + expected.map(CanonicalBytes::length).orElse(0)
                    + physicalAdmission
                            .map(value -> value.writerDescriptorBytes())
                            .orElse(0L);
            if (closed || pending.size() + readHolds.size() >= 128 || pendingBytes + bytes > 1_048_576) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native control writer bound is exhausted"));
            }
            original = new Mutation(kind, key, expected, candidate, bytes, physicalAdmission, metadata);
            pending.add(original);
            pendingBytes += bytes;
        }
        original.start();
        return original.result.copy();
    }

    synchronized CompletableFuture<Void> reconcileCleanup() {
        if (cleanup == null || cleanup.isCompletedExceptionally()) {
            var originals = Set.copyOf(pending);
            cleanup = CompletableFuture.allOf(java.util.stream.Stream.concat(
                            originals.stream().map(Mutation::reconcile),
                            Set.copyOf(readHolds).stream()
                                    .filter(value ->
                                            value.nativeEnded.isDone() || value.opened.isCompletedExceptionally())
                                    .map(ReadHold::reconcile))
                    .toArray(CompletableFuture[]::new));
        } else if (cleanup.isDone() && (!pending.isEmpty() || !readHolds.isEmpty())) {
            cleanup = null;
            return reconcileCleanup();
        }
        return cleanup.copy();
    }

    synchronized CompletableFuture<Void> close() {
        closed = true;
        if (closing == null || closing.isCompletedExceptionally()) {
            closing = CompletableFuture.allOf(Set.copyOf(readHolds).stream()
                            .map(value -> value.settled)
                            .toArray(CompletableFuture[]::new))
                    .thenCompose(unused -> reconcileCleanup())
                    .thenCompose(unused -> lifetime.releaseAfterDrain());
        }
        return closing.copy();
    }

    CompletableFuture<PulsarNativeM4ReadOwnerV1> openReadHold(
            PhysicalWriter physical,
            java.util.function.Function<ReadHold, PulsarNativeM4ReadOwnerV1> start,
            java.util.concurrent.Executor executor) {
        return openReadHold(Optional.of(physical), physical.root(), start, executor);
    }

    CompletableFuture<PulsarNativeM4ReadOwnerV1> openReadHold(
            Sha256Digest sourceView,
            java.util.function.Function<ReadHold, PulsarNativeM4ReadOwnerV1> start,
            java.util.concurrent.Executor executor) {
        return openReadHold(Optional.empty(), sourceView, start, executor);
    }

    private CompletableFuture<PulsarNativeM4ReadOwnerV1> openReadHold(
            Optional<PhysicalWriter> physical,
            Sha256Digest root,
            java.util.function.Function<ReadHold, PulsarNativeM4ReadOwnerV1> start,
            java.util.concurrent.Executor executor) {
        final ReadHold original;
        synchronized (this) {
            long bytes = Math.addExact(
                    4096, physical.map(PhysicalWriter::writerDescriptorBytes).orElse(0L));
            if (closed || pending.size() + readHolds.size() >= 128 || pendingBytes + bytes > 1_048_576) {
                return CompletableFuture.failedFuture(new IllegalStateException("Native read-hold bound is exhausted"));
            }
            original = new ReadHold(physical, root, bytes);
            readHolds.add(original);
            pendingBytes += bytes;
        }
        original.start(start, executor);
        return original.opened.copy();
    }

    /** The original read actor holds C/D across SDK reads; ending an observer cannot acknowledge its native drain. */
    final class ReadHold {
        private final Optional<PhysicalWriter> physical;
        private final Sha256Digest root;
        private Optional<com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position> trim =
                Optional.empty();
        private final long bytes;
        private final GuardedMutationRequestV1 request;
        private final M5TargetDeleteMultiWriterGuardV2 physicalGuard;
        private final java.util.Map<
                        com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2,
                        M5TargetDeleteMultiWriterGuardV2.Context>
                contexts;
        private final CompletableFuture<Void> nativeEnded = new CompletableFuture<>();
        private final CompletableFuture<Void> settled = new CompletableFuture<>();
        private final CompletableFuture<PulsarNativeM4ReadOwnerV1> opened = new CompletableFuture<>();
        private M5TargetDeleteMultiWriterGuardV2.Result<Boolean> physicalResult;
        private ExternalMutationResultV1 terminal;
        private boolean actorStarted;
        private Throwable startFailure;

        private ReadHold(Optional<PhysicalWriter> physical, Sha256Digest root, long bytes) {
            this.physical = physical;
            this.root = root;
            this.bytes = bytes;
            var writer = registry.writerFor(ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE);
            if (!writer.writerClass().equals(PulsarNativeM4ReadOwnerV1.class.getName())) {
                throw new IllegalArgumentException("Native read owner is not enrolled");
            }
            request = new GuardedMutationRequestV1(
                    writer,
                    ReferenceTargetKindV1.PULSAR_AGGREGATE,
                    binding.authority().aggregate().canonicalStoredDigest(),
                    Sha256Digest.hash(CanonicalBytes.copyOf(
                            UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII))),
                    root);
            physicalGuard = physical.map(PhysicalWriter::writerGuard).orElse(null);
            contexts = physical.map(
                            value -> value.writerContexts(root, ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE))
                    .orElseGet(java.util.Map::of);
        }

        private void start(
                java.util.function.Function<ReadHold, PulsarNativeM4ReadOwnerV1> start,
                java.util.concurrent.Executor executor) {
            guard.execute(request, () -> budget.requireNativeOwner(binding, nativeOwner)
                            .thenCompose(unused -> binding.revalidate())
                            .thenCompose(
                                    unused -> binding.logicalTrimCoordinator().readFrontier())
                            .thenAccept(value -> trim = value.map(frontier ->
                                    frontier.pulsarContext().orElseThrow().newFrontier()))
                            .thenCompose(unused -> {
                                if (physical.isEmpty()) {
                                    return startActor(start, executor)
                                            .thenCompose(done -> nativeEnded.thenApply(ended -> terminal()));
                                }
                                var actual = physical.orElseThrow();
                                return actual.admit()
                                        .thenCompose(
                                                done -> physicalGuard.executeWithContexts(contexts, () -> actual.admit()
                                                        .thenCompose(accepted -> startActor(start, executor))
                                                        .thenCompose(accepted -> nativeEnded.thenApply(ended ->
                                                                new M5TargetDeleteMultiWriterGuardV2.Completion<>(
                                                                        actorStarted, Optional.of(root))))
                                                        .exceptionallyCompose(failure -> {
                                                            failOpening(failure);
                                                            return nativeEnded.thenApply(ended ->
                                                                    new M5TargetDeleteMultiWriterGuardV2.Completion<>(
                                                                            actorStarted, Optional.of(root)));
                                                        })))
                                        .thenApply(value -> {
                                            physicalResult = value;
                                            terminal = terminal();
                                            return value.unresolvedTargets().isEmpty()
                                                    ? terminal
                                                    : new ExternalMutationResultV1(
                                                            ExternalMutationOutcomeV1.RESPONSE_UNKNOWN, root);
                                        });
                            }))
                    .whenComplete((value, failure) -> {
                        settled.complete(null);
                        if (!opened.isDone()) {
                            opened.completeExceptionally(
                                    startFailure != null
                                            ? startFailure
                                            : failure != null
                                                    ? failure
                                                    : new IllegalStateException("Native read admission was fenced"));
                        }
                        if (failure == null
                                && (value.outcome() == GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED
                                        || value.outcome() == GuardOutcomeV1.MUTATION_NOT_APPLIED_AND_TICKET_CLEARED)) {
                            release();
                        }
                    });
        }

        private CompletableFuture<Void> startActor(
                java.util.function.Function<ReadHold, PulsarNativeM4ReadOwnerV1> start,
                java.util.concurrent.Executor executor) {
            return CompletableFuture.runAsync(
                    () -> {
                        try {
                            var owner = start.apply(this);
                            actorStarted = true;
                            if (!opened.complete(owner)) {
                                owner.fenceUndelivered(executor);
                            }
                        } catch (Throwable failure) {
                            failOpening(failure);
                        }
                    },
                    executor);
        }

        private void failOpening(Throwable failure) {
            startFailure = failure;
            opened.completeExceptionally(failure);
            if (!actorStarted) {
                nativeEnded.complete(null);
            }
        }

        private ExternalMutationResultV1 terminal() {
            terminal = new ExternalMutationResultV1(
                    actorStarted
                            ? ExternalMutationOutcomeV1.APPLIED_EXACT
                            : ExternalMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED,
                    root);
            return terminal;
        }

        void requireTrimDirectory(PulsarNativeBindingReadViewV1 view) {
            if (trim.filter(position -> !view.ledgerIds().contains(position.ledgerId()))
                    .isPresent()) {
                throw new IllegalStateException("Native read view lacks committed trim directory lineage");
            }
        }

        void requireReadAtOrAfterTrim(PulsarNativeBindingReadViewV1 view, long ledgerId, long firstEntry) {
            if (trim.isEmpty()) {
                return;
            }
            var frontier = trim.orElseThrow();
            var directory = view.ledgerIds();
            int requestedIndex = directory.indexOf(ledgerId);
            int trimIndex = directory.indexOf(frontier.ledgerId());
            if (requestedIndex < 0
                    || trimIndex < 0
                    || requestedIndex < trimIndex
                    || requestedIndex == trimIndex && firstEntry < frontier.entryId()) {
                throw new IllegalArgumentException("Native read precedes committed logical trim");
            }
        }

        void actorStarted() {
            actorStarted = true;
        }

        CompletableFuture<Void> closeAfterNativeDrain() {
            nativeEnded.complete(null);
            return reconcile().copy();
        }

        private CompletableFuture<Void> reconcile() {
            return settled.thenCompose(unused -> {
                CompletableFuture<Void> physicalCleanup = CompletableFuture.completedFuture(null);
                if (physicalResult != null
                        && !physicalResult.unresolvedTargets().isEmpty()) {
                    if (physicalResult.terminalCleanup().isEmpty()) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("original read-hold D outcome is unknown"));
                    }
                    physicalCleanup = physicalResult
                            .terminalCleanup()
                            .orElseThrow()
                            .reconcile()
                            .thenAccept(left -> {
                                if (!left.isEmpty()) {
                                    throw new IllegalStateException("original read-hold D cleanup remains unknown");
                                }
                            })
                            .toCompletableFuture();
                }
                return physicalCleanup
                        .thenCompose(done -> terminal == null
                                ? guard.reconcileUndispatched(request)
                                : guard.reconcileCompleted(request, terminal))
                        .thenAccept(value -> {
                            if (value.outcome() != GuardOutcomeV1.UNDISPATCHED_TICKET_FENCED_AND_CLEARED
                                    && value.outcome() != GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED
                                    && value.outcome() != GuardOutcomeV1.MUTATION_NOT_APPLIED_AND_TICKET_CLEARED) {
                                throw new IllegalStateException("original read-hold C cleanup remains unknown");
                            }
                            release();
                        });
            });
        }

        private void release() {
            synchronized (PulsarNativeControlWriterV2.this) {
                if (readHolds.remove(this)) {
                    pendingBytes -= bytes;
                }
            }
        }
    }

    private final class Mutation {
        private final com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1 metadata;
        private final ReferenceKindV1 kind;
        private final String key;
        private final Optional<CanonicalBytes> expected;
        private final CanonicalBytes candidate;
        private final long bytes;
        private final Optional<PhysicalWriter> physicalAdmission;
        private GuardedMutationRequestV1 request;
        private ExternalMutationResultV1 terminal;
        private M5TargetDeleteMultiWriterGuardV2 physicalGuard;
        private java.util.Map<
                        com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2,
                        M5TargetDeleteMultiWriterGuardV2.Context>
                physicalContexts;
        private M5TargetDeleteMultiWriterGuardV2.Result<ExternalMutationResultV1> physicalResult;
        private Optional<MetadataVersion> predecessorVersion = Optional.empty();
        private boolean enteredGuard;
        private boolean dispatched;
        private final CompletableFuture<ControlMutationOutcome> result = new CompletableFuture<>();
        private final CompletableFuture<Void> settled = new CompletableFuture<>();

        private Mutation(
                ReferenceKindV1 kind,
                String key,
                Optional<CanonicalBytes> expected,
                CanonicalBytes candidate,
                long bytes,
                Optional<PhysicalWriter> physicalAdmission,
                com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1 metadata) {
            this.metadata = metadata;
            this.kind = kind;
            this.key = key;
            this.expected = expected;
            this.candidate = candidate;
            this.bytes = bytes;
            this.physicalAdmission = physicalAdmission;
        }

        private void start() {
            CompletableFuture.completedFuture(null)
                    .thenCompose(unused -> budget.requireNativeOwner(binding, nativeOwner))
                    .thenCompose(unused -> binding.revalidate())
                    .thenCompose(unused -> metadata.read(key))
                    .thenCompose(predecessor -> {
                        if (!predecessor
                                .map(VersionedValue::canonicalStoredBytes)
                                .equals(expected)) {
                            return CompletableFuture.completedFuture(ControlMutationOutcome.DEFINITIVE_CONFLICT);
                        }
                        predecessorVersion = predecessor.map(VersionedValue::metadataVersion);
                        var writer = registry.writerFor(kind);
                        String expectedClass = kind == ReferenceKindV1.MATERIALIZATION_OR_COMPACTION_TASK
                                ? PulsarNativeMaterializationTaskV2.class.getName()
                                : PulsarNativeM4ControlV1.class.getName();
                        if (!writer.writerClass().equals(expectedClass)) {
                            throw new IllegalArgumentException("Native control writer declaration differs");
                        }
                        var encoded = new ByteArrayOutputStream();
                        try (var out = new DataOutputStream(encoded)) {
                            out.writeInt(0x4e504357); // NPCW
                            out.writeInt(2);
                            out.write(binding.metadataNamespace().encode().toByteArray());
                            out.write(binding.bindingIdentity()
                                    .bindingId()
                                    .digest()
                                    .bytes()
                                    .toByteArray());
                            out.write(binding.bindingIdentity()
                                    .incarnationSha256()
                                    .bytes()
                                    .toByteArray());
                            out.write(binding.bindingIdentity()
                                    .storageEpochSha256()
                                    .bytes()
                                    .toByteArray());
                            out.write(Sha256Digest.hash(nativeOwner).bytes().toByteArray());
                            out.writeUTF(key);
                            out.writeBoolean(predecessor.isPresent());
                            if (predecessor.isPresent()) {
                                out.write(predecessor
                                        .orElseThrow()
                                        .canonicalStoredSha256()
                                        .bytes()
                                        .toByteArray());
                                out.write(Sha256Digest.hash(predecessor
                                                .orElseThrow()
                                                .metadataVersion()
                                                .value())
                                        .bytes()
                                        .toByteArray());
                            }
                            out.write(Sha256Digest.hash(candidate).bytes().toByteArray());
                            out.writeBoolean(physicalAdmission.isPresent());
                            if (physicalAdmission.isPresent()) {
                                out.write(physicalAdmission
                                        .orElseThrow()
                                        .root()
                                        .bytes()
                                        .toByteArray());
                            }
                        } catch (java.io.IOException failure) {
                            throw new java.util.concurrent.CompletionException(failure);
                        }
                        var root = Sha256Digest.hash(CanonicalBytes.copyOf(encoded.toByteArray()));
                        request = new GuardedMutationRequestV1(
                                writer,
                                ReferenceTargetKindV1.PULSAR_AGGREGATE,
                                binding.authority().aggregate().canonicalStoredDigest(),
                                Sha256Digest.hash(CanonicalBytes.copyOf(UUID.randomUUID()
                                        .toString()
                                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII))),
                                root);
                        enteredGuard = true;
                        return guard.execute(request, () -> budget.requireNativeOwner(binding, nativeOwner)
                                        .thenCompose(unused -> binding.revalidate())
                                        .thenCompose(unused -> physicalAdmission
                                                .map(value -> value.admit())
                                                .orElseGet(() -> CompletableFuture.completedFuture(null)))
                                        .thenCompose(done -> dispatchWithPhysicalGuard(predecessor, root)))
                                .thenApply(guarded -> switch (guarded.outcome()) {
                                    case MUTATION_APPLIED_AND_TICKET_CLEARED -> ControlMutationOutcome.APPLIED;
                                    case MUTATION_NOT_APPLIED_AND_TICKET_CLEARED ->
                                        ControlMutationOutcome.DEFINITIVE_CONFLICT;
                                    default -> ControlMutationOutcome.RESPONSE_UNKNOWN;
                                });
                    })
                    .whenComplete((outcome, failure) -> {
                        settled.complete(null);
                        if (!enteredGuard
                                || failure == null
                                        && terminal != null
                                        && outcome != ControlMutationOutcome.RESPONSE_UNKNOWN) {
                            release();
                        }
                        if (failure != null) {
                            result.completeExceptionally(failure);
                        } else {
                            result.complete(outcome);
                        }
                    });
        }

        private CompletableFuture<ExternalMutationResultV1> dispatchWithPhysicalGuard(
                Optional<VersionedValue> predecessor, Sha256Digest root) {
            if (physicalAdmission.isEmpty()) {
                return dispatch(predecessor, root);
            }
            var admission = physicalAdmission.orElseThrow();
            physicalGuard = admission.writerGuard();
            physicalContexts = admission.writerContexts(root, kind);
            return physicalGuard
                    .executeWithContexts(physicalContexts, () -> admission
                            .admit()
                            .thenCompose(unused -> dispatch(predecessor, root))
                            .handle((value, failure) -> {
                                if (failure == null) {
                                    return value;
                                }
                                if (!dispatched) {
                                    terminal = new ExternalMutationResultV1(
                                            ExternalMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED, root);
                                    return terminal;
                                }
                                throw new java.util.concurrent.CompletionException(failure);
                            })
                            .thenApply(value -> new M5TargetDeleteMultiWriterGuardV2.Completion<>(
                                    value,
                                    value.outcome() == ExternalMutationOutcomeV1.RESPONSE_UNKNOWN
                                            ? Optional.empty()
                                            : Optional.of(root))))
                    .thenApply(value -> {
                        physicalResult = value;
                        if (!value.unresolvedTargets().isEmpty()
                                || value.value().isEmpty()) {
                            return new ExternalMutationResultV1(ExternalMutationOutcomeV1.RESPONSE_UNKNOWN, root);
                        }
                        return value.value().orElseThrow();
                    })
                    .toCompletableFuture();
        }

        private CompletableFuture<ExternalMutationResultV1> dispatch(
                Optional<VersionedValue> predecessor, Sha256Digest root) {
            return budget.requireNativeOwner(binding, nativeOwner)
                    .thenCompose(unused -> binding.revalidate())
                    .thenCompose(unused -> {
                        dispatched = true;
                        return metadata.compareAndSet(predecessor, key, candidate);
                    })
                    .thenCompose(outcome -> {
                        if (outcome == MutationOutcome.APPLIED_EXACT) {
                            return metadata.read(key).thenApply(value -> {
                                if (!value.map(VersionedValue::canonicalStoredBytes)
                                        .equals(Optional.of(candidate))) {
                                    return new ExternalMutationResultV1(
                                            ExternalMutationOutcomeV1.RESPONSE_UNKNOWN, root);
                                }
                                terminal = new ExternalMutationResultV1(ExternalMutationOutcomeV1.APPLIED_EXACT, root);
                                return terminal;
                            });
                        }
                        if (outcome == MutationOutcome.DEFINITIVE_CONFLICT
                                || outcome == MutationOutcome.PREDECESSOR_UNCHANGED) {
                            terminal = new ExternalMutationResultV1(
                                    ExternalMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED, root);
                            return CompletableFuture.completedFuture(terminal);
                        }
                        return CompletableFuture.completedFuture(
                                new ExternalMutationResultV1(ExternalMutationOutcomeV1.RESPONSE_UNKNOWN, root));
                    })
                    .toCompletableFuture();
        }

        private CompletableFuture<Void> reconcilePhysical() {
            if (physicalGuard == null) {
                return CompletableFuture.completedFuture(null);
            }
            if (physicalResult == null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("original Native source physical dispatch remains unknown"));
            }
            if (physicalResult.unresolvedTargets().isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            var work = physicalResult.terminalCleanup().isPresent()
                    ? physicalResult.terminalCleanup().orElseThrow().reconcile()
                    : terminal != null
                            ? physicalGuard.reconcileOperationWithContexts(
                                    physicalContexts,
                                    physicalResult.operationId(),
                                    terminal.authoritativeKeySetRootSha256())
                            : CompletableFuture
                                    .<java.util.List<com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2>>
                                            failedFuture(new IllegalStateException(
                                    "original Native source physical SDK outcome remains unknown"));
            return work.thenAccept(left -> {
                        if (!left.isEmpty()) {
                            throw new IllegalStateException(
                                    "original Native source physical ticket cleanup remains unknown");
                        }
                    })
                    .toCompletableFuture();
        }

        private CompletableFuture<Void> reconcile() {
            return settled.thenCompose(unused -> {
                if (!enteredGuard) {
                    release();
                    return CompletableFuture.completedFuture(null);
                }
                if (terminal == null) {
                    if (!dispatched) {
                        return physicalAdmission
                                .map(value -> value.reconcile())
                                .orElseGet(() -> CompletableFuture.completedFuture(null))
                                .thenCompose(done -> reconcilePhysical())
                                .thenCompose(done -> guard.reconcileUndispatched(request))
                                .thenAccept(value -> {
                                    if (value.outcome() != GuardOutcomeV1.UNDISPATCHED_TICKET_FENCED_AND_CLEARED) {
                                        throw new IllegalStateException(
                                                "original Native control no-dispatch ticket remains unknown");
                                    }
                                    release();
                                });
                    }
                    return metadata.read(key).thenCompose(observed -> {
                        if (!observed.map(VersionedValue::canonicalStoredBytes).equals(Optional.of(candidate))
                                || observed.map(VersionedValue::metadataVersion).equals(predecessorVersion)) {
                            return CompletableFuture.failedFuture(new IllegalStateException(
                                    "original Native control SDK outcome remains unknown; redispatch is forbidden"));
                        }
                        // The settled original conditional PUT cannot apply after this distinct predecessor
                        // revision. Exact candidate observation supplies EXISTING_EXACT without a SDK redispatch.
                        terminal = new ExternalMutationResultV1(
                                ExternalMutationOutcomeV1.EXISTING_EXACT, request.externalAuthorityRootSha256());
                        return reconcileTerminal();
                    });
                }
                return reconcileTerminal();
            });
        }

        private CompletableFuture<Void> reconcileTerminal() {
            return reconcilePhysical()
                    .thenCompose(unused -> guard.reconcileCompleted(request, terminal))
                    .thenAccept(value -> {
                        if (value.outcome() != GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED
                                && value.outcome() != GuardOutcomeV1.MUTATION_NOT_APPLIED_AND_TICKET_CLEARED) {
                            throw new IllegalStateException("original Native control ticket cleanup remains unknown");
                        }
                        release();
                    })
                    .toCompletableFuture();
        }

        private void release() {
            synchronized (PulsarNativeControlWriterV2.this) {
                if (pending.remove(this)) {
                    pendingBytes -= bytes;
                }
            }
        }
    }
}
