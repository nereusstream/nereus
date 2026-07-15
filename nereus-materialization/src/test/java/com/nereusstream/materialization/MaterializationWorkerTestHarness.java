/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.compacted.CompactedObjectWriter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

/** Shared deterministic worker fixture for failure-injection and claim-interleaving tests. */
final class MaterializationWorkerTestHarness {
    static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    private MaterializationWorkerTestHarness() {
    }

    static Scenario scenario(UnaryOperator<GenerationMetadataStore> decorator) {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/harness-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/harness-4", 2, 4, 100, 100, 4));
        MaterializationTask task = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 4)
                .plan(
                        STREAM,
                        new OffsetRange(0, 4),
                        MaterializationPlannerTestSupport.policy(),
                        1)
                .join()
                .getFirst();
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore composite = MaterializationPlannerTestSupport.generationStore(
                candidates, List.of(), durable);
        GenerationMetadataStore generations = decorator.apply(composite);
        MaterializationTaskStore tasks = new MaterializationTaskStore(CLUSTER, generations, CLOCK);
        tasks.create(task).join();
        return new Scenario(task, generations, tasks);
    }

    static DefaultMaterializationWorker worker(
            String processRunId,
            Scenario scenario,
            TrackingProtections protections,
            TrackingExactReader exactReader,
            CompactedObjectWriter writer,
            ObjectStore objectStore,
            MaterializationOutputVerifier verifier,
            WorkerClaimIdGenerator claimIds,
            ScheduledExecutorService scheduler) {
        return new DefaultMaterializationWorker(
                CLUSTER,
                processRunId,
                scenario.tasks(),
                scenario.generations(),
                (target, view) -> CompletableFuture.completedFuture(sourceIdentity(target)),
                protections,
                ignored -> exactReader,
                writer,
                objectStore,
                verifier,
                claimIds,
                1,
                64 * 1024,
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                Duration.ofSeconds(1),
                3,
                Duration.ofSeconds(10),
                "worker-model-test",
                scheduler,
                Runnable::run,
                CLOCK);
    }

    static PhysicalObjectIdentity sourceIdentity(ObjectSliceReadTarget target) {
        return PhysicalObjectIdentity.create(
                target.objectKey(),
                Optional.of(target.objectId()),
                PhysicalObjectKind.OBJECT_WAL,
                100,
                new Checksum(ChecksumType.CRC32C, "11223344"),
                Optional.empty(),
                Optional.empty());
    }

    record Scenario(
            MaterializationTask task,
            GenerationMetadataStore generations,
            MaterializationTaskStore tasks) {
    }

    static final class TrackingExactReader implements ExactSourceRangeReader {
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maximumActive = new AtomicInteger();
        private final AtomicInteger completedSources = new AtomicInteger();

        @Override
        public CompletableFuture<ExactSourceRead> read(
                SourceGeneration source,
                ReadOptions options) {
            int current = active.incrementAndGet();
            maximumActive.accumulateAndGet(current, Math::max);
            return CompletableFuture.completedFuture(new ExactSourceRead() {
                private final CompletableFuture<ExactSourceReadSummary> completion =
                        new CompletableFuture<>();

                @Override
                public SourceGeneration source() {
                    return source;
                }

                @Override
                public Flow.Publisher<ReadBatch> batches() {
                    return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
                        private long cursor = source.range().startOffset();
                        private boolean terminal;

                        @Override
                        public void request(long count) {
                            if (terminal) {
                                return;
                            }
                            if (count <= 0) {
                                terminal = true;
                                subscriber.onError(new IllegalArgumentException("demand must be positive"));
                                return;
                            }
                            long emitted = 0;
                            while (!terminal
                                    && emitted < count
                                    && cursor < source.range().endOffset()) {
                                ObjectSliceReadTarget target =
                                        (ObjectSliceReadTarget) source.readTarget();
                                byte[] payload = new byte[50];
                                java.util.Arrays.fill(payload, (byte) (cursor + 1));
                                subscriber.onNext(new ReadBatch(
                                        new OffsetRange(cursor, cursor + 1),
                                        source.payloadFormat(),
                                        payload,
                                        source.schemaRefs(),
                                        target.entryIndexRef(),
                                        source.projectionRef(),
                                        target.objectId(),
                                        target.objectOffset(),
                                        target.objectLength()));
                                cursor++;
                                emitted++;
                            }
                            if (!terminal && cursor == source.range().endOffset()) {
                                terminal = true;
                                active.decrementAndGet();
                                completedSources.incrementAndGet();
                                completion.complete(new ExactSourceReadSummary(
                                        source.range(),
                                        source.recordCount(),
                                        source.entryCount(),
                                        source.logicalBytes(),
                                        MaterializationPlannerTestSupport.sha('a')));
                                subscriber.onComplete();
                            }
                        }

                        @Override
                        public void cancel() {
                            if (!terminal) {
                                terminal = true;
                                active.decrementAndGet();
                            }
                        }
                    });
                }

                @Override
                public CompletableFuture<ExactSourceReadSummary> completion() {
                    return completion;
                }

                @Override
                public void close() {
                }
            });
        }

        int maximumActive() {
            return maximumActive.get();
        }

        int completedSources() {
            return completedSources.get();
        }
    }

    static final class TrackingProtections implements ObjectProtectionManager {
        private final AtomicInteger acquired = new AtomicInteger();
        private final AtomicInteger transferred = new AtomicInteger();
        private final AtomicInteger released = new AtomicInteger();
        private final AtomicInteger version = new AtomicInteger();

        @Override
        public CompletableFuture<ObjectProtection> acquire(
                ObjectProtectionRequest request,
                OwnerRevalidator ownerRevalidator) {
            return ownerRevalidator.revalidate(request.owner()).thenApply(ignored -> {
                acquired.incrementAndGet();
                return protection(
                        request.object(), request.identity(), request.owner(), version.getAndIncrement());
            });
        }

        @Override
        public CompletableFuture<ObjectProtection> acquireOrTransfer(
                ObjectProtectionRequest request,
                OwnerRevalidator ownerRevalidator) {
            return acquire(request, ownerRevalidator);
        }

        @Override
        public CompletableFuture<ObjectProtection> revalidate(
                ObjectProtection protection,
                OwnerRevalidator ownerRevalidator) {
            return ownerRevalidator.revalidate(protection.owner()).thenApply(ignored -> protection);
        }

        @Override
        public CompletableFuture<ObjectProtection> transfer(
                ObjectProtection protection,
                ObjectProtectionOwner newOwner,
                OwnerRevalidator newOwnerRevalidator) {
            return newOwnerRevalidator.revalidate(newOwner).thenApply(ignored -> {
                transferred.incrementAndGet();
                return protection(
                        protection.object(),
                        protection.identity(),
                        newOwner,
                        version.getAndIncrement());
            });
        }

        @Override
        public CompletableFuture<Void> release(
                ObjectProtection protection,
                RemovalAuthorizer removalAuthorizer) {
            return removalAuthorizer.authorizeRemoval(protection).thenRun(released::incrementAndGet);
        }

        @Override
        public void close() {
        }

        int acquired() {
            return acquired.get();
        }

        int transferred() {
            return transferred.get();
        }

        int released() {
            return released.get();
        }

        private static ObjectProtection protection(
                PhysicalObjectIdentity object,
                ObjectProtectionIdentity identity,
                ObjectProtectionOwner owner,
                long version) {
            return new ObjectProtection(
                    object,
                    identity,
                    owner,
                    1,
                    1_000,
                    0,
                    version,
                    new Checksum(ChecksumType.SHA256, "a".repeat(64)));
        }
    }

}
