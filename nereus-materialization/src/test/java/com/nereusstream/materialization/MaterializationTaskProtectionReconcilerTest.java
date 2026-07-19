/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.GenerationPublicationTestSupport.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MaterializationTaskProtectionReconcilerTest {
    @Test
    void reconstructsBookKeeperProtectionThroughTheProviderRegistry() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            var target = new BookKeeperEntryRangeReadTarget(
                    1,
                    "bk-cluster",
                    41,
                    7,
                    2,
                    BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                    MaterializationPlannerTestSupport.sha('b'));
            var candidate = MaterializationPlannerTestSupport.zero(
                    "/index/bk-source",
                    0,
                    2,
                    0,
                    100,
                    2,
                    target);
            GenerationMetadataStore durable =
                    GenerationMetadataStoreTestFactory.inMemory(
                            GenerationPublicationTestSupport.CLOCK);
            GenerationMetadataStore generations =
                    MaterializationPlannerTestSupport.generationStore(
                            List.of(candidate), List.of(), durable);
            try {
                MaterializationTask task = MaterializationPlannerTestSupport.planner(
                                List.of(candidate), List.of(), 0, 2)
                        .plan(
                                MaterializationPlannerTestSupport.STREAM,
                                candidate.value().range(),
                                MaterializationPlannerTestSupport.policy(),
                                1)
                        .join()
                        .getFirst();
                MaterializationTaskStore tasks = new MaterializationTaskStore(
                        MaterializationPlannerTestSupport.CLUSTER,
                        generations,
                        GenerationPublicationTestSupport.CLOCK);
                VersionedMaterializationTask planned = tasks.create(task).join();
                VersionedMaterializationTask claimed = tasks.compareAndSet(
                                MaterializationRecordMapper.claimed(
                                        planned.value(),
                                        "c".repeat(26),
                                        "d".repeat(26),
                                        1_000,
                                        31_000),
                                planned.metadataVersion())
                        .join();
                AtomicInteger acquisitions = new AtomicInteger();
                MaterializationSourceProtectionAdapter<BookKeeperEntryRangeReadTarget> adapter =
                        bookKeeperProtectionAdapter(acquisitions);
                DefaultMaterializationTaskProtectionReconciler reconciler =
                        new DefaultMaterializationTaskProtectionReconciler(
                                MaterializationPlannerTestSupport.CLUSTER,
                                tasks,
                                generations,
                                (readTarget, view) -> CompletableFuture.failedFuture(
                                        new AssertionError("BK source must not resolve as an Object identity")),
                                context.protections(),
                                new MaterializationSourceProtectionRegistry(List.of(adapter)),
                                Duration.ofSeconds(10),
                                context.scheduler());

                MaterializationTaskProtections recovered = reconciler.reconcile(claimed).join();

                assertThat(acquisitions).hasValue(1);
                assertThat(recovered.sources()).singleElement().satisfies(protection -> {
                    assertThat(protection.targetType())
                            .isEqualTo(ReadTargetType.BOOKKEEPER_ENTRY_RANGE);
                    assertThat(protection.owner())
                            .isEqualTo(MaterializationProtectionIdentities.taskOwner(claimed));
                    assertThat(protection.providerHandle()).isEqualTo("bk-protection");
                });
                assertThat(recovered.output()).isEmpty();
            } finally {
                durable.close();
            }
        }
    }

    @Test
    void reconstructsEveryProtectionAndConvergesAcrossTaskStateCrashCuts() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            MaterializationTaskStore tasks = new MaterializationTaskStore(
                    CLUSTER,
                    context.generations(),
                    GenerationPublicationTestSupport.CLOCK);
            DefaultMaterializationTaskProtectionReconciler reconciler =
                    new DefaultMaterializationTaskProtectionReconciler(
                            CLUSTER,
                            tasks,
                            context.generations(),
                            (target, view) -> context.physical()
                                    .getRoot(CLUSTER, ObjectKeyHash.from(target.objectKey()))
                                    .thenApply(root -> PhysicalObjectIdentity.from(
                                            root.orElseThrow().value())),
                            context.protections(),
                            Duration.ofSeconds(30),
                            context.scheduler());
            VersionedMaterializationTask ready = tasks.get(
                    context.task().streamId(), context.task().taskId()).join().orElseThrow();

            MaterializationTaskProtections readyProtections = reconciler.reconcile(ready).join();

            assertThat(readyProtections.sources()).hasSize(context.task().sources().size());
            assertThat(readyProtections.output()).isPresent();
            assertThat(readyProtections.sources())
                    .allSatisfy(protection -> assertThat(protection.owner())
                            .isEqualTo(MaterializationProtectionIdentities.taskOwner(ready)));
            assertThat(readyProtections.output().orElseThrow().owner())
                    .isEqualTo(MaterializationProtectionIdentities.taskOwner(ready));

            VersionedMaterializationTask publishing = tasks.compareAndSet(
                    MaterializationRecordMapper.publishing(
                            ready.value(), new PublicationId("p".repeat(26)), 1_100),
                    ready.metadataVersion()).join();
            VersionedMaterializationTask attached = tasks.compareAndSet(
                    MaterializationRecordMapper.attachGeneration(
                            publishing.value(), 7, 1_200),
                    publishing.metadataVersion()).join();

            MaterializationTaskProtections recovered = reconciler.reconcile(attached).join();

            assertThat(recovered.sources()).allSatisfy(protection -> {
                assertThat(protection.owner())
                        .isEqualTo(MaterializationProtectionIdentities.taskOwner(attached));
                assertThat(protection.metadataVersion())
                        .isGreaterThan(readyProtections.sources().get(0).metadataVersion());
            });
            assertThat(recovered.output().orElseThrow().owner())
                    .isEqualTo(MaterializationProtectionIdentities.taskOwner(attached));
            assertThat(recovered.output().orElseThrow().metadataVersion())
                    .isGreaterThan(readyProtections.output().orElseThrow().metadataVersion());

            assertThatThrownBy(() -> reconciler.reconcile(ready).join())
                    .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);

            MaterializationTaskProtections duplicate = reconciler.reconcile(attached).join();
            assertThat(duplicate.sources()).isEqualTo(recovered.sources());
            assertThat(duplicate.output()).isEqualTo(recovered.output());
        }
    }

    private static MaterializationSourceProtectionAdapter<BookKeeperEntryRangeReadTarget>
            bookKeeperProtectionAdapter(AtomicInteger acquisitions) {
        return new MaterializationSourceProtectionAdapter<>() {
            @Override
            public ReadTargetType targetType() {
                return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
            }

            @Override
            public Class<BookKeeperEntryRangeReadTarget> targetClass() {
                return BookKeeperEntryRangeReadTarget.class;
            }

            @Override
            public CompletableFuture<MaterializationSourceProtection> acquireOrTransfer(
                    com.nereusstream.api.StreamId streamId,
                    SourceGeneration source,
                    String referenceId,
                    ObjectProtectionOwner owner,
                    OwnerRevalidator ownerRevalidator) {
                acquisitions.incrementAndGet();
                return ownerRevalidator.revalidate(owner)
                        .thenApply(ignored -> new MaterializationSourceProtection(
                                targetType(), referenceId, owner, 3, "bk-protection"));
            }

            @Override
            public CompletableFuture<MaterializationSourceProtection> revalidate(
                    MaterializationSourceProtection protection,
                    OwnerRevalidator ownerRevalidator) {
                return CompletableFuture.failedFuture(new AssertionError("not used"));
            }

            @Override
            public CompletableFuture<MaterializationSourceProtection> transfer(
                    MaterializationSourceProtection protection,
                    ObjectProtectionOwner newOwner,
                    OwnerRevalidator newOwnerRevalidator) {
                return CompletableFuture.failedFuture(new AssertionError("not used"));
            }

            @Override
            public CompletableFuture<Void> release(
                    MaterializationSourceProtection protection,
                    RemovalAuthorizer removalAuthorizer) {
                return CompletableFuture.failedFuture(new AssertionError("not used"));
            }
        };
    }
}
