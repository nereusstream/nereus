/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.GenerationPublicationTestSupport.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedMaterializationCheckpoint;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TerminalWorkflowMetadataRetirementTest {
    private static final Clock RETIREMENT_CLOCK = Clock.fixed(
            Instant.ofEpochMilli(3_000), ZoneOffset.UTC);

    @Test
    void requiresCompletePublishedProofThenReleasesTemporaryProtectionsAndConvergesLostDeleteResponse() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            context.committer(
                            context.generations(),
                            GenerationPublicationTestSupport.successfulGuard())
                    .publish(context.task(), context.output())
                    .join();
            GenerationMetadataStore lostDelete = loseFirstTaskDeleteResponse(
                    context.generations());
            MaterializationTaskStore tasks = new MaterializationTaskStore(
                    CLUSTER, lostDelete, GenerationPublicationTestSupport.CLOCK);
            PhysicalObjectMetadataStore lostProtectionDelete =
                    loseFirstProtectionDeleteResponse(context.physical());
            DefaultTerminalWorkflowMetadataRetirer retirer = new DefaultTerminalWorkflowMetadataRetirer(
                    CLUSTER,
                    tasks,
                    lostDelete,
                    lostProtectionDelete,
                    Duration.ofMillis(500),
                    1,
                    Duration.ofSeconds(10),
                    context.scheduler(),
                    RETIREMENT_CLOCK);

            var beforeCheckpoint = retirer.retire(
                            context.task().streamId(),
                            context.task().policy(),
                            0,
                            MaterializationTaskMutationGuard.noOp())
                    .join();
            assertThat(beforeCheckpoint.tasksScanned()).isOne();
            assertThat(beforeCheckpoint.tasksEligible()).isZero();
            assertThat(tasks.get(context.task().streamId(), context.task().taskId()).join())
                    .isPresent();
            assertThat(taskOwnedProtections(context)).hasSize(2);

            advanceCheckpoint(context);
            createRetirableStats(context);
            VersionedMaterializationCheckpoint oldCheckpoint = context.generations()
                    .getOrCreateMaterializationCheckpoint(
                            CLUSTER,
                            context.task().streamId(),
                            "old-policy",
                            7,
                            context.task().policyDigestSha256())
                    .join();
            AtomicInteger guardCalls = new AtomicInteger();
            var retired = retirer.retire(
                            context.task().streamId(),
                            context.task().policy(),
                            2,
                            () -> {
                                guardCalls.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            })
                    .join();

            assertThat(retired.tasksScanned()).isOne();
            assertThat(retired.tasksEligible()).isOne();
            assertThat(retired.tasksRetired()).isOne();
            assertThat(retired.protectionsReleased()).isEqualTo(2);
            assertThat(retired.retentionStatsScanned()).isEqualTo(2);
            assertThat(retired.retentionStatsRetired()).isEqualTo(2);
            assertThat(retired.checkpointsScanned()).isEqualTo(2);
            assertThat(retired.checkpointsRetired()).isOne();
            assertThat(guardCalls.get()).isGreaterThanOrEqualTo(4);
            assertThat(tasks.get(context.task().streamId(), context.task().taskId()).join())
                    .isEmpty();
            assertThat(taskOwnedProtections(context)).isEmpty();
            assertThat(context.physical().scanProtections(
                            CLUSTER,
                            context.output().objectKeyHash(),
                            Optional.empty(),
                            1_000).join().values())
                    .singleElement()
                    .satisfies(protection -> assertThat(protection.value().protectionTypeId())
                            .isEqualTo(ObjectProtectionType.VISIBLE_GENERATION.wireId()));
            assertThat(context.generations().getRangeRetentionStats(
                            CLUSTER, context.task().streamId(), 2, 1).join())
                    .isEmpty();
            assertThat(context.generations().getRangeRetentionStats(
                            CLUSTER, context.task().streamId(), 4, 2).join())
                    .isEmpty();
            assertThat(context.generations().getMaterializationCheckpoint(
                            CLUSTER,
                            context.task().streamId(),
                            oldCheckpoint.value().policyId(),
                            oldCheckpoint.value().policyVersion()).join())
                    .isEmpty();

            var replay = retirer.retire(
                            context.task().streamId(),
                            context.task().policy(),
                            2,
                            MaterializationTaskMutationGuard.noOp())
                    .join();
            assertThat(replay.tasksScanned()).isZero();
            assertThat(replay.retentionStatsScanned()).isZero();
            assertThat(replay.checkpointsScanned()).isOne();
            assertThat(replay.checkpointsRetired()).isZero();
        }
    }

    @Test
    void retiresTerminalFailureOnlyWhenNoPublicationIndexReferencesItsTaskOrOutput() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            MaterializationPolicy base = context.task().policy();
            MaterializationPolicy failedPolicy = new MaterializationPolicy(
                    "terminal-policy",
                    2,
                    base.view(),
                    base.taskKind(),
                    base.targetPhysicalFormat(),
                    base.minMergeSourceRanges(),
                    base.maxSourceRanges(),
                    base.maxRangeRecords(),
                    base.targetObjectBytes(),
                    base.targetRowGroupRecords(),
                    base.compression(),
                    base.topicCompaction());
            MaterializationTask failedTask = MaterializationTask.create(
                    context.task().streamId(),
                    context.task().coverage(),
                    context.task().sources(),
                    failedPolicy);
            MaterializationTaskStore tasks = new MaterializationTaskStore(
                    CLUSTER,
                    context.generations(),
                    GenerationPublicationTestSupport.CLOCK);
            var planned = tasks.create(failedTask).join();
            var claimed = tasks.compareAndSet(
                    MaterializationRecordMapper.claimed(
                            planned.value(),
                            "x".repeat(26),
                            "y".repeat(26),
                            500,
                            2_000),
                    planned.metadataVersion()).join();
            ObjectSliceReadTarget sourceTarget = (ObjectSliceReadTarget) failedTask
                    .sources().get(0).readTarget();
            PhysicalObjectIdentity sourceIdentity = PhysicalObjectIdentity.from(
                    context.physical().getRoot(
                                    CLUSTER,
                                    ObjectKeyHash.from(sourceTarget.objectKey()))
                            .join().orElseThrow().value());
            context.protections().acquire(
                    new ObjectProtectionRequest(
                            sourceIdentity,
                            ObjectProtectionType.MATERIALIZATION_SOURCE,
                            MaterializationProtectionIdentities.sourceReferenceId(
                                    CLUSTER, failedTask, failedTask.sources().get(0)),
                            MaterializationProtectionIdentities.taskOwner(claimed),
                            0),
                    ignored -> CompletableFuture.completedFuture(null)).join();
            var terminal = tasks.compareAndSet(
                    MaterializationRecordMapper.failedClaim(
                            claimed.value(),
                            TaskLifecycle.TERMINAL_FAILED,
                            TaskFailureClass.UNSUPPORTED_MAPPING,
                            "unsupported mapping",
                            0,
                            1_000),
                    claimed.metadataVersion()).join();
            DefaultTerminalWorkflowMetadataRetirer retirer = new DefaultTerminalWorkflowMetadataRetirer(
                    CLUSTER,
                    tasks,
                    context.generations(),
                    context.physical(),
                    Duration.ofMillis(500),
                    1,
                    Duration.ofSeconds(10),
                    context.scheduler(),
                    RETIREMENT_CLOCK);

            var result = retirer.retire(
                            failedTask.streamId(),
                            failedPolicy,
                            0,
                            MaterializationTaskMutationGuard.noOp())
                    .join();

            assertThat(result.tasksScanned()).isEqualTo(2);
            assertThat(result.tasksEligible()).isOne();
            assertThat(result.tasksRetired()).isOne();
            assertThat(result.protectionsReleased()).isOne();
            assertThat(tasks.get(failedTask.streamId(), terminal.value().taskId()).join())
                    .isEmpty();
            assertThat(tasks.get(
                            context.task().streamId(), context.task().taskId()).join())
                    .isPresent();
        }
    }

    @Test
    void retiresTerminalTaskWithProviderOwnedBookKeeperSourceProtection() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                    1,
                    "primary",
                    71,
                    3,
                    2,
                    BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                    sha('7'));
            SourceGeneration source = new SourceGeneration(
                    ReadView.COMMITTED,
                    new OffsetRange(0, 2),
                    0,
                    11,
                    "/indexes/bk-terminal-source",
                    0,
                    sha('8'),
                    target,
                    new Checksum(
                            ChecksumType.SHA256,
                            ReadTargetCodecRegistry.phase15()
                                    .encode(target)
                                    .identityChecksumValue()),
                    Optional.empty(),
                    PayloadFormat.OPAQUE_RECORD_BATCH,
                    Optional.empty(),
                    2,
                    2,
                    10,
                    List.of(),
                    0,
                    10);
            MaterializationTask task = MaterializationTask.create(
                    GenerationPublicationTestSupport.STREAM,
                    source.range(),
                    List.of(source),
                    context.task().policy());
            var created = context.generations().createTask(
                    CLUSTER, MaterializationRecordMapper.plannedTask(task, 100)).join();
            var claimed = context.generations().compareAndSetTask(
                    CLUSTER,
                    MaterializationRecordMapper.claimed(
                            created.value(), "q".repeat(26), "r".repeat(26), 500, 2_000),
                    created.metadataVersion()).join();
            var terminal = context.generations().compareAndSetTask(
                    CLUSTER,
                    MaterializationRecordMapper.failedClaim(
                            claimed.value(),
                            TaskLifecycle.TERMINAL_FAILED,
                            TaskFailureClass.UNSUPPORTED_MAPPING,
                            "terminal BK source",
                            0,
                            1_000),
                    claimed.metadataVersion()).join();
            String referenceId = MaterializationProtectionIdentities.sourceReferenceId(
                    CLUSTER, task, source);
            TrackingProviderProtectionAdapter adapter = new TrackingProviderProtectionAdapter(
                    source,
                    new MaterializationSourceProtection(
                            ReadTargetType.BOOKKEEPER_ENTRY_RANGE,
                            referenceId,
                            MaterializationProtectionIdentities.taskOwner(terminal),
                            4,
                            "bk-provider-handle"));
            DefaultTerminalWorkflowMetadataRetirer retirer = new DefaultTerminalWorkflowMetadataRetirer(
                    CLUSTER,
                    new MaterializationTaskStore(
                            CLUSTER, context.generations(), GenerationPublicationTestSupport.CLOCK),
                    context.generations(),
                    context.physical(),
                    new MaterializationSourceProtectionRegistry(List.of(adapter)),
                    Duration.ofMillis(500),
                    1,
                    Duration.ofSeconds(10),
                    context.scheduler(),
                    RETIREMENT_CLOCK);

            var result = retirer.retire(
                            task.streamId(),
                            task.policy(),
                            0,
                            MaterializationTaskMutationGuard.noOp())
                    .join();

            assertThat(result.tasksEligible()).isOne();
            assertThat(result.tasksRetired()).isOne();
            assertThat(result.protectionsReleased()).isOne();
            assertThat(adapter.findCalls).hasValueGreaterThanOrEqualTo(3);
            assertThat(adapter.releaseCalls).hasValue(1);
            assertThat(adapter.current).isEmpty();
            assertThat(context.generations().getTask(CLUSTER, task.streamId(), task.taskId()).join())
                    .isEmpty();
        }
    }

    private static final class TrackingProviderProtectionAdapter
            implements MaterializationSourceProtectionAdapter<BookKeeperEntryRangeReadTarget> {
        private final SourceGeneration expectedSource;
        private final AtomicInteger findCalls = new AtomicInteger();
        private final AtomicInteger releaseCalls = new AtomicInteger();
        private Optional<MaterializationSourceProtection> current;

        private TrackingProviderProtectionAdapter(
                SourceGeneration expectedSource,
                MaterializationSourceProtection current) {
            this.expectedSource = expectedSource;
            this.current = Optional.of(current);
        }

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
                StreamId streamId,
                SourceGeneration source,
                String referenceId,
                ObjectProtectionOwner owner,
                OwnerRevalidator ownerRevalidator) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Optional<MaterializationSourceProtection>> findExisting(
                StreamId streamId,
                SourceGeneration source,
                String referenceId) {
            assertThat(streamId).isEqualTo(GenerationPublicationTestSupport.STREAM);
            assertThat(source).isEqualTo(expectedSource);
            current.ifPresent(value -> assertThat(value.referenceId()).isEqualTo(referenceId));
            findCalls.incrementAndGet();
            return CompletableFuture.completedFuture(current);
        }

        @Override
        public CompletableFuture<MaterializationSourceProtection> revalidate(
                MaterializationSourceProtection protection,
                OwnerRevalidator ownerRevalidator) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<MaterializationSourceProtection> transfer(
                MaterializationSourceProtection protection,
                ObjectProtectionOwner newOwner,
                OwnerRevalidator newOwnerRevalidator) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> release(
                MaterializationSourceProtection protection,
                RemovalAuthorizer removalAuthorizer) {
            assertThat(current).contains(protection);
            return removalAuthorizer.authorize(protection).thenRun(() -> {
                current = Optional.empty();
                releaseCalls.incrementAndGet();
            });
        }
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static void advanceCheckpoint(GenerationPublicationTestSupport.Context context) {
        VersionedMaterializationCheckpoint current = context.generations()
                .getOrCreateMaterializationCheckpoint(
                        CLUSTER,
                        context.task().streamId(),
                        context.task().policy().policyId(),
                        context.task().policy().policyVersion(),
                        context.task().policyDigestSha256())
                .join();
        MaterializationCheckpointRecord value = current.value();
        context.generations().compareAndSetMaterializationCheckpoint(
                CLUSTER,
                new MaterializationCheckpointRecord(
                        value.schemaVersion(),
                        value.streamId(),
                        value.policyId(),
                        value.policyVersion(),
                        value.policySha256(),
                        context.task().coverage().endOffset(),
                        context.task().taskSequence(),
                        context.task().taskSequence(),
                        context.task().taskId(),
                        value.updatedAtMillis(),
                        0),
                current.metadataVersion()).join();
    }

    private static void createRetirableStats(GenerationPublicationTestSupport.Context context) {
        var source = context.task().sources().get(0);
        context.generations().createRangeRetentionStats(
                CLUSTER,
                new RangeRetentionStatsRecord(
                        1,
                        context.task().streamId().value(),
                        0,
                        2,
                        1,
                        0,
                        100,
                        100,
                        200,
                        source.indexKey(),
                        source.indexRecordSha256().value(),
                        source.indexMetadataVersion(),
                        "retirement-test",
                        1_000,
                        0)).join();
        String missingIndex = new F4Keyspace(CLUSTER).generationIndexKey(
                context.task().streamId(), ReadView.COMMITTED, 4, 99);
        context.generations().createRangeRetentionStats(
                CLUSTER,
                new RangeRetentionStatsRecord(
                        1,
                        context.task().streamId().value(),
                        2,
                        4,
                        2,
                        100,
                        200,
                        200,
                        300,
                        missingIndex,
                        "9".repeat(64),
                        999,
                        "retirement-test",
                        1_000,
                        0)).join();
    }

    private static List<com.nereusstream.metadata.oxia.VersionedObjectProtection> taskOwnedProtections(
            GenerationPublicationTestSupport.Context context) {
        ObjectSliceReadTarget source = (ObjectSliceReadTarget) context.task()
                .sources().get(0).readTarget();
        return java.util.stream.Stream.concat(
                        context.physical().scanProtections(
                                        CLUSTER,
                                        ObjectKeyHash.from(source.objectKey()),
                                        Optional.empty(),
                                        1_000).join().values().stream(),
                        context.physical().scanProtections(
                                        CLUSTER,
                                        context.output().objectKeyHash(),
                                        Optional.empty(),
                                        1_000).join().values().stream())
                .filter(protection -> protection.value().ownerKey().contains(
                        "/materialization/v1/tasks/"))
                .toList();
    }

    private static GenerationMetadataStore loseFirstTaskDeleteResponse(
            GenerationMetadataStore delegate) {
        AtomicBoolean lose = new AtomicBoolean(true);
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(delegate, args);
                    }
                    Object result;
                    try {
                        result = method.invoke(delegate, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (method.getName().equals("deleteTask")
                            && lose.compareAndSet(true, false)) {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<Void> exact = (CompletableFuture<Void>) result;
                        return exact.thenCompose(ignored -> CompletableFuture.failedFuture(
                                new NereusException(
                                        ErrorCode.METADATA_UNAVAILABLE,
                                        true,
                                        "lost terminal task delete response")));
                    }
                    return result;
                });
    }

    private static PhysicalObjectMetadataStore loseFirstProtectionDeleteResponse(
            PhysicalObjectMetadataStore delegate) {
        AtomicBoolean lose = new AtomicBoolean(true);
        return (PhysicalObjectMetadataStore) Proxy.newProxyInstance(
                PhysicalObjectMetadataStore.class.getClassLoader(),
                new Class<?>[] {PhysicalObjectMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(delegate, args);
                    }
                    Object result;
                    try {
                        result = method.invoke(delegate, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (method.getName().equals("deleteProtection")
                            && lose.compareAndSet(true, false)) {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<Void> exact = (CompletableFuture<Void>) result;
                        return exact.thenCompose(ignored -> CompletableFuture.failedFuture(
                                new NereusException(
                                        ErrorCode.METADATA_UNAVAILABLE,
                                        true,
                                        "lost terminal protection delete response")));
                    }
                    return result;
                });
    }
}
