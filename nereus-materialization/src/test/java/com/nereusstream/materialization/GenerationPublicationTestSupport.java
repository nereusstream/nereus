/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.AllocatedGeneration;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.metadata.oxia.records.WorkerClaimRecord;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class GenerationPublicationTestSupport {
    static final String CLUSTER = "cluster-publication";
    static final StreamId STREAM = new StreamId("stream-publication");
    static final ProjectionRef PROJECTION = new ProjectionRef(
            ProjectionType.VIRTUAL_LEDGER, "projection-publication");
    static final Checksum PROJECTION_SHA = sha("c");
    static final Checksum REFERENCE_DOMAINS_SHA = sha("f");
    static final String CLAIM_ID = "c".repeat(26);
    static final String PROCESS_ID = "d".repeat(26);
    static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    private GenerationPublicationTestSupport() {
    }

    static Context context() {
        return context(false);
    }

    static Context topicContext() {
        return context(true);
    }

    private static Context context(boolean topicCompacted) {
        GenerationMetadataStore generations = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        FakePhysicalObjectMetadataStore physical = new FakePhysicalObjectMetadataStore();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        ObjectProtectionManager protections = new DefaultObjectProtectionManager(
                CLUSTER,
                physical,
                Duration.ofMinutes(5),
                Duration.ofSeconds(1),
                Duration.ofMinutes(10),
                CLOCK);

        ObjectSliceReadTarget sourceTarget = target(
                "source-object",
                "objects/publication/source-object",
                "source-slice",
                "11111111");
        PhysicalObjectIdentity sourceIdentity = PhysicalObjectIdentity.create(
                sourceTarget.objectKey(),
                Optional.of(sourceTarget.objectId()),
                PhysicalObjectKind.COMMITTED_COMPACTED,
                sourceTarget.objectLength(),
                new Checksum(ChecksumType.CRC32C, sourceTarget.sliceChecksum().value()),
                Optional.of(sha("a")),
                Optional.of("source-etag"));
        physical.createRoot(CLUSTER, activeRoot(sourceIdentity, 100)).join();

        PublicationId sourcePublication = new PublicationId("s".repeat(26));
        AllocatedGeneration sourceAllocation = generations.allocateGeneration(
                CLUSTER, STREAM, ReadView.COMMITTED, sourcePublication).join();
        Checksum sourcePolicy = sha("b");
        GenerationIndexRecord sourcePrepared = generation(
                sourceTarget,
                sourceAllocation.generation().value(),
                sourcePublication.value(),
                "source-task",
                sourcePolicy,
                GenerationLifecycle.PREPARED,
                100,
                0,
                100);
        VersionedGenerationIndex sourceCreated = generations.createPrepared(
                CLUSTER, sourcePrepared).join();
        VersionedGenerationIndex sourceCommitted = generations.compareAndSetIndex(
                CLUSTER,
                generation(
                        sourceTarget,
                        sourceAllocation.generation().value(),
                        sourcePublication.value(),
                        "source-task",
                        sourcePolicy,
                        GenerationLifecycle.COMMITTED,
                        100,
                        110,
                        110),
                sourceCreated.metadataVersion()).join();

        SourceGeneration source = new SourceGeneration(
                ReadView.COMMITTED,
                new OffsetRange(0, 2),
                sourceCommitted.value().generation(),
                1,
                sourceCommitted.key(),
                sourceCommitted.metadataVersion(),
                sourceCommitted.durableValueSha256(),
                sourceTarget,
                new Checksum(
                        ChecksumType.SHA256,
                        ReadTargetCodecRegistry.phase15()
                                .encode(sourceTarget)
                                .identityChecksumValue()),
                Optional.of(sourcePolicy),
                PayloadFormat.PULSAR_ENTRY_BATCH,
                Optional.of(PROJECTION),
                2,
                2,
                100,
                schemas(),
                0,
                100);
        MaterializationPolicy policy = topicCompacted
                ? new MaterializationPolicy(
                        "policy-topic-publication",
                        1,
                        ReadView.TOPIC_COMPACTED,
                        TaskKind.TOPIC_KEY_COMPACTION,
                        MaterializationPolicy.TOPIC_COMPACTED_FORMAT,
                        2,
                        128,
                        1_048_576,
                        1L << 20,
                        65_536,
                        "ZSTD",
                        Optional.of(new TopicCompactionSpec("latest", 1, "test-key-v1")))
                : new MaterializationPolicy(
                        "policy-publication",
                        1,
                        ReadView.COMMITTED,
                        TaskKind.LOSSLESS_REWRITE,
                        MaterializationPolicy.COMMITTED_FORMAT,
                        2,
                        128,
                        1_048_576,
                        1L << 20,
                        65_536,
                        "ZSTD",
                        Optional.empty());
        MaterializationTask task = MaterializationTask.create(
                STREAM, new OffsetRange(0, 2), List.of(source), policy);

        ObjectSliceReadTarget outputTarget = target(
                "output-object",
                "objects/publication/output-object",
                "output-slice",
                "22222222",
                topicCompacted
                        ? MaterializationPolicy.TOPIC_COMPACTED_FORMAT
                        : MaterializationPolicy.COMMITTED_FORMAT);
        MaterializationOutput output = new MaterializationOutput(
                task.taskId(),
                STREAM,
                policy.view(),
                task.coverage(),
                CLAIM_ID,
                outputTarget.objectId(),
                outputTarget.objectKey(),
                ObjectKeyHash.from(outputTarget.objectKey()),
                outputTarget.objectLength(),
                new Checksum(ChecksumType.CRC32C, outputTarget.sliceChecksum().value()),
                sha("e"),
                "output-etag",
                outputTarget.physicalFormat(),
                outputTarget.logicalFormat(),
                outputTarget,
                new Checksum(
                        ChecksumType.SHA256,
                        ReadTargetCodecRegistry.phase15()
                                .encode(outputTarget)
                                .identityChecksumValue()),
                outputTarget.entryIndexRef(),
                2,
                topicCompacted ? 1 : 2,
                2,
                100,
                schemas(),
                0,
                100,
                task.sourceSetSha256(),
                Optional.of(PROJECTION));

        createOutputReadyTask(generations, task, output);
        generations.createOrVerifyStreamRegistration(
                CLUSTER,
                new MaterializationStreamRegistrationRecord(
                        1,
                        STREAM.value(),
                        MaterializationRecordMapper.projectionIdentity(Optional.of(PROJECTION)),
                        PROJECTION_SHA.value(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        100,
                        1,
                        100,
                        0)).join();

        StreamMetadataSnapshot snapshot = new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/topic",
                        "stream-name-hash",
                        StreamState.ACTIVE.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        5),
                new CommittedEndOffsetRecord(STREAM.value(), 2, 100, 1, 5),
                new TrimRecord(STREAM.value(), 0, "", 1, 5));
        OxiaMetadataStore l0Store = l0Store(snapshot);
        return new Context(
                generations,
                physical,
                protections,
                scheduler,
                l0Store,
                task,
                output);
    }

    static GenerationProtocolActivationGuard successfulGuard() {
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<GenerationActivationProof> requireReady(
                    GenerationOperation operation,
                    com.nereusstream.core.capability.GenerationActivationSubject subject,
                    boolean activateLiveProjectionIfAbsent) {
                return CompletableFuture.completedFuture(GenerationActivationProof.create(
                        operation,
                        subject,
                        1,
                        1,
                        1,
                        REFERENCE_DOMAINS_SHA,
                        true,
                        false,
                        CLOCK.millis()));
            }

            @Override
            public CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    static GenerationProtocolActivationGuard failFirstRevalidation() {
        AtomicBoolean fail = new AtomicBoolean(true);
        GenerationProtocolActivationGuard delegate = successfulGuard();
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<GenerationActivationProof> requireReady(
                    GenerationOperation operation,
                    com.nereusstream.core.capability.GenerationActivationSubject subject,
                    boolean activateLiveProjectionIfAbsent) {
                return delegate.requireReady(operation, subject, activateLiveProjectionIfAbsent);
            }

            @Override
            public CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
                if (fail.compareAndSet(true, false)) {
                    return CompletableFuture.failedFuture(
                            new F4MetadataConditionFailedException("activation changed before commit"));
                }
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    static GenerationMetadataStore loseFirstCommittedIndexResponse(
            GenerationMetadataStore delegate) {
        AtomicBoolean lose = new AtomicBoolean(true);
        return proxy(delegate, (method, args, result) -> {
            if (method.equals("compareAndSetIndex")
                    && args[1] instanceof GenerationIndexRecord replacement
                    && replacement.lifecycle() == GenerationLifecycle.COMMITTED
                    && lose.compareAndSet(true, false)) {
                @SuppressWarnings("unchecked")
                CompletableFuture<VersionedGenerationIndex> exact =
                        (CompletableFuture<VersionedGenerationIndex>) result;
                return exact.thenCompose(ignored -> CompletableFuture.failedFuture(
                        new F4MetadataConditionFailedException("lost committed-index response")));
            }
            return result;
        });
    }

    static GenerationMetadataStore hideSources(GenerationMetadataStore delegate) {
        return proxy(delegate, (method, args, result) -> method.equals("getCandidate")
                ? CompletableFuture.completedFuture(Optional.empty())
                : result);
    }

    static GenerationMetadataStore loseFirstGenerationAttachmentResponse(
            GenerationMetadataStore delegate) {
        AtomicBoolean lose = new AtomicBoolean(true);
        return proxy(delegate, (method, args, result) -> {
            if (method.equals("compareAndSetTask")
                    && args[1] instanceof MaterializationTaskRecord replacement
                    && replacement.lifecycle() == TaskLifecycle.PUBLISHING
                    && replacement.allocatedGeneration().isPresent()
                    && lose.compareAndSet(true, false)) {
                @SuppressWarnings("unchecked")
                CompletableFuture<VersionedMaterializationTask> exact =
                        (CompletableFuture<VersionedMaterializationTask>) result;
                return exact.thenCompose(ignored -> CompletableFuture.failedFuture(
                        new F4MetadataConditionFailedException("lost task attachment response")));
            }
            return result;
        });
    }

    private static GenerationMetadataStore proxy(
            GenerationMetadataStore delegate,
            ResultInterceptor interceptor) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(delegate, args);
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        return interceptor.intercept(method.getName(), args, result);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static void createOutputReadyTask(
            GenerationMetadataStore store,
            MaterializationTask task,
            MaterializationOutput output) {
        MaterializationTaskRecord planned = new MaterializationTaskRecord(
                1,
                task.taskId(),
                task.taskSequence(),
                task.streamId().value(),
                task.view().wireId(),
                task.taskKind().wireId(),
                task.coverage().startOffset(),
                task.coverage().endOffset(),
                task.sources().stream().map(MaterializationRecordMapper::sourceRecord).toList(),
                task.sourceSetSha256().value(),
                task.policy().policyId(),
                task.policy().policyVersion(),
                task.policyDigestSha256().value(),
                MaterializationRecordMapper.policyRecord(task.policy()),
                TaskLifecycle.PLANNED,
                0,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                "",
                TaskFailureClass.NONE.wireId(),
                "",
                0,
                200,
                200,
                0);
        VersionedMaterializationTask created = store.createTask(CLUSTER, planned).join();
        MaterializationTaskRecord claimed = taskState(
                planned,
                TaskLifecycle.CLAIMED,
                1,
                Optional.of(new WorkerClaimRecord(CLAIM_ID, PROCESS_ID, 1, 300, 2_000)),
                Optional.empty(),
                300);
        VersionedMaterializationTask claimedValue = store.compareAndSetTask(
                CLUSTER, claimed, created.metadataVersion()).join();
        MaterializationTaskRecord ready = taskState(
                claimed,
                TaskLifecycle.OUTPUT_READY,
                1,
                Optional.empty(),
                Optional.of(MaterializationRecordMapper.outputRecord(output)),
                400);
        store.compareAndSetTask(CLUSTER, ready, claimedValue.metadataVersion()).join();
    }

    private static MaterializationTaskRecord taskState(
            MaterializationTaskRecord source,
            TaskLifecycle lifecycle,
            long attempt,
            Optional<WorkerClaimRecord> claim,
            Optional<com.nereusstream.metadata.oxia.records.MaterializationOutputRecord> output,
            long updatedAt) {
        return new MaterializationTaskRecord(
                source.schemaVersion(),
                source.taskId(),
                source.taskSequence(),
                source.streamId(),
                source.readViewId(),
                source.taskKindId(),
                source.offsetStart(),
                source.offsetEnd(),
                source.sources(),
                source.sourceSetSha256(),
                source.policyId(),
                source.policyVersion(),
                source.policySha256(),
                source.policy(),
                lifecycle,
                attempt,
                claim,
                output,
                OptionalLong.empty(),
                "",
                TaskFailureClass.NONE.wireId(),
                "",
                0,
                source.createdAtMillis(),
                updatedAt,
                0);
    }

    private static GenerationIndexRecord generation(
            ObjectSliceReadTarget target,
            long generation,
            String publicationId,
            String taskId,
            Checksum policy,
            GenerationLifecycle lifecycle,
            long createdAt,
            long committedAt,
            long stateChangedAt) {
        var encoded = ReadTargetCodecRegistry.phase15().encode(target);
        return new GenerationIndexRecord(
                1,
                STREAM.value(),
                ReadView.COMMITTED.wireId(),
                0,
                2,
                generation,
                publicationId,
                taskId,
                lifecycle,
                sha("a").value(),
                policy.value(),
                encoded,
                encoded.identityChecksumValue(),
                policy.value(),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                2,
                2,
                2,
                100,
                0,
                100,
                1,
                1,
                schemas(),
                MaterializationRecordMapper.projectionIdentity(Optional.of(PROJECTION)),
                createdAt,
                committedAt,
                "",
                stateChangedAt,
                0);
    }

    private static ObjectSliceReadTarget target(
            String objectId,
            String objectKey,
            String sliceId,
            String checksum) {
        return target(
                objectId,
                objectKey,
                sliceId,
                checksum,
                MaterializationPolicy.COMMITTED_FORMAT);
    }

    private static ObjectSliceReadTarget target(
            String objectId,
            String objectKey,
            String sliceId,
            String checksum,
            String physicalFormat) {
        ObjectId id = new ObjectId(objectId);
        ObjectKey key = new ObjectKey(objectKey);
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.of(id),
                Optional.of(key),
                Optional.empty(),
                96,
                32,
                new Checksum(ChecksumType.CRC32C, "33333333"));
        return new ObjectSliceReadTarget(
                1,
                id,
                key,
                ObjectType.STREAM_COMPACTED_OBJECT,
                physicalFormat,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                sliceId,
                0,
                128,
                new Checksum(ChecksumType.CRC32C, checksum),
                index);
    }

    private static PhysicalObjectRootRecord activeRoot(
            PhysicalObjectIdentity identity,
            long createdAt) {
        return new PhysicalObjectRootRecord(
                1,
                identity.objectKeyHash().value(),
                identity.objectKey().value(),
                identity.objectId().orElseThrow().value(),
                identity.kind().wireId(),
                identity.objectLength(),
                identity.storageChecksum().type().name(),
                identity.storageChecksum().value(),
                identity.contentSha256().orElseThrow().value(),
                identity.etag().orElseThrow(),
                PhysicalObjectLifecycle.ACTIVE,
                1,
                createdAt,
                createdAt + 10_000,
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                0);
    }

    private static OxiaMetadataStore l0Store(StreamMetadataSnapshot snapshot) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getStreamSnapshot" -> CompletableFuture.completedFuture(snapshot);
                    case "close" -> null;
                    case "toString" -> "PublicationTestL0Store";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static List<SchemaRef> schemas() {
        return List.of(new SchemaRef("pulsar", "schema-publication", 1));
    }

    private static Checksum sha(String digit) {
        return new Checksum(ChecksumType.SHA256, digit.repeat(64));
    }

    @FunctionalInterface
    private interface ResultInterceptor {
        Object intercept(String method, Object[] args, Object result);
    }

    static final class Context implements AutoCloseable {
        private final GenerationMetadataStore generations;
        private final FakePhysicalObjectMetadataStore physical;
        private final ObjectProtectionManager protections;
        private final ScheduledExecutorService scheduler;
        private final OxiaMetadataStore l0Store;
        private final MaterializationTask task;
        private final MaterializationOutput output;
        private final AtomicInteger publicationSequence = new AtomicInteger();

        private Context(
                GenerationMetadataStore generations,
                FakePhysicalObjectMetadataStore physical,
                ObjectProtectionManager protections,
                ScheduledExecutorService scheduler,
                OxiaMetadataStore l0Store,
                MaterializationTask task,
                MaterializationOutput output) {
            this.generations = generations;
            this.physical = physical;
            this.protections = protections;
            this.scheduler = scheduler;
            this.l0Store = l0Store;
            this.task = task;
            this.output = output;
        }

        GenerationMetadataStore generations() {
            return generations;
        }

        FakePhysicalObjectMetadataStore physical() {
            return physical;
        }

        ObjectProtectionManager protections() {
            return protections;
        }

        ScheduledExecutorService scheduler() {
            return scheduler;
        }

        MaterializationTask task() {
            return task;
        }

        MaterializationOutput output() {
            return output;
        }

        DefaultGenerationCommitter committer(
                GenerationMetadataStore store,
                GenerationProtocolActivationGuard guard) {
            return new DefaultGenerationCommitter(
                    CLUSTER,
                    l0Store,
                    store,
                    physical,
                    protections,
                    guard,
                    (ignoredTask, ignoredOutput, timeout) -> CompletableFuture.completedFuture(null),
                    () -> {
                        int value = publicationSequence.getAndIncrement();
                        if (value > 9) {
                            throw new IllegalStateException("test publication id sequence exhausted");
                        }
                        return new PublicationId(String.valueOf((char) ('p' + value)).repeat(26));
                    },
                    Duration.ofSeconds(30),
                    scheduler,
                    CLOCK);
        }

        @Override
        public void close() {
            protections.close();
            physical.close();
            generations.close();
            l0Store.close();
            scheduler.shutdownNow();
        }
    }
}
