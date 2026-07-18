/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.GenerationPublicationTestSupport.CLUSTER;
import static com.nereusstream.materialization.GenerationPublicationTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.capability.GenerationProjectionAuthoritySnapshot;
import com.nereusstream.core.capability.StreamRetirementReferenceAuthoritySnapshot;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.materialization.gc.StreamRegistrationRetirementCoordinator;
import com.nereusstream.materialization.gc.StreamRegistrationRetirementResult;
import com.nereusstream.materialization.gc.StreamRegistrationRetirementStatus;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class StreamRegistrationRetirementWorkflowTest {
    private static final Clock RETIREMENT_CLOCK = Clock.fixed(
            Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @Test
    void nonTerminalTaskAndLiveIndexBlockWithoutMutation() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            StreamRegistrationRetirementResult nonTerminal = coordinator(
                            context,
                            context.generations(),
                            context.physical(),
                            RETIREMENT_CLOCK)
                    .retire(STREAM)
                    .join();

            assertThat(nonTerminal.status())
                    .isEqualTo(StreamRegistrationRetirementStatus.TASK_NOT_TERMINAL);
            assertThat(context.generations()
                            .getTask(CLUSTER, STREAM, context.task().taskId())
                            .join())
                    .isPresent();
            assertThat(context.generations()
                            .getStreamRegistration(CLUSTER, STREAM)
                            .join())
                    .isPresent();

            context.committer(
                            context.generations(),
                            GenerationPublicationTestSupport.successfulGuard())
                    .publish(context.task(), context.output())
                    .join();

            StreamRegistrationRetirementResult liveIndex = coordinator(
                            context,
                            context.generations(),
                            context.physical(),
                            RETIREMENT_CLOCK)
                    .retire(STREAM)
                    .join();

            assertThat(liveIndex.status())
                    .isEqualTo(StreamRegistrationRetirementStatus.INDEX_STILL_LIVE);
            assertThat(context.generations()
                            .getStreamRegistration(CLUSTER, STREAM)
                            .join())
                    .isPresent();
            assertThat(scanIndexes(context.generations())).hasSize(2);
        }
    }

    @Test
    void auditGraceBlocksTerminalTaskBeforeAnyOwnerRetirement() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            context.committer(
                            context.generations(),
                            GenerationPublicationTestSupport.successfulGuard())
                    .publish(context.task(), context.output())
                    .join();
            List<VersionedObjectProtection> protections = allProtections(context);

            StreamRegistrationRetirementResult result = coordinator(
                            context,
                            context.generations(),
                            context.physical(),
                            GenerationPublicationTestSupport.CLOCK)
                    .retire(STREAM)
                    .join();

            assertThat(result.status())
                    .isEqualTo(StreamRegistrationRetirementStatus.AUDIT_GRACE_PENDING);
            assertThat(allProtections(context)).isEqualTo(protections);
            assertThat(scanIndexes(context.generations())).hasSize(2);
            assertThat(context.generations()
                            .getTask(CLUSTER, STREAM, context.task().taskId())
                            .join())
                    .isPresent();
            assertThat(context.generations()
                            .getStreamRegistration(CLUSTER, STREAM)
                            .join())
                    .isPresent();
        }
    }

    @Test
    void retiresPublishedWorkflowOwnersAcrossDeleteResponseLossButKeepsPhysicalRoots() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            context.committer(
                            context.generations(),
                            GenerationPublicationTestSupport.successfulGuard())
                    .publish(context.task(), context.output())
                    .join();
            retireAllIndexes(context.generations());

            ObjectKeyHash sourceObject = sourceObject(context);
            ObjectKeyHash outputObject = context.output().objectKeyHash();
            assertThat(allProtections(context)).hasSize(3);
            assertThat(context.physical().getRoot(CLUSTER, sourceObject).join()).isPresent();
            assertThat(context.physical().getRoot(CLUSTER, outputObject).join()).isPresent();

            AtomicBoolean loseIndexDelete = new AtomicBoolean(true);
            AtomicBoolean loseTaskDelete = new AtomicBoolean(true);
            AtomicBoolean loseRegistrationDelete = new AtomicBoolean(true);
            AtomicBoolean loseProtectionDelete = new AtomicBoolean(true);
            GenerationMetadataStore generations = loseFirstGenerationDeleteResponses(
                    context.generations(),
                    loseIndexDelete,
                    loseTaskDelete,
                    loseRegistrationDelete);
            PhysicalObjectMetadataStore physical = loseFirstProtectionDeleteResponse(
                    context.physical(), loseProtectionDelete);

            StreamRegistrationRetirementResult result = coordinator(
                            context,
                            generations,
                            physical,
                            RETIREMENT_CLOCK)
                    .retire(STREAM)
                    .join();

            assertThat(result.status()).isEqualTo(StreamRegistrationRetirementStatus.RETIRED);
            assertThat(result.protectionsRetired()).isEqualTo(3);
            assertThat(result.indexesRetired()).isEqualTo(2);
            assertThat(result.tasksRetired()).isOne();
            assertThat(result.sequencesRetired()).isOne();
            assertThat(result.registrationRetired()).isTrue();
            assertThat(loseIndexDelete).isFalse();
            assertThat(loseTaskDelete).isFalse();
            assertThat(loseRegistrationDelete).isFalse();
            assertThat(loseProtectionDelete).isFalse();
            assertThat(allProtections(context)).isEmpty();
            assertThat(scanIndexes(context.generations())).isEmpty();
            assertThat(context.generations()
                            .getTask(CLUSTER, STREAM, context.task().taskId())
                            .join())
                    .isEmpty();
            assertThat(context.generations()
                            .getStreamRegistration(CLUSTER, STREAM)
                            .join())
                    .isEmpty();
            assertThat(context.physical().getRoot(CLUSTER, sourceObject).join()).isPresent();
            assertThat(context.physical().getRoot(CLUSTER, outputObject).join()).isPresent();
        }
    }

    private static StreamRegistrationRetirementCoordinator coordinator(
            GenerationPublicationTestSupport.Context context,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physical,
            Clock clock) {
        return new StreamRegistrationRetirementCoordinator(
                CLUSTER,
                deletedL0(),
                generations,
                physical,
                subject -> CompletableFuture.completedFuture(
                        new GenerationProjectionAuthoritySnapshot(
                                subject,
                                false,
                                Optional.empty(),
                                List.of(new GcAuthorityToken(
                                        "/projection/registration-retirement",
                                        1,
                                        sha('a'))))),
                subject -> CompletableFuture.completedFuture(
                        StreamRetirementReferenceAuthoritySnapshot.complete(
                                subject, 0, List.of())),
                unavailableCheckpointCodec(),
                config(),
                Duration.ofMillis(500),
                clock,
                context.scheduler());
    }

    private static OxiaMetadataStore deletedL0() {
        StreamMetadataSnapshot snapshot = new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/deleted-registration-retirement",
                        "deleted-registration-retirement-hash",
                        StreamState.DELETED.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        5),
                new CommittedEndOffsetRecord(STREAM.value(), 2, 100, 1, 5),
                new TrimRecord(STREAM.value(), 0, "", 1, 5));
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getStreamSnapshot" -> CompletableFuture.completedFuture(snapshot);
                    case "close" -> null;
                    case "toString" -> "DeletedRegistrationRetirementL0";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new AssertionError(
                            "root-absent retirement must not call " + method.getName());
                });
    }

    private static List<VersionedGenerationIndex> scanIndexes(GenerationMetadataStore store) {
        ArrayList<VersionedGenerationIndex> indexes = new ArrayList<>();
        Optional<F4ScanToken> continuation = Optional.empty();
        do {
            var page = store.scanIndex(
                            CLUSTER,
                            STREAM,
                            ReadView.COMMITTED,
                            0,
                            Long.MAX_VALUE,
                            continuation,
                            1)
                    .join();
            for (VersionedGenerationCandidate candidate : page.values()) {
                assertThat(candidate).isInstanceOf(VersionedGenerationIndex.class);
                indexes.add((VersionedGenerationIndex) candidate);
            }
            continuation = page.continuation();
        } while (continuation.isPresent());
        return List.copyOf(indexes);
    }

    private static void retireAllIndexes(GenerationMetadataStore store) {
        for (VersionedGenerationIndex current : scanIndexes(store)) {
            VersionedGenerationIndex draining = store.compareAndSetIndex(
                            CLUSTER,
                            indexState(
                                    current.value(),
                                    GenerationLifecycle.DRAINING,
                                    "registration-retirement-test-drain",
                                    1_500),
                            current.metadataVersion())
                    .join();
            store.compareAndSetIndex(
                            CLUSTER,
                            indexState(
                                    draining.value(),
                                    GenerationLifecycle.RETIRED,
                                    "registration-retirement-test-retired",
                                    1_600),
                            draining.metadataVersion())
                    .join();
        }
    }

    private static GenerationIndexRecord indexState(
            GenerationIndexRecord current,
            GenerationLifecycle lifecycle,
            String reason,
            long changedAtMillis) {
        return new GenerationIndexRecord(
                current.schemaVersion(),
                current.streamId(),
                current.readViewId(),
                current.offsetStart(),
                current.offsetEnd(),
                current.generation(),
                current.publicationId(),
                current.taskId(),
                lifecycle,
                current.sourceSetSha256(),
                current.policySha256(),
                current.readTarget(),
                current.targetIdentitySha256(),
                current.materializationPolicySha256(),
                current.payloadFormat(),
                current.sourceRecordCount(),
                current.outputRecordCount(),
                current.entryCount(),
                current.logicalBytes(),
                current.cumulativeSizeAtStart(),
                current.cumulativeSizeAtEnd(),
                current.firstCommitVersion(),
                current.lastCommitVersion(),
                current.schemaRefs(),
                current.projectionRef(),
                current.createdAtMillis(),
                current.committedAtMillis(),
                reason,
                changedAtMillis,
                0);
    }

    private static ObjectKeyHash sourceObject(GenerationPublicationTestSupport.Context context) {
        ObjectSliceReadTarget target = (ObjectSliceReadTarget) context.task()
                .sources()
                .get(0)
                .readTarget();
        return ObjectKeyHash.from(target.objectKey());
    }

    private static List<VersionedObjectProtection> allProtections(
            GenerationPublicationTestSupport.Context context) {
        ArrayList<VersionedObjectProtection> protections = new ArrayList<>();
        protections.addAll(context.physical()
                .scanProtections(
                        CLUSTER,
                        sourceObject(context),
                        Optional.empty(),
                        100)
                .join()
                .values());
        protections.addAll(context.physical()
                .scanProtections(
                        CLUSTER,
                        context.output().objectKeyHash(),
                        Optional.empty(),
                        100)
                .join()
                .values());
        return List.copyOf(protections);
    }

    private static GenerationMetadataStore loseFirstGenerationDeleteResponses(
            GenerationMetadataStore delegate,
            AtomicBoolean loseIndexDelete,
            AtomicBoolean loseTaskDelete,
            AtomicBoolean loseRegistrationDelete) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        AtomicBoolean loss = switch (method.getName()) {
                            case "deleteIndex" -> loseIndexDelete;
                            case "deleteTask" -> loseTaskDelete;
                            case "deleteStreamRegistration" -> loseRegistrationDelete;
                            default -> null;
                        };
                        if (loss != null && loss.compareAndSet(true, false)) {
                            @SuppressWarnings("unchecked")
                            CompletableFuture<Void> deleted = (CompletableFuture<Void>) result;
                            return deleted.thenCompose(ignored -> CompletableFuture.failedFuture(
                                    new IllegalStateException(
                                            "lost " + method.getName() + " response")));
                        }
                        return result;
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static PhysicalObjectMetadataStore loseFirstProtectionDeleteResponse(
            PhysicalObjectMetadataStore delegate,
            AtomicBoolean loseResponse) {
        return (PhysicalObjectMetadataStore) Proxy.newProxyInstance(
                PhysicalObjectMetadataStore.class.getClassLoader(),
                new Class<?>[] {PhysicalObjectMetadataStore.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("deleteProtection")
                                && loseResponse.compareAndSet(true, false)) {
                            @SuppressWarnings("unchecked")
                            CompletableFuture<Void> deleted = (CompletableFuture<Void>) result;
                            return deleted.thenCompose(ignored -> CompletableFuture.failedFuture(
                                    new IllegalStateException(
                                            "lost deleteProtection response")));
                        }
                        return result;
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static RecoveryCheckpointCodecV1 unavailableCheckpointCodec() {
        return (RecoveryCheckpointCodecV1) Proxy.newProxyInstance(
                RecoveryCheckpointCodecV1.class.getClassLoader(),
                new Class<?>[] {RecoveryCheckpointCodecV1.class},
                (proxy, method, arguments) -> {
                    throw new AssertionError(
                            "root-absent retirement must not call " + method.getName());
                });
    }

    private static PhysicalGcConfig config() {
        return new PhysicalGcConfig(
                true,
                false,
                1,
                1,
                1,
                10,
                100,
                100,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ofMillis(5),
                Duration.ofSeconds(11),
                Duration.ofSeconds(30),
                Duration.ofHours(1),
                Duration.ofHours(2),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2));
    }

    private static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256, String.valueOf(value).repeat(64));
    }
}
