/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationIndexDigests;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.RecoveryCheckpointRootDigests;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointDirectory;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublicationPage;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteRequest;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class HigherGenerationPreDrainCoordinatorTest {
    private static final String CLUSTER = "cluster-pre-drain";
    private static final StreamId STREAM = new StreamId("tenant/ns/pre-drain");

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void committedSourceDrainsOnlyAfterExactRecoveryReplacementCoverage() {
        Fixture fixture = fixture(GenerationLifecycle.COMMITTED);
        StoreState state = new StoreState(fixture);
        HigherGenerationPreDrainCoordinator coordinator = coordinator(
                fixture, state, false, false);

        HigherGenerationPreDrainResult result = coordinator.preDrain(
                fixture.candidate()).join();

        assertThat(result.status())
                .isEqualTo(HigherGenerationPreDrainStatus.DRAINING_READY);
        assertThat(result.transitionedCount()).isEqualTo(1);
        assertThat(result.alreadyDrainingCount()).isZero();
        assertThat(result.drainingIndexes()).singleElement().satisfies(index -> {
            assertThat(index.value().lifecycle())
                    .isEqualTo(GenerationLifecycle.DRAINING);
            assertThat(index.value().stateReason())
                    .isEqualTo("physical-gc-pre-drain:"
                            + fixture.candidate().candidateId());
        });
        assertThat(state.casCalls).hasValue(1);
        assertThat(state.openCalls).hasValueGreaterThanOrEqualTo(1);
        assertThat(state.publicationReads).hasValueGreaterThanOrEqualTo(1);
    }

    @Test
    void quarantinedSourceUsesTheSameCoverageProofBeforeDraining() {
        Fixture fixture = fixture(GenerationLifecycle.QUARANTINED);
        StoreState state = new StoreState(fixture);

        HigherGenerationPreDrainResult result = coordinator(
                        fixture, state, false, false)
                .preDrain(fixture.candidate())
                .join();

        assertThat(result.transitionedCount()).isEqualTo(1);
        assertThat(state.source.get().value().lifecycle())
                .isEqualTo(GenerationLifecycle.DRAINING);
    }

    @Test
    void markedReplacementRootVetoesBeforeTheSourceCas() {
        Fixture base = fixture(GenerationLifecycle.COMMITTED);
        Fixture fixture = base.withReplacementRoot(
                markedRoot(base.replacementRoot()));
        StoreState state = new StoreState(fixture);

        assertThatThrownBy(() -> coordinator(fixture, state, false, false)
                        .preDrain(fixture.candidate())
                        .join())
                .hasRootCauseMessage(
                        "higher-generation source has no current healthy NRC1 replacement");
        assertThat(state.casCalls).hasValue(0);
    }

    @Test
    void lostCasResponseConvergesOnlyOnTheExactDrainingReplacement() {
        Fixture fixture = fixture(GenerationLifecycle.COMMITTED);
        StoreState state = new StoreState(fixture);

        HigherGenerationPreDrainResult result = coordinator(
                        fixture, state, true, false)
                .preDrain(fixture.candidate())
                .join();

        assertThat(result.transitionedCount()).isEqualTo(1);
        assertThat(result.drainingIndexes()).singleElement().isEqualTo(
                state.source.get());
        assertThat(state.casCalls).hasValue(1);
    }

    @Test
    void candidateRootChangeAtTheFinalFencePreventsTheSourceCas() {
        Fixture fixture = fixture(GenerationLifecycle.COMMITTED);
        StoreState state = new StoreState(fixture);

        assertThatThrownBy(() -> coordinator(fixture, state, false, true)
                        .preDrain(fixture.candidate())
                        .join())
                .hasRootCauseMessage(
                        "candidate physical root changed before higher-generation pre-drain");
        assertThat(state.casCalls).hasValue(0);
        assertThat(state.candidateRootReads).hasValue(2);
    }

    @Test
    void alreadyDrainingSourceStillRequiresCurrentHealthyCoverage() {
        Fixture base = fixture(GenerationLifecycle.DRAINING);
        Fixture fixture = base.withReplacementRoot(
                markedRoot(base.replacementRoot()));
        StoreState state = new StoreState(fixture);

        assertThatThrownBy(() -> coordinator(fixture, state, false, false)
                        .preDrain(fixture.candidate())
                        .join())
                .hasRootCauseMessage(
                        "higher-generation source has no current healthy NRC1 replacement");
        assertThat(state.casCalls).hasValue(0);
    }

    @Test
    void incompleteNrc1TilingCountsVetoBeforeTheSourceCas() {
        Fixture base = fixture(GenerationLifecycle.COMMITTED);
        Fixture fixture = base.withSource(
                sourceWithEntryCount(base.source(), 2));
        StoreState state = new StoreState(fixture);

        assertThatThrownBy(() -> coordinator(fixture, state, false, false)
                        .preDrain(fixture.candidate())
                        .join())
                .hasRootCauseMessage(
                        "higher-generation NRC1 tiling does not reproduce source counts or schemas");
        assertThat(state.casCalls).hasValue(0);
    }

    @Test
    void dryRunReturnsBeforeAnyMetadataOrRootRead() {
        Fixture fixture = fixture(GenerationLifecycle.COMMITTED);
        StoreState state = new StoreState(fixture);
        HigherGenerationPreDrainCoordinator coordinator = new HigherGenerationPreDrainCoordinator(
                CLUSTER,
                l0Store(state),
                generationStore(state, false),
                physicalStore(state, false),
                checkpointCodec(state),
                disabledConfig(),
                fixedClock(),
                scheduler);

        HigherGenerationPreDrainResult result = coordinator.preDrain(
                fixture.candidate()).join();

        assertThat(result.status())
                .isEqualTo(HigherGenerationPreDrainStatus.MUTATION_DISABLED);
        assertThat(state.scanCalls).hasValue(0);
        assertThat(state.l0Reads).hasValue(0);
        assertThat(state.candidateRootReads).hasValue(0);
    }

    @Test
    void sourceRetirementGraceReturnsBeforeAnyMetadataOrRootRead() {
        Fixture fixture = fixture(GenerationLifecycle.COMMITTED);
        StoreState state = new StoreState(fixture);
        GcCandidate source = fixture.candidate();
        GcCandidate future = new GcCandidate(
                source.candidateId(),
                source.object(),
                source.referenceQuery(),
                source.discoveryEvidenceSha256(),
                source.rootState(),
                source.rootMetadataVersion(),
                source.rootLifecycleEpoch(),
                source.discoveredAtMillis(),
                101);

        HigherGenerationPreDrainResult result = coordinator(
                        fixture, state, false, false)
                .preDrain(future)
                .join();

        assertThat(result.status())
                .isEqualTo(HigherGenerationPreDrainStatus.NOT_ELIGIBLE_YET);
        assertThat(state.scanCalls).hasValue(0);
        assertThat(state.l0Reads).hasValue(0);
        assertThat(state.candidateRootReads).hasValue(0);
    }

    @Test
    void drainingHigherRemovalPlannerReprovesCoverageBeforeFreezing() {
        Fixture fixture = fixture(GenerationLifecycle.DRAINING);
        StoreState state = new StoreState(fixture);
        SourceRetirementPlanBuilder planner = new SourceRetirementPlanBuilder(
                CLUSTER,
                l0Store(state),
                generationStore(state, false),
                physicalStore(state, false),
                sourceStore(),
                checkpointCodec(state),
                enabledConfig());

        List<GcPlannedMetadataRemoval> removals = planner.build(
                fixture.candidate()).join();

        assertThat(removals).containsExactly(new GcPlannedMetadataRemoval(
                HigherGenerationIndexRetirementHandler.REMOVAL_TYPE,
                state.source.get().key(),
                state.source.get().metadataVersion(),
                state.source.get().durableValueSha256()));
        assertThat(state.publicationReads).hasValueGreaterThanOrEqualTo(2);
    }

    @Test
    void topicCompactedSourceUsesAHealthySameViewReplacement() {
        Fixture fixture = fixture(
                ReadView.TOPIC_COMPACTED,
                GenerationLifecycle.COMMITTED);
        StoreState state = new StoreState(fixture);

        HigherGenerationPreDrainResult result = coordinator(
                        fixture, state, false, false)
                .preDrain(fixture.candidate())
                .join();

        assertThat(result.transitionedCount()).isEqualTo(1);
        assertThat(result.drainingIndexes()).singleElement().satisfies(index -> {
            assertThat(index.value().readViewId())
                    .isEqualTo(ReadView.TOPIC_COMPACTED.wireId());
            assertThat(index.value().lifecycle())
                    .isEqualTo(GenerationLifecycle.DRAINING);
        });
        assertThat(state.openCalls).hasValue(0);
        assertThat(state.publicationReads).hasValue(0);
        assertThat(state.replacementRootReads).hasValueGreaterThanOrEqualTo(2);
    }

    @Test
    void topicCompactedMarkedReplacementVetoesBeforeTheSourceCas() {
        Fixture base = fixture(
                ReadView.TOPIC_COMPACTED,
                GenerationLifecycle.COMMITTED);
        Fixture fixture = base.withReplacementRoot(
                markedRoot(base.replacementRoot()));
        StoreState state = new StoreState(fixture);

        assertThatThrownBy(() -> coordinator(fixture, state, false, false)
                        .preDrain(fixture.candidate())
                        .join())
                .hasRootCauseMessage(
                        "TOPIC_COMPACTED source has no current healthy same-view replacement");
        assertThat(state.casCalls).hasValue(0);
    }

    @Test
    void drainingTopicRemovalPlannerReprovesTheSameViewReplacement() {
        Fixture fixture = fixture(
                ReadView.TOPIC_COMPACTED,
                GenerationLifecycle.DRAINING);
        StoreState state = new StoreState(fixture);
        SourceRetirementPlanBuilder planner = new SourceRetirementPlanBuilder(
                CLUSTER,
                l0Store(state),
                generationStore(state, false),
                physicalStore(state, false),
                sourceStore(),
                checkpointCodec(state),
                enabledConfig());

        List<GcPlannedMetadataRemoval> removals = planner.build(
                fixture.candidate()).join();

        assertThat(removals).containsExactly(new GcPlannedMetadataRemoval(
                HigherGenerationIndexRetirementHandler.REMOVAL_TYPE,
                state.source.get().key(),
                state.source.get().metadataVersion(),
                state.source.get().durableValueSha256()));
        assertThat(state.openCalls).hasValue(0);
        assertThat(state.replacementRootReads).hasValueGreaterThanOrEqualTo(2);
    }

    @Test
    void completedTrimDrainsTopicSourceWithoutReplacementOrCheckpointReads() {
        Fixture base = fixture(
                ReadView.TOPIC_COMPACTED,
                GenerationLifecycle.COMMITTED);
        Fixture fixture = base.withReplacementRoot(
                markedRoot(base.replacementRoot()));
        StoreState state = new StoreState(fixture);
        state.streamSnapshot.set(snapshot(12));

        HigherGenerationPreDrainResult result = coordinator(
                        fixture, state, false, false)
                .preDrain(fixture.candidate())
                .join();

        assertThat(result.transitionedCount()).isEqualTo(1);
        assertThat(state.casCalls).hasValue(1);
        assertThat(state.openCalls).hasValue(0);
        assertThat(state.publicationReads).hasValue(0);
        assertThat(state.replacementRootReads).hasValue(0);
    }

    @Test
    void completedTrimDriftVetoesBeforeTheSourceCas() {
        Fixture fixture = fixture(
                ReadView.TOPIC_COMPACTED,
                GenerationLifecycle.COMMITTED);
        StoreState state = new StoreState(fixture);
        state.streamSnapshot.set(snapshot(12));
        state.loseCompletedTrimOnRevalidation.set(true);

        assertThatThrownBy(() -> coordinator(fixture, state, false, false)
                        .preDrain(fixture.candidate())
                        .join())
                .hasRootCauseMessage(
                        "completed trim changed while retirement facts were frozen");
        assertThat(state.casCalls).hasValue(0);
        assertThat(state.replacementRootReads).hasValue(0);
    }

    private HigherGenerationPreDrainCoordinator coordinator(
            Fixture fixture,
            StoreState state,
            boolean loseCasResponse,
            boolean changeCandidateRootAtFinalFence) {
        return new HigherGenerationPreDrainCoordinator(
                CLUSTER,
                l0Store(state),
                generationStore(state, loseCasResponse),
                physicalStore(state, changeCandidateRootAtFinalFence),
                checkpointCodec(state),
                enabledConfig(),
                fixedClock(),
                scheduler);
    }

    private static Fixture fixture(GenerationLifecycle sourceLifecycle) {
        return fixture(ReadView.COMMITTED, sourceLifecycle);
    }

    private static Fixture fixture(
            ReadView view,
            GenerationLifecycle sourceLifecycle) {
        ObjectSliceReadTarget sourceTarget = target(
                view,
                "source-object",
                "objects/source-object",
                "11223344");
        ObjectSliceReadTarget replacementTarget = target(
                view,
                "replacement-object",
                "objects/replacement-object",
                "55667788");
        VersionedGenerationIndex source = generation(
                view, 1, "p".repeat(52), sourceTarget, sourceLifecycle, 5);
        VersionedGenerationIndex replacement = generation(
                view,
                2,
                "q".repeat(52),
                replacementTarget,
                GenerationLifecycle.COMMITTED,
                6);

        StreamCommitTargetRecord commit = commit(sourceTarget);
        byte[] commitBytes = MetadataRecordCodecFactory.encodeEnvelope(
                commit, StreamCommitTargetRecord.class);
        RecoveryCheckpointEntry entry = new RecoveryCheckpointEntry(
                1,
                new OffsetRange(0, 12),
                12,
                commit.commitId(),
                "",
                ByteBuffer.wrap(commitBytes),
                sha256(commitBytes),
                List.of(0));
        GenerationIndexRecord embedded = replacement.value().withMetadataVersion(0);
        byte[] publicationBytes = new GenerationIndexRecordCodecV1().encode(
                embedded);
        RecoveryCheckpointPublication publication = new RecoveryCheckpointPublication(
                embedded.generation(),
                new PublicationId(embedded.publicationId()),
                new OffsetRange(embedded.offsetStart(), embedded.offsetEnd()),
                ByteBuffer.wrap(publicationBytes),
                GenerationIndexDigests.canonicalRecordSha256(embedded));

        RecoveryCheckpointWriteRequest header = new RecoveryCheckpointWriteRequest(
                CLUSTER,
                STREAM,
                1,
                "r".repeat(52),
                new OffsetRange(0, 12),
                1,
                1,
                0,
                12,
                commit.commitId(),
                commit.commitId(),
                commit.commitId(),
                1,
                sha('a'),
                1,
                1);
        Checksum checkpointContent = sha('b');
        ObjectKey checkpointKey = RecoveryCheckpointFormatV1.objectKey(
                header, checkpointContent);
        RecoveryCheckpointDirectory directory = new RecoveryCheckpointDirectory(
                100,
                Integer.BYTES,
                100 + Integer.BYTES,
                Integer.BYTES * 2L,
                RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE);
        long checkpointLength = directory.commitDirectoryOffset()
                + directory.commitDirectoryLength()
                + RecoveryCheckpointFormatV1.FOOTER_BYTES;
        RecoveryCheckpointObject checkpoint = new RecoveryCheckpointObject(
                header,
                RecoveryCheckpointFormatV1.objectId(checkpointKey),
                checkpointKey,
                checkpointLength,
                sha('c'),
                checkpointContent,
                directory);
        RecoveryCheckpointReferenceRecord reference = new RecoveryCheckpointReferenceRecord(
                1,
                header.checkpointAttemptId(),
                0,
                12,
                1,
                1,
                0,
                12,
                commit.commitId(),
                commit.commitId(),
                commit.commitId(),
                1,
                header.projectionIdentitySha256().value(),
                checkpoint.objectId().value(),
                checkpoint.objectKey().value(),
                ObjectKeyHash.from(checkpoint.objectKey()).value(),
                checkpointLength,
                "01020304",
                checkpointContent.value(),
                1,
                1);
        RecoveryCheckpointRootRecord rootValue = new RecoveryCheckpointRootRecord(
                1,
                STREAM.value(),
                1,
                0,
                12,
                1,
                1,
                0,
                12,
                commit.commitId(),
                commit.commitId(),
                List.of(reference),
                RecoveryCheckpointRootDigests.checkpointSetSha256(
                        List.of(reference)).value(),
                commit.commitId(),
                1,
                20,
                5);
        VersionedRecoveryCheckpointRoot recoveryRoot = new VersionedRecoveryCheckpointRoot(
                new F4Keyspace(CLUSTER).recoveryRootKey(STREAM),
                rootValue,
                rootValue.metadataVersion(),
                sha('d'));

        VersionedPhysicalObjectRoot candidateRoot = activeRoot(
                sourceTarget,
                view == ReadView.COMMITTED
                        ? PhysicalObjectKind.COMMITTED_COMPACTED
                        : PhysicalObjectKind.TOPIC_COMPACTED,
                3,
                sha('e'));
        VersionedPhysicalObjectRoot replacementRoot = activeRoot(
                replacementTarget,
                view == ReadView.COMMITTED
                        ? PhysicalObjectKind.COMMITTED_COMPACTED
                        : PhysicalObjectKind.TOPIC_COMPACTED,
                4,
                sha('f'));
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(
                candidateRoot.value());
        Checksum evidence = sha('1');
        GcReferenceQuery query = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object,
                List.of(STREAM),
                evidence);
        GcCandidate candidate = new GcCandidate(
                "s".repeat(52),
                object,
                query,
                evidence,
                GcCandidateRootState.ACTIVE_DISCOVERY,
                candidateRoot.metadataVersion(),
                candidateRoot.value().lifecycleEpoch(),
                30,
                30);
        return new Fixture(
                source,
                replacement,
                recoveryRoot,
                checkpoint,
                entry,
                publication,
                candidateRoot,
                replacementRoot,
                candidate);
    }

    private static VersionedGenerationIndex generation(
            ReadView view,
            long generation,
            String publicationId,
            ObjectSliceReadTarget target,
            GenerationLifecycle lifecycle,
            long metadataVersion) {
        var encoded = ReadTargetCodecRegistry.phase15().encode(target);
        boolean committed = lifecycle != GenerationLifecycle.PREPARED
                && lifecycle != GenerationLifecycle.ABORTED;
        String reason = switch (lifecycle) {
            case QUARANTINED -> "quarantined-before-pre-drain";
            case DRAINING -> "earlier-safe-pre-drain";
            case RETIRED -> "retired";
            case ABORTED -> "aborted";
            default -> "";
        };
        GenerationIndexRecord value = new GenerationIndexRecord(
                1,
                STREAM.value(),
                view.wireId(),
                0,
                12,
                generation,
                publicationId,
                "task-" + generation,
                lifecycle,
                sha('2').value(),
                sha('3').value(),
                encoded,
                encoded.identityChecksumValue(),
                sha('3').value(),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                12,
                12,
                1,
                12,
                0,
                12,
                1,
                1,
                List.of(),
                CommitSliceRequest.emptyProjectionIdentity(),
                1,
                committed ? 2 : 0,
                reason,
                lifecycle == GenerationLifecycle.COMMITTED ? 2 : 3,
                metadataVersion);
        String key = new F4Keyspace(CLUSTER).generationIndexKey(
                STREAM, view, 12, generation);
        return new VersionedGenerationIndex(
                key,
                value,
                metadataVersion,
                GenerationIndexDigests.durableValueSha256(
                        value.withMetadataVersion(0)));
    }

    private static VersionedGenerationIndex sourceWithEntryCount(
            VersionedGenerationIndex source,
            int entryCount) {
        GenerationIndexRecord value = source.value();
        GenerationIndexRecord changed = new GenerationIndexRecord(
                value.schemaVersion(),
                value.streamId(),
                value.readViewId(),
                value.offsetStart(),
                value.offsetEnd(),
                value.generation(),
                value.publicationId(),
                value.taskId(),
                value.lifecycle(),
                value.sourceSetSha256(),
                value.policySha256(),
                value.readTarget(),
                value.targetIdentitySha256(),
                value.materializationPolicySha256(),
                value.payloadFormat(),
                value.sourceRecordCount(),
                value.outputRecordCount(),
                entryCount,
                value.logicalBytes(),
                value.cumulativeSizeAtStart(),
                value.cumulativeSizeAtEnd(),
                value.firstCommitVersion(),
                value.lastCommitVersion(),
                value.schemaRefs(),
                value.projectionRef(),
                value.createdAtMillis(),
                value.committedAtMillis(),
                value.stateReason(),
                value.stateChangedAtMillis(),
                value.metadataVersion());
        return new VersionedGenerationIndex(
                source.key(),
                changed,
                source.metadataVersion(),
                GenerationIndexDigests.durableValueSha256(
                        changed.withMetadataVersion(0)));
    }

    private static StreamCommitTargetRecord commit(
            ObjectSliceReadTarget sourceTarget) {
        return new StreamCommitTargetRecord(
                STREAM.value(),
                "commit-pre-drain",
                "",
                0,
                12,
                0,
                12,
                1,
                "writer",
                "writer-run",
                1,
                "fencing",
                ReadTargetCodecRegistry.phase15().encode(sourceTarget),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                12,
                1,
                12,
                List.of(),
                CommitSliceRequest.emptyProjectionIdentity(),
                0,
                0,
                1,
                0);
    }

    private static ObjectSliceReadTarget target(
            ReadView view,
            String objectId,
            String objectKey,
            String checksum) {
        EntryIndexRef entryIndex = new EntryIndexRef(
                EntryIndexLocation.INLINE,
                Optional.empty(),
                Optional.empty(),
                Optional.of(new byte[] {1, 2, 3}),
                0,
                0,
                new Checksum(ChecksumType.CRC32C, "01020304"));
        return new ObjectSliceReadTarget(
                1,
                new ObjectId(objectId),
                new ObjectKey(objectKey),
                ObjectType.STREAM_COMPACTED_OBJECT,
                view == ReadView.COMMITTED
                        ? "NEREUS_COMPACTED_PARQUET_V1"
                        : "NEREUS_TOPIC_COMPACTED_PARQUET_V1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "slice-" + objectId,
                0,
                100,
                new Checksum(ChecksumType.CRC32C, checksum),
                entryIndex);
    }

    private static VersionedPhysicalObjectRoot activeRoot(
            ObjectSliceReadTarget target,
            PhysicalObjectKind kind,
            long metadataVersion,
            Checksum digest) {
        ObjectKeyHash hash = ObjectKeyHash.from(target.objectKey());
        PhysicalObjectRootRecord value = new PhysicalObjectRootRecord(
                1,
                hash.value(),
                target.objectKey().value(),
                target.objectId().value(),
                kind.wireId(),
                100,
                ChecksumType.CRC32C.name(),
                target.sliceChecksum().value(),
                "",
                "",
                PhysicalObjectLifecycle.ACTIVE,
                1,
                1,
                1,
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                metadataVersion);
        return new VersionedPhysicalObjectRoot(
                new F4Keyspace(CLUSTER).physicalRootKey(hash),
                value,
                metadataVersion,
                digest);
    }

    private static VersionedPhysicalObjectRoot markedRoot(
            VersionedPhysicalObjectRoot source) {
        PhysicalObjectRootRecord value = source.value();
        long version = Math.addExact(source.metadataVersion(), 1);
        PhysicalObjectRootRecord changed = new PhysicalObjectRootRecord(
                value.schemaVersion(),
                value.objectKeyHash(),
                value.objectKey(),
                value.objectId(),
                value.objectKindId(),
                value.objectLength(),
                value.storageChecksumType(),
                value.storageChecksumValue(),
                value.contentSha256(),
                value.etag(),
                PhysicalObjectLifecycle.MARKED,
                Math.addExact(value.lifecycleEpoch(), 1),
                value.createdAtMillis(),
                value.orphanNotBeforeMillis(),
                "t".repeat(52),
                sha('4').value(),
                10,
                20,
                0,
                0,
                0,
                "",
                "",
                version);
        return new VersionedPhysicalObjectRoot(
                source.key(), changed, version, sha('4'));
    }

    private static OxiaMetadataStore l0Store(StoreState state) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    return switch (method.getName()) {
                        case "getStreamSnapshot" -> {
                            int read = state.l0Reads.incrementAndGet();
                            StreamMetadataSnapshot snapshot = state.streamSnapshot.get();
                            if (state.loseCompletedTrimOnRevalidation.get()
                                    && read > 1) {
                                snapshot = snapshot(Math.subtractExact(
                                        snapshot.trim().trimOffset(), 1));
                            }
                            yield CompletableFuture.completedFuture(
                                    snapshot);
                        }
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static StreamMetadataSnapshot snapshot(long trimOffset) {
        long version = 7;
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/pre-drain",
                        "pre-drain-name-hash",
                        StreamState.ACTIVE.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        version),
                new CommittedEndOffsetRecord(
                        STREAM.value(), 12, 12, 1, version),
                new TrimRecord(
                        STREAM.value(), trimOffset, "test", 2, version));
    }

    private static GenerationMetadataStore generationStore(
            StoreState state,
            boolean loseCasResponse) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    return switch (method.getName()) {
                        case "scanIndex" -> {
                            state.scanCalls.incrementAndGet();
                            ReadView sourceView = ReadView.fromWireId(
                                    state.source.get().value().readViewId());
                            yield CompletableFuture.completedFuture(
                                    new GenerationScanPage(
                                            args[2] == sourceView
                                                    ? List.of(
                                                            state.source.get(),
                                                            state.replacement.get())
                                                    : List.of(),
                                            Optional.empty()));
                        }
                        case "getRecoveryRoot" -> CompletableFuture.completedFuture(
                                Optional.of(state.fixture.recoveryRoot()));
                        case "getIndex" -> {
                            GenerationIndexIdentity identity =
                                    (GenerationIndexIdentity) args[1];
                            if (identity.equals(identity(state.source.get()))) {
                                yield CompletableFuture.completedFuture(
                                        Optional.of(state.source.get()));
                            }
                            yield CompletableFuture.completedFuture(
                                    identity.equals(identity(state.replacement.get()))
                                            ? Optional.of(state.replacement.get())
                                            : Optional.empty());
                        }
                        case "getCandidateByKey" -> {
                            String key = (String) args[3];
                            if (key.equals(state.source.get().key())) {
                                yield CompletableFuture.completedFuture(
                                        Optional.of(state.source.get()));
                            }
                            yield CompletableFuture.completedFuture(
                                    key.equals(state.replacement.get().key())
                                            ? Optional.of(state.replacement.get())
                                            : Optional.empty());
                        }
                        case "compareAndSetIndex" -> {
                            state.casCalls.incrementAndGet();
                            GenerationIndexRecord requested =
                                    (GenerationIndexRecord) args[1];
                            long expectedVersion = (long) args[2];
                            VersionedGenerationIndex current = state.source.get();
                            if (current.metadataVersion() != expectedVersion) {
                                yield CompletableFuture.failedFuture(
                                        new IllegalStateException("version mismatch"));
                            }
                            long version = Math.addExact(expectedVersion, 1);
                            GenerationIndexRecord stored = requested.withMetadataVersion(
                                    version);
                            VersionedGenerationIndex updated = new VersionedGenerationIndex(
                                    current.key(),
                                    stored,
                                    version,
                                    GenerationIndexDigests.durableValueSha256(requested));
                            state.source.set(updated);
                            if (loseCasResponse) {
                                yield CompletableFuture.failedFuture(
                                        new IllegalStateException("lost CAS response"));
                            }
                            yield CompletableFuture.completedFuture(updated);
                        }
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static PhysicalObjectMetadataStore physicalStore(
            StoreState state,
            boolean changeCandidateRootAtFinalFence) {
        return (PhysicalObjectMetadataStore) Proxy.newProxyInstance(
                PhysicalObjectMetadataStore.class.getClassLoader(),
                new Class<?>[] {PhysicalObjectMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    return switch (method.getName()) {
                        case "getRoot" -> {
                            ObjectKeyHash object = (ObjectKeyHash) args[1];
                            VersionedPhysicalObjectRoot candidate =
                                    state.candidateRoot.get();
                            if (object.value().equals(
                                    candidate.value().objectKeyHash())) {
                                int read = state.candidateRootReads.incrementAndGet();
                                VersionedPhysicalObjectRoot returned =
                                        changeCandidateRootAtFinalFence && read > 1
                                                ? markedRoot(candidate)
                                                : candidate;
                                yield CompletableFuture.completedFuture(
                                        Optional.of(returned));
                            }
                            VersionedPhysicalObjectRoot replacement =
                                    state.replacementRoot.get();
                            state.replacementRootReads.incrementAndGet();
                            yield CompletableFuture.completedFuture(
                                    object.value().equals(
                                                    replacement.value().objectKeyHash())
                                            ? Optional.of(replacement)
                                            : Optional.empty());
                        }
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static RecoveryCheckpointCodecV1 checkpointCodec(StoreState state) {
        return (RecoveryCheckpointCodecV1) Proxy.newProxyInstance(
                RecoveryCheckpointCodecV1.class.getClassLoader(),
                new Class<?>[] {RecoveryCheckpointCodecV1.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    return switch (method.getName()) {
                        case "openAndVerify" -> {
                            state.openCalls.incrementAndGet();
                            yield CompletableFuture.completedFuture(
                                    state.fixture.checkpoint());
                        }
                        case "findCommitCoveringOffset" ->
                                CompletableFuture.completedFuture(
                                        Optional.of(state.fixture.entry()));
                        case "scanPublications" -> {
                            state.publicationReads.incrementAndGet();
                            yield CompletableFuture.completedFuture(
                                    new RecoveryCheckpointPublicationPage(
                                            List.of(state.fixture.publication()),
                                            OptionalInt.empty()));
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static SourceRetirementMetadataStore sourceStore() {
        return (SourceRetirementMetadataStore) Proxy.newProxyInstance(
                SourceRetirementMetadataStore.class.getClassLoader(),
                new Class<?>[] {SourceRetirementMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Object objectMethod(Object proxy, String name, Object[] args) {
        return switch (name) {
            case "toString" -> "pre-drain-test-proxy";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException(name);
        };
    }

    private static GenerationIndexIdentity identity(
            VersionedGenerationIndex index) {
        GenerationIndexRecord value = index.value();
        return new GenerationIndexIdentity(
                STREAM,
                ReadView.fromWireId(value.readViewId()),
                value.offsetEnd(),
                value.generation());
    }

    private static PhysicalGcConfig enabledConfig() {
        PhysicalGcConfig defaults = PhysicalGcConfig.defaults();
        return new PhysicalGcConfig(
                true,
                false,
                defaults.metadataScanPageSize(),
                defaults.objectListPageSize(),
                defaults.maxConcurrentDeletes(),
                defaults.maxStreamsPerCandidate(),
                defaults.maxAuthoritiesPerDomainSnapshot(),
                defaults.maxReferencesPerDomainSnapshot(),
                defaults.scanInterval(),
                defaults.readerLeaseDuration(),
                defaults.readerLeaseRenewInterval(),
                defaults.maximumClockSkew(),
                defaults.drainGrace(),
                defaults.pendingProtectionDuration(),
                defaults.orphanGrace(),
                defaults.tombstoneAuditGrace(),
                defaults.operationTimeout(),
                defaults.closeTimeout());
    }

    private static PhysicalGcConfig disabledConfig() {
        return PhysicalGcConfig.defaults();
    }

    private static Clock fixedClock() {
        return Clock.fixed(
                Instant.ofEpochMilli(100), ZoneOffset.UTC);
    }

    private static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256, String.valueOf(value).repeat(64));
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static final class StoreState {
        private final Fixture fixture;
        private final AtomicReference<VersionedGenerationIndex> source;
        private final AtomicReference<VersionedGenerationIndex> replacement;
        private final AtomicReference<VersionedPhysicalObjectRoot> candidateRoot;
        private final AtomicReference<VersionedPhysicalObjectRoot> replacementRoot;
        private final AtomicReference<StreamMetadataSnapshot> streamSnapshot;
        private final AtomicBoolean loseCompletedTrimOnRevalidation =
                new AtomicBoolean();
        private final AtomicInteger l0Reads = new AtomicInteger();
        private final AtomicInteger scanCalls = new AtomicInteger();
        private final AtomicInteger openCalls = new AtomicInteger();
        private final AtomicInteger publicationReads = new AtomicInteger();
        private final AtomicInteger candidateRootReads = new AtomicInteger();
        private final AtomicInteger replacementRootReads = new AtomicInteger();
        private final AtomicInteger casCalls = new AtomicInteger();

        private StoreState(Fixture fixture) {
            this.fixture = fixture;
            source = new AtomicReference<>(fixture.source());
            replacement = new AtomicReference<>(fixture.replacement());
            candidateRoot = new AtomicReference<>(fixture.candidateRoot());
            replacementRoot = new AtomicReference<>(fixture.replacementRoot());
            streamSnapshot = new AtomicReference<>(snapshot(0));
        }
    }

    private record Fixture(
            VersionedGenerationIndex source,
            VersionedGenerationIndex replacement,
            VersionedRecoveryCheckpointRoot recoveryRoot,
            RecoveryCheckpointObject checkpoint,
            RecoveryCheckpointEntry entry,
            RecoveryCheckpointPublication publication,
            VersionedPhysicalObjectRoot candidateRoot,
            VersionedPhysicalObjectRoot replacementRoot,
            GcCandidate candidate) {
        private Fixture withReplacementRoot(
                VersionedPhysicalObjectRoot replacement) {
            return new Fixture(
                    source,
                    this.replacement,
                    recoveryRoot,
                    checkpoint,
                    entry,
                    publication,
                    candidateRoot,
                    replacement,
                    candidate);
        }

        private Fixture withSource(VersionedGenerationIndex replacementSource) {
            return new Fixture(
                    replacementSource,
                    replacement,
                    recoveryRoot,
                    checkpoint,
                    entry,
                    publication,
                    candidateRoot,
                    replacementRoot,
                    candidate);
        }
    }
}
