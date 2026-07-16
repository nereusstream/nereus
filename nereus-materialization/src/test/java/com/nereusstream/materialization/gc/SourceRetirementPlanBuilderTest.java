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
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationIndexDigests;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.GenerationZeroIndexEncoding;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.RecoveryCheckpointRootDigests;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.retirement.GenericCommittedAppendIdentity;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroCommit;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroMarker;
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
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SourceRetirementPlanBuilderTest {
    private static final String CLUSTER = "cluster-source-plan";
    private static final StreamId STREAM = new StreamId("tenant/ns/source-plan");
    private static final String COMMIT_ID = "commit-source-plan";

    @Test
    void freezesIndexMarkerAndCheckpointReplacedCommitThenReloadsExactFacts() {
        Fixture fixture = fixture();
        AtomicReference<VersionedGenerationZeroMarker> marker = new AtomicReference<>(
                fixture.marker());
        AtomicInteger rootReads = new AtomicInteger();
        AtomicInteger physicalRootReads = new AtomicInteger();
        SourceRetirementPlanBuilder builder = new SourceRetirementPlanBuilder(
                CLUSTER,
                generationStore(fixture, rootReads, false),
                physicalStore(fixture, physicalRootReads, false),
                sourceStore(fixture, marker),
                checkpointCodec(fixture),
                PhysicalGcConfig.defaults());

        List<GcPlannedMetadataRemoval> removals = builder.build(fixture.candidate()).join();

        assertThat(removals).extracting(GcPlannedMetadataRemoval::removalType)
                .containsExactlyInAnyOrder(
                        GenerationZeroIndexRetirementHandler.REMOVAL_TYPE,
                        GenerationZeroMarkerRetirementHandler.REMOVAL_TYPE,
                        GenerationZeroCommitRetirementHandler.REMOVAL_TYPE);
        assertThat(removals).extracting(GcPlannedMetadataRemoval::key)
                .containsExactlyInAnyOrder(
                        fixture.index().key(),
                        fixture.marker().key(),
                        fixture.commit().key());
        assertThat(rootReads).hasValue(4);
        assertThat(physicalRootReads).hasValue(4);
        assertThat(builder.reload(fixture.candidate(), removals).join())
                .isEqualTo(removals);
        assertThat(rootReads).hasValue(6);
        assertThat(physicalRootReads).hasValue(6);

        marker.set(new VersionedGenerationZeroMarker(
                fixture.marker().key(),
                STREAM,
                fixture.marker().identity(),
                0,
                12,
                1,
                fixture.marker().readTargetIdentitySha256(),
                fixture.marker().metadataVersion() + 1,
                sha('f')));
        assertThat(builder.reload(fixture.candidate(), removals).join())
                .isNotEqualTo(removals);
    }

    @Test
    void recoveryRootChangeDuringFreezeRejectsThePlan() {
        Fixture fixture = fixture();
        AtomicInteger rootReads = new AtomicInteger();
        SourceRetirementPlanBuilder builder = new SourceRetirementPlanBuilder(
                CLUSTER,
                generationStore(fixture, rootReads, true),
                physicalStore(fixture, new AtomicInteger(), false),
                sourceStore(fixture, new AtomicReference<>(fixture.marker())),
                checkpointCodec(fixture),
                PhysicalGcConfig.defaults());

        assertThatThrownBy(() -> builder.build(fixture.candidate()).join())
                .hasRootCauseMessage(
                        "recovery root changed while source facts were frozen");
        assertThat(rootReads).hasValue(2);
    }

    @Test
    void quarantinedReplacementIndexCannotAuthorizeSourceRetirement() {
        Fixture fixture = fixture();
        Fixture quarantined = fixture.withReplacementIndex(
                generationWithLifecycle(
                        fixture.replacementIndex(),
                        GenerationLifecycle.QUARANTINED,
                        "target-corrupt"));
        AtomicInteger physicalReads = new AtomicInteger();
        SourceRetirementPlanBuilder builder = new SourceRetirementPlanBuilder(
                CLUSTER,
                generationStore(quarantined, new AtomicInteger(), false),
                physicalStore(quarantined, physicalReads, false),
                sourceStore(quarantined, new AtomicReference<>(quarantined.marker())),
                checkpointCodec(quarantined),
                PhysicalGcConfig.defaults());

        assertThatThrownBy(() -> builder.build(quarantined.candidate()).join())
                .hasRootCauseMessage(
                        "generation-zero source has no current healthy NRC1 replacement");
        assertThat(physicalReads).hasValue(0);
    }

    @Test
    void markedReplacementRootCannotAuthorizeSourceRetirement() {
        Fixture fixture = fixture();
        Fixture marked = fixture.withReplacementRoot(
                markedRoot(fixture.replacementRoot()));
        SourceRetirementPlanBuilder builder = new SourceRetirementPlanBuilder(
                CLUSTER,
                generationStore(marked, new AtomicInteger(), false),
                physicalStore(marked, new AtomicInteger(), false),
                sourceStore(marked, new AtomicReference<>(marked.marker())),
                checkpointCodec(marked),
                PhysicalGcConfig.defaults());

        assertThatThrownBy(() -> builder.build(marked.candidate()).join())
                .hasRootCauseMessage(
                        "generation-zero source has no current healthy NRC1 replacement");
    }

    @Test
    void replacementIndexChangeDuringFreezeRejectsThePlan() {
        Fixture fixture = fixture();
        AtomicInteger indexReads = new AtomicInteger();
        SourceRetirementPlanBuilder builder = new SourceRetirementPlanBuilder(
                CLUSTER,
                generationStore(
                        fixture,
                        new AtomicInteger(),
                        false,
                        indexReads,
                        true),
                physicalStore(fixture, new AtomicInteger(), false),
                sourceStore(fixture, new AtomicReference<>(fixture.marker())),
                checkpointCodec(fixture),
                PhysicalGcConfig.defaults());

        assertThatThrownBy(() -> builder.build(fixture.candidate()).join())
                .hasRootCauseMessage(
                        "healthy NRC1 replacement index changed while source facts were frozen");
        assertThat(indexReads).hasValue(2);
    }

    @Test
    void replacementRootChangeDuringFreezeRejectsThePlan() {
        Fixture fixture = fixture();
        AtomicInteger physicalReads = new AtomicInteger();
        SourceRetirementPlanBuilder builder = new SourceRetirementPlanBuilder(
                CLUSTER,
                generationStore(fixture, new AtomicInteger(), false),
                physicalStore(fixture, physicalReads, true),
                sourceStore(fixture, new AtomicReference<>(fixture.marker())),
                checkpointCodec(fixture),
                PhysicalGcConfig.defaults());

        assertThatThrownBy(() -> builder.build(fixture.candidate()).join())
                .hasRootCauseMessage(
                        "healthy NRC1 replacement root changed while source facts were frozen");
        assertThat(physicalReads).hasValue(2);
    }

    @Test
    void revalidatorRejectsAnUnboundExtraSourceRemoval() {
        Fixture fixture = fixture();
        VersionedGenerationZeroCommit unbound = commitWithId(
                fixture.commit(), "unbound-commit", 11, sha('f'));
        SourceRetirementPlanBuilder builder = new SourceRetirementPlanBuilder(
                CLUSTER,
                generationStore(fixture, new AtomicInteger(), false),
                physicalStore(fixture, new AtomicInteger(), false),
                sourceStore(
                        fixture,
                        new AtomicReference<>(fixture.marker()),
                        Optional.of(unbound)),
                checkpointCodec(fixture),
                PhysicalGcConfig.defaults());
        List<GcPlannedMetadataRemoval> removals = builder.build(
                fixture.candidate()).join();
        List<GcPlannedMetadataRemoval> withExtra = new java.util.ArrayList<>(removals);
        withExtra.add(new GcPlannedMetadataRemoval(
                GenerationZeroCommitRetirementHandler.REMOVAL_TYPE,
                unbound.key(),
                unbound.metadataVersion(),
                unbound.durableValueSha256()));
        withExtra.sort(GcPlanValidation.METADATA_ORDER);

        assertThatThrownBy(() -> builder.reload(
                        fixture.candidate(), withExtra).join())
                .hasRootCauseMessage(
                        "source-retirement removals are not exactly bound to the candidate");
    }

    @Test
    void checkpointEntryWithAnotherCanonicalCommitCannotAuthorizeSourceRetirement() {
        Fixture fixture = fixture();
        StreamCommitTargetRecord different = withReadTargetIdentity(
                fixture.commit().canonicalCommit(), sha('f').value());
        byte[] bytes = MetadataRecordCodecFactory.encodeEnvelope(
                different, StreamCommitTargetRecord.class);
        RecoveryCheckpointEntry differentEntry = new RecoveryCheckpointEntry(
                1,
                new OffsetRange(0, 12),
                12,
                COMMIT_ID,
                "",
                ByteBuffer.wrap(bytes),
                sha256(bytes),
                List.of(0));
        Fixture conflicting = fixture.withEntry(differentEntry);
        SourceRetirementPlanBuilder builder = new SourceRetirementPlanBuilder(
                CLUSTER,
                generationStore(conflicting, new AtomicInteger(), false),
                physicalStore(conflicting, new AtomicInteger(), false),
                sourceStore(conflicting, new AtomicReference<>(conflicting.marker())),
                checkpointCodec(conflicting),
                PhysicalGcConfig.defaults());

        assertThatThrownBy(() -> builder.build(conflicting.candidate()).join())
                .hasRootCauseMessage(
                        "checkpoint-replaced commit does not match generation-zero index facts");
    }

    private static Fixture fixture() {
        ObjectSliceReadTarget target = target();
        var encodedTarget = ReadTargetCodecRegistry.phase15().encode(target);
        StreamCommitTargetRecord canonical = new StreamCommitTargetRecord(
                STREAM.value(),
                COMMIT_ID,
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
                encodedTarget,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                12,
                1,
                12,
                List.of(),
                CommitSliceRequest.emptyProjectionIdentity(),
                0,
                0,
                10,
                0);
        byte[] canonicalBytes = MetadataRecordCodecFactory.encodeEnvelope(
                canonical, StreamCommitTargetRecord.class);
        Checksum canonicalSha = sha256(canonicalBytes);
        OffsetIndexEntry indexValue = new OffsetIndexEntry(
                STREAM,
                new OffsetRange(0, 12),
                0,
                12,
                target,
                PayloadFormat.PULSAR_ENTRY_BATCH,
                12,
                1,
                12,
                List.of(),
                Optional.empty(),
                1,
                false,
                7);
        String indexKey = new F4Keyspace(CLUSTER).generationIndexKey(
                STREAM, ReadView.COMMITTED, 12, 0);
        VersionedGenerationZeroIndex index = new VersionedGenerationZeroIndex(
                indexKey,
                GenerationZeroIndexEncoding.GENERIC_OFFSET_INDEX_TARGET_RECORD,
                indexValue,
                7,
                sha('a'));
        GenericCommittedAppendIdentity markerIdentity =
                new GenericCommittedAppendIdentity(COMMIT_ID);
        OxiaKeyspace l0Keys = new OxiaKeyspace(CLUSTER);
        VersionedGenerationZeroMarker marker = new VersionedGenerationZeroMarker(
                l0Keys.committedAppendKey(STREAM, COMMIT_ID),
                STREAM,
                markerIdentity,
                0,
                12,
                1,
                Optional.of(new Checksum(
                        ChecksumType.SHA256,
                        encodedTarget.identityChecksumValue())),
                8,
                sha('b'));
        VersionedGenerationZeroCommit commit = new VersionedGenerationZeroCommit(
                l0Keys.streamCommitKey(STREAM, COMMIT_ID),
                STREAM,
                COMMIT_ID,
                AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1,
                markerIdentity,
                canonical,
                0,
                12,
                1,
                canonicalSha,
                9,
                sha('c'));
        ObjectSliceReadTarget replacementTarget = replacementTarget();
        var encodedReplacement = ReadTargetCodecRegistry.phase15().encode(
                replacementTarget);
        String publicationId = "p".repeat(52);
        GenerationIndexRecord replacementValue = new GenerationIndexRecord(
                1,
                STREAM.value(),
                ReadView.COMMITTED.wireId(),
                0,
                12,
                1,
                publicationId,
                "task-source-plan",
                GenerationLifecycle.COMMITTED,
                sha('3').value(),
                sha('4').value(),
                encodedReplacement,
                encodedReplacement.identityChecksumValue(),
                sha('4').value(),
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
                2,
                "",
                2,
                6);
        String replacementKey = new F4Keyspace(CLUSTER).generationIndexKey(
                STREAM, ReadView.COMMITTED, 12, 1);
        VersionedGenerationIndex replacementIndex = new VersionedGenerationIndex(
                replacementKey,
                replacementValue,
                replacementValue.metadataVersion(),
                GenerationIndexDigests.durableValueSha256(
                        replacementValue.withMetadataVersion(0)));
        GenerationIndexRecord embeddedReplacement = replacementValue.withMetadataVersion(0);
        byte[] publicationBytes = new GenerationIndexRecordCodecV1().encode(
                embeddedReplacement);
        RecoveryCheckpointPublication publication = new RecoveryCheckpointPublication(
                1,
                new PublicationId(publicationId),
                new OffsetRange(0, 12),
                ByteBuffer.wrap(publicationBytes),
                GenerationIndexDigests.canonicalRecordSha256(embeddedReplacement));
        ObjectKeyHash replacementHash = ObjectKeyHash.from(
                replacementTarget.objectKey());
        PhysicalObjectRootRecord replacementRootValue = new PhysicalObjectRootRecord(
                1,
                replacementHash.value(),
                replacementTarget.objectKey().value(),
                replacementTarget.objectId().value(),
                PhysicalObjectKind.COMMITTED_COMPACTED.wireId(),
                100,
                ChecksumType.CRC32C.name(),
                "55667788",
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
                4);
        VersionedPhysicalObjectRoot replacementRoot = new VersionedPhysicalObjectRoot(
                new F4Keyspace(CLUSTER).physicalRootKey(replacementHash),
                replacementRootValue,
                replacementRootValue.metadataVersion(),
                sha('5'));
        RecoveryCheckpointWriteRequest header = new RecoveryCheckpointWriteRequest(
                CLUSTER,
                STREAM,
                1,
                "d".repeat(52),
                new OffsetRange(0, 12),
                1,
                1,
                0,
                12,
                COMMIT_ID,
                COMMIT_ID,
                COMMIT_ID,
                1,
                sha('e'),
                1,
                1);
        Checksum checkpointContent = sha('1');
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
                sha('2'),
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
                COMMIT_ID,
                COMMIT_ID,
                COMMIT_ID,
                1,
                header.projectionIdentitySha256().value(),
                checkpoint.objectId().value(),
                checkpoint.objectKey().value(),
                checkpoint.objectKey().value().isEmpty()
                        ? ""
                        : com.nereusstream.api.ObjectKeyHash.from(
                                checkpoint.objectKey()).value(),
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
                COMMIT_ID,
                COMMIT_ID,
                List.of(reference),
                RecoveryCheckpointRootDigests.checkpointSetSha256(
                        List.of(reference)).value(),
                COMMIT_ID,
                1,
                20,
                5);
        VersionedRecoveryCheckpointRoot root = new VersionedRecoveryCheckpointRoot(
                new F4Keyspace(CLUSTER).recoveryRootKey(STREAM),
                rootValue,
                5,
                sha('d'));
        RecoveryCheckpointEntry entry = new RecoveryCheckpointEntry(
                1,
                new OffsetRange(0, 12),
                12,
                COMMIT_ID,
                "",
                ByteBuffer.wrap(canonicalBytes),
                canonicalSha,
                List.of(0));
        PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                target.objectKey(),
                Optional.of(target.objectId()),
                PhysicalObjectKind.OBJECT_WAL,
                100,
                new Checksum(ChecksumType.CRC32C, "11223344"),
                Optional.empty(),
                Optional.empty());
        Checksum evidence = sha('9');
        GcReferenceQuery query = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object,
                List.of(STREAM),
                evidence);
        GcCandidate candidate = new GcCandidate(
                "a".repeat(52),
                object,
                query,
                evidence,
                GcCandidateRootState.ACTIVE_DISCOVERY,
                3,
                1,
                30,
                30);
        return new Fixture(
                index,
                marker,
                commit,
                replacementIndex,
                replacementRoot,
                publication,
                root,
                checkpoint,
                entry,
                candidate);
    }

    private static GenerationMetadataStore generationStore(
            Fixture fixture,
            AtomicInteger rootReads,
            boolean loseRootOnFinalReload) {
        return generationStore(
                fixture,
                rootReads,
                loseRootOnFinalReload,
                new AtomicInteger(),
                false);
    }

    private static GenerationMetadataStore generationStore(
            Fixture fixture,
            AtomicInteger rootReads,
            boolean loseRootOnFinalReload,
            AtomicInteger indexReads,
            boolean changeIndexOnFinalReload) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    return switch (method.getName()) {
                        case "scanIndex" -> CompletableFuture.completedFuture(
                                new GenerationScanPage(
                                        args[2] == ReadView.COMMITTED
                                                ? List.of(
                                                        fixture.index(),
                                                        fixture.replacementIndex())
                                                : List.of(),
                                        Optional.empty()));
                        case "getRecoveryRoot" -> {
                            int read = rootReads.incrementAndGet();
                            yield CompletableFuture.completedFuture(
                                    loseRootOnFinalReload && read > 1
                                            ? Optional.empty()
                                            : Optional.of(fixture.root()));
                        }
                        case "getIndex" -> {
                            int read = indexReads.incrementAndGet();
                            VersionedGenerationIndex value =
                                    changeIndexOnFinalReload && read > 1
                                            ? generationWithLifecycle(
                                                    fixture.replacementIndex(),
                                                    GenerationLifecycle.QUARANTINED,
                                                    "target-changed")
                                            : fixture.replacementIndex();
                            yield CompletableFuture.completedFuture(
                                    replacementIdentity(fixture).equals(args[1])
                                            ? Optional.of(value)
                                            : Optional.empty());
                        }
                        case "getCandidateByKey" -> {
                            String key = (String) args[3];
                            if (fixture.index().key().equals(key)) {
                                yield CompletableFuture.completedFuture(
                                        Optional.of(fixture.index()));
                            }
                            yield CompletableFuture.completedFuture(
                                    fixture.replacementIndex().key().equals(key)
                                            ? Optional.of(fixture.replacementIndex())
                                            : Optional.empty());
                        }
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static PhysicalObjectMetadataStore physicalStore(
            Fixture fixture,
            AtomicInteger rootReads,
            boolean changeOnFinalReload) {
        return (PhysicalObjectMetadataStore) Proxy.newProxyInstance(
                PhysicalObjectMetadataStore.class.getClassLoader(),
                new Class<?>[] {PhysicalObjectMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    return switch (method.getName()) {
                        case "getRoot" -> {
                            int read = rootReads.incrementAndGet();
                            VersionedPhysicalObjectRoot value =
                                    changeOnFinalReload && read > 1
                                            ? markedRoot(fixture.replacementRoot())
                                            : fixture.replacementRoot();
                            yield CompletableFuture.completedFuture(
                                    new ObjectKeyHash(value.value().objectKeyHash())
                                                    .equals(args[1])
                                            ? Optional.of(value)
                                            : Optional.empty());
                        }
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static SourceRetirementMetadataStore sourceStore(
            Fixture fixture,
            AtomicReference<VersionedGenerationZeroMarker> marker) {
        return sourceStore(fixture, marker, Optional.empty());
    }

    private static SourceRetirementMetadataStore sourceStore(
            Fixture fixture,
            AtomicReference<VersionedGenerationZeroMarker> marker,
            Optional<VersionedGenerationZeroCommit> extraCommit) {
        return (SourceRetirementMetadataStore) Proxy.newProxyInstance(
                SourceRetirementMetadataStore.class.getClassLoader(),
                new Class<?>[] {SourceRetirementMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    return switch (method.getName()) {
                        case "getCommitNodeByKey" -> {
                            String key = (String) args[1];
                            yield CompletableFuture.completedFuture(
                                    fixture.commit().key().equals(key)
                                            ? Optional.of(fixture.commit())
                                            : extraCommit.filter(value -> value.key().equals(key)));
                        }
                        case "getCommittedMarkerByKey" -> CompletableFuture.completedFuture(
                                marker.get() != null && marker.get().key().equals(args[1])
                                        ? Optional.of(marker.get())
                                        : Optional.empty());
                        case "getCommittedMarker" -> CompletableFuture.completedFuture(
                                marker.get() != null && marker.get().identity().equals(args[2])
                                        ? Optional.of(marker.get())
                                        : Optional.empty());
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static RecoveryCheckpointCodecV1 checkpointCodec(Fixture fixture) {
        return (RecoveryCheckpointCodecV1) Proxy.newProxyInstance(
                RecoveryCheckpointCodecV1.class.getClassLoader(),
                new Class<?>[] {RecoveryCheckpointCodecV1.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return objectMethod(proxy, method.getName(), args);
                    }
                    return switch (method.getName()) {
                        case "openAndVerify" -> CompletableFuture.completedFuture(
                                fixture.checkpoint());
                        case "findCommitCoveringOffset" -> CompletableFuture.completedFuture(
                                Optional.of(fixture.entry()));
                        case "scanPublications" -> CompletableFuture.completedFuture(
                                new RecoveryCheckpointPublicationPage(
                                        List.of(fixture.publication()),
                                        OptionalInt.empty()));
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static GenerationIndexIdentity replacementIdentity(Fixture fixture) {
        GenerationIndexRecord value = fixture.replacementIndex().value();
        return new GenerationIndexIdentity(
                STREAM,
                ReadView.COMMITTED,
                value.offsetEnd(),
                value.generation());
    }

    private static Object objectMethod(Object proxy, String name, Object[] args) {
        return switch (name) {
            case "toString" -> "source-retirement-test-proxy";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException(name);
        };
    }

    private static ObjectSliceReadTarget target() {
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
                new ObjectId("source-object"),
                new ObjectKey("objects/source-object"),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "slice-source",
                0,
                100,
                new Checksum(ChecksumType.CRC32C, "11223344"),
                entryIndex);
    }

    private static ObjectSliceReadTarget replacementTarget() {
        EntryIndexRef entryIndex = new EntryIndexRef(
                EntryIndexLocation.INLINE,
                Optional.empty(),
                Optional.empty(),
                Optional.of(new byte[] {4, 5, 6}),
                0,
                0,
                new Checksum(ChecksumType.CRC32C, "05060708"));
        return new ObjectSliceReadTarget(
                1,
                new ObjectId("replacement-object"),
                new ObjectKey("objects/replacement-object"),
                ObjectType.STREAM_COMPACTED_OBJECT,
                "NEREUS_COMPACTED_PARQUET_V1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "slice-replacement",
                0,
                100,
                new Checksum(ChecksumType.CRC32C, "55667788"),
                entryIndex);
    }

    private static VersionedPhysicalObjectRoot markedRoot(
            VersionedPhysicalObjectRoot source) {
        PhysicalObjectRootRecord value = source.value();
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
                "m".repeat(52),
                sha('6').value(),
                10,
                20,
                0,
                0,
                0,
                "",
                "",
                Math.addExact(source.metadataVersion(), 1));
        return new VersionedPhysicalObjectRoot(
                source.key(),
                changed,
                changed.metadataVersion(),
                sha('6'));
    }

    private static VersionedGenerationIndex generationWithLifecycle(
            VersionedGenerationIndex source,
            GenerationLifecycle lifecycle,
            String reason) {
        GenerationIndexRecord value = source.value();
        long version = Math.addExact(source.metadataVersion(), 1);
        GenerationIndexRecord changed = new GenerationIndexRecord(
                value.schemaVersion(),
                value.streamId(),
                value.readViewId(),
                value.offsetStart(),
                value.offsetEnd(),
                value.generation(),
                value.publicationId(),
                value.taskId(),
                lifecycle,
                value.sourceSetSha256(),
                value.policySha256(),
                value.readTarget(),
                value.targetIdentitySha256(),
                value.materializationPolicySha256(),
                value.payloadFormat(),
                value.sourceRecordCount(),
                value.outputRecordCount(),
                value.entryCount(),
                value.logicalBytes(),
                value.cumulativeSizeAtStart(),
                value.cumulativeSizeAtEnd(),
                value.firstCommitVersion(),
                value.lastCommitVersion(),
                value.schemaRefs(),
                value.projectionRef(),
                value.createdAtMillis(),
                value.committedAtMillis(),
                reason,
                Math.addExact(value.stateChangedAtMillis(), 1),
                version);
        return new VersionedGenerationIndex(
                source.key(),
                changed,
                version,
                GenerationIndexDigests.durableValueSha256(
                        changed.withMetadataVersion(0)));
    }

    private static StreamCommitTargetRecord withReadTargetIdentity(
            StreamCommitTargetRecord source,
            String identity) {
        var target = source.readTarget();
        var changedTarget = new com.nereusstream.metadata.oxia.records.ReadTargetRecord(
                target.targetType(),
                target.targetVersion(),
                target.payloadEncoding(),
                target.payload(),
                target.identityChecksumType(),
                identity);
        return new StreamCommitTargetRecord(
                source.streamId(),
                source.commitId(),
                source.previousCommitId(),
                source.offsetStart(),
                source.offsetEnd(),
                source.generation(),
                source.cumulativeSize(),
                source.commitVersion(),
                source.writerId(),
                source.writerRunIdHash(),
                source.writerEpoch(),
                source.fencingTokenHash(),
                changedTarget,
                source.payloadFormat(),
                source.recordCount(),
                source.entryCount(),
                source.logicalBytes(),
                source.schemaRefs(),
                source.projectionRef(),
                source.minEventTimeMillis(),
                source.maxEventTimeMillis(),
                source.preparedAtMillis(),
                source.metadataVersion());
    }

    private static VersionedGenerationZeroCommit commitWithId(
            VersionedGenerationZeroCommit source,
            String commitId,
            long metadataVersion,
            Checksum durableDigest) {
        StreamCommitTargetRecord value = source.canonicalCommit();
        StreamCommitTargetRecord changed = new StreamCommitTargetRecord(
                value.streamId(),
                commitId,
                value.previousCommitId(),
                value.offsetStart(),
                value.offsetEnd(),
                value.generation(),
                value.cumulativeSize(),
                value.commitVersion(),
                value.writerId(),
                value.writerRunIdHash(),
                value.writerEpoch(),
                value.fencingTokenHash(),
                value.readTarget(),
                value.payloadFormat(),
                value.recordCount(),
                value.entryCount(),
                value.logicalBytes(),
                value.schemaRefs(),
                value.projectionRef(),
                value.minEventTimeMillis(),
                value.maxEventTimeMillis(),
                value.preparedAtMillis(),
                value.metadataVersion());
        byte[] bytes = MetadataRecordCodecFactory.encodeEnvelope(
                changed, StreamCommitTargetRecord.class);
        return new VersionedGenerationZeroCommit(
                new OxiaKeyspace(CLUSTER).streamCommitKey(STREAM, commitId),
                STREAM,
                commitId,
                AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1,
                new GenericCommittedAppendIdentity(commitId),
                changed,
                changed.offsetStart(),
                changed.offsetEnd(),
                changed.commitVersion(),
                sha256(bytes),
                metadataVersion,
                durableDigest);
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, String.valueOf(value).repeat(64));
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

    private record Fixture(
            VersionedGenerationZeroIndex index,
            VersionedGenerationZeroMarker marker,
            VersionedGenerationZeroCommit commit,
            VersionedGenerationIndex replacementIndex,
            VersionedPhysicalObjectRoot replacementRoot,
            RecoveryCheckpointPublication publication,
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointObject checkpoint,
            RecoveryCheckpointEntry entry,
            GcCandidate candidate) {
        private Fixture withEntry(RecoveryCheckpointEntry replacement) {
            return new Fixture(
                    index,
                    marker,
                    commit,
                    replacementIndex,
                    replacementRoot,
                    publication,
                    root,
                    checkpoint,
                    replacement,
                    candidate);
        }

        private Fixture withReplacementIndex(
                VersionedGenerationIndex replacement) {
            return new Fixture(
                    index,
                    marker,
                    commit,
                    replacement,
                    replacementRoot,
                    publication,
                    root,
                    checkpoint,
                    entry,
                    candidate);
        }

        private Fixture withReplacementRoot(
                VersionedPhysicalObjectRoot replacement) {
            return new Fixture(
                    index,
                    marker,
                    commit,
                    replacementIndex,
                    replacement,
                    publication,
                    root,
                    checkpoint,
                    entry,
                    candidate);
        }
    }
}
