/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;

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
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.core.read.GenerationIndexRepairResult;
import com.nereusstream.core.read.GenerationIndexRepairSource;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.GenerationIndexDigests;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.RecoveryCheckpointRootDigests;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.checkpoint.DefaultRecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointVerifier;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteRequest;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteResult;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointDerivedIndexRepairTest {
    private static final String CLUSTER = "checkpoint-index-repair-cluster";
    private static final StreamId STREAM =
            new StreamId("checkpoint-index-repair-stream");
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(10_000), ZoneOffset.UTC);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final ProjectionRef PROJECTION = new ProjectionRef(
            ProjectionType.PROTOCOL_HINT, "checkpoint-index-repair");
    private static final String PROJECTION_IDENTITY =
            projectionIdentity(PROJECTION);
    private static final Checksum PROJECTION_SHA = sha(PROJECTION_IDENTITY);

    @TempDir
    Path temporaryDirectory;

    @Test
    void restoresHighestHealthyPublicationAfterIndexRetirementAndProtectsTarget()
            throws Exception {
        try (Fixture fixture = fixture(false)) {
            GenerationIndexRepairResult result = fixture.repairer().repair(
                    STREAM, 0, TIMEOUT).join();

            assertThat(result.source())
                    .isEqualTo(GenerationIndexRepairSource.RECOVERY_CHECKPOINT);
            VersionedGenerationIndex restored = result.restoredIndex().orElseThrow();
            assertThat(restored.value().generation()).isOne();
            assertThat(fixture.generations().getIndex(
                            CLUSTER, fixture.lowIdentity()).join())
                    .contains(restored);
            assertThat(fixture.generations().getIndex(
                            CLUSTER, fixture.highIdentity()).join())
                    .isEmpty();
            assertThat(fixture.targetProtection(restored)).isPresent();
            assertThat(fixture.pins().acquired()).hasValue(1);
            assertThat(fixture.pins().released()).hasValue(1);
            assertThat(fixture.activation().revalidations())
                    .hasValueGreaterThanOrEqualTo(3);
        }
    }

    @Test
    void restartsWholeProofWhenRootChangesDuringCheckpointPin()
            throws Exception {
        try (Fixture fixture = fixture(true)) {
            fixture.pins().beforeFirstValidation(fixture::publishNextRoot);

            GenerationIndexRepairResult result = fixture.repairer().repair(
                    STREAM, 0, TIMEOUT).join();

            assertThat(result.restoredIndex().orElseThrow().value().generation())
                    .isEqualTo(2);
            assertThat(fixture.generations().getRecoveryRoot(CLUSTER, STREAM)
                            .join().orElseThrow().value().checkpointSequence())
                    .isEqualTo(2);
            assertThat(fixture.pins().acquired()).hasValue(1);
            assertThat(fixture.pins().released()).hasValue(1);
            assertThat(fixture.pins().validationAttempts())
                    .hasValueGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void trimmedTargetCreatesNeitherIndexNorReadPin() throws Exception {
        try (Fixture fixture = fixture(true)) {
            fixture.trimOffset().set(1);

            GenerationIndexRepairResult result = fixture.repairer().repair(
                    STREAM, 0, TIMEOUT).join();

            assertThat(result.source())
                    .isEqualTo(GenerationIndexRepairSource.TRIMMED);
            assertThat(fixture.generations().getIndex(
                            CLUSTER, fixture.lowIdentity()).join())
                    .isEmpty();
            assertThat(fixture.generations().getIndex(
                            CLUSTER, fixture.highIdentity()).join())
                    .isEmpty();
            assertThat(fixture.pins().acquired()).hasValue(0);
        }
    }

    private Fixture fixture(boolean highTargetHealthy) throws Exception {
        LocalFileObjectStore objects = new LocalFileObjectStore(
                temporaryDirectory.resolve(
                        "objects-" + highTargetHealthy));
        StagingFileManager staging = staging(temporaryDirectory.resolve(
                "staging-" + highTargetHealthy));
        DefaultRecoveryCheckpointCodecV1 codec =
                new DefaultRecoveryCheckpointCodecV1(
                        objects,
                        staging,
                        Runnable::run,
                        verifier());
        GenerationIndexRecord low = generation(1, "b".repeat(26), "low");
        GenerationIndexRecord high = generation(2, "c".repeat(26), "high");
        StreamCommitTargetRecord commit = commit(low);
        byte[] canonicalCommit = MetadataRecordCodecFactory.encodeEnvelope(
                commit, StreamCommitTargetRecord.class);
        RecoveryCheckpointWriteRequest header =
                new RecoveryCheckpointWriteRequest(
                        CLUSTER,
                        STREAM,
                        1,
                        "a".repeat(26),
                        new OffsetRange(0, 1),
                        1,
                        1,
                        0,
                        7,
                        commit.commitId(),
                        commit.commitId(),
                        commit.commitId(),
                        1,
                        PROJECTION_SHA,
                        1,
                        2);
        RecoveryCheckpointPublication lowPublication = publication(low);
        RecoveryCheckpointPublication highPublication = publication(high);
        RecoveryCheckpointEntry entry = new RecoveryCheckpointEntry(
                1,
                new OffsetRange(0, 1),
                7,
                commit.commitId(),
                "",
                ByteBuffer.wrap(canonicalCommit),
                sha(canonicalCommit),
                List.of(0, 1));
        RecoveryCheckpointWriteResult written = codec.write(
                        header,
                        publisher(List.of(lowPublication, highPublication)),
                        publisher(List.of(entry)))
                .join();
        objects.putObject(
                        written.objectKey(),
                        written.stagingFile(),
                        new PutObjectOptions(
                                RecoveryCheckpointFormatV1.CONTENT_TYPE,
                                written.storageCrc32c(),
                                true,
                                Map.of("nereus-format", "NRC1"),
                                TIMEOUT))
                .join();

        GenerationMetadataStore generations =
                GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        generations.createOrVerifyStreamRegistration(
                CLUSTER,
                new MaterializationStreamRegistrationRecord(
                        1,
                        STREAM.value(),
                        PROJECTION_IDENTITY,
                        PROJECTION_SHA.value(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        1,
                        1,
                        1,
                        0)).join();
        RecoveryCheckpointReferenceRecord reference = reference(header, written);
        VersionedRecoveryCheckpointRoot bootstrap = generations
                .getOrCreateRecoveryRoot(CLUSTER, STREAM).join();
        VersionedRecoveryCheckpointRoot root = generations
                .compareAndSetRecoveryRoot(
                        CLUSTER,
                        rootRecord(reference, 1, 2),
                        bootstrap.metadataVersion())
                .join();

        seedAndDelete(generations, low);
        seedAndDelete(generations, high);

        FakePhysicalObjectMetadataStore physical =
                new FakePhysicalObjectMetadataStore();
        createPhysicalRoot(physical, low);
        if (highTargetHealthy) {
            createPhysicalRoot(physical, high);
        }
        DefaultObjectProtectionManager protections =
                new DefaultObjectProtectionManager(
                        CLUSTER,
                        physical,
                        Duration.ofMinutes(1),
                        Duration.ofSeconds(1),
                        Duration.ofMinutes(5),
                        CLOCK);
        AtomicLong trim = new AtomicLong();
        OxiaMetadataStore l0 = l0Store(trim);
        TrackingPinManager pins = new TrackingPinManager();
        TrackingActivationGuard activation =
                new TrackingActivationGuard();
        CheckpointDerivedIndexRepairer repairer =
                new CheckpointDerivedIndexRepairer(
                        CLUSTER,
                        l0,
                        generations,
                        physical,
                        new AnchorAwareCommitWalker(
                                CLUSTER, l0, generations),
                        codec,
                        pins,
                        protections,
                        activation,
                        16,
                        4,
                        CLOCK);
        return new Fixture(
                objects,
                staging,
                written,
                generations,
                physical,
                protections,
                repairer,
                pins,
                activation,
                trim,
                reference,
                root,
                low,
                high);
    }

    private static void seedAndDelete(
            GenerationMetadataStore generations,
            GenerationIndexRecord record) {
        VersionedGenerationIndex seeded = generations
                .restoreCommittedFromCheckpoint(
                        CLUSTER,
                        record,
                        GenerationIndexDigests.canonicalRecordSha256(record))
                .join();
        generations.deleteIndex(
                CLUSTER,
                identity(record),
                seeded.metadataVersion()).join();
    }

    private static void createPhysicalRoot(
            FakePhysicalObjectMetadataStore store,
            GenerationIndexRecord record) {
        ObjectSliceReadTarget target = (ObjectSliceReadTarget)
                ReadTargetCodecRegistry.phase15().decode(record.readTarget());
        store.createRoot(
                CLUSTER,
                new PhysicalObjectRootRecord(
                        1,
                        ObjectKeyHash.from(target.objectKey()).value(),
                        target.objectKey().value(),
                        target.objectId().value(),
                        PhysicalObjectKind.COMMITTED_COMPACTED.wireId(),
                        256,
                        ChecksumType.CRC32C.name(),
                        "01020304",
                        sha("content-" + record.generation()).value(),
                        "etag-" + record.generation(),
                        PhysicalObjectLifecycle.ACTIVE,
                        1,
                        1,
                        2,
                        "",
                        "",
                        0,
                        0,
                        0,
                        0,
                        0,
                        "",
                        "",
                        0)).join();
    }

    private static OxiaMetadataStore l0Store(AtomicLong trimOffset) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getStreamSnapshot" ->
                            CompletableFuture.completedFuture(
                                    snapshot(trimOffset.get()));
                    case "readAppendRecoveryTail" -> {
                        AppendRecoveryAnchor anchor =
                                (AppendRecoveryAnchor) arguments[2];
                        AppendRecoveryHead head = new AppendRecoveryHead(
                                STREAM,
                                anchor.lastCommitId(),
                                anchor.offsetEnd(),
                                anchor.cumulativeSize(),
                                anchor.commitVersion(),
                                20);
                        yield CompletableFuture.completedFuture(
                                new AppendRecoveryTailPage(
                                        anchor,
                                        head,
                                        List.of(),
                                        true,
                                        Optional.empty()));
                    }
                    case "close" -> null;
                    case "toString" -> "checkpoint-index-repair-l0";
                    default -> throw new UnsupportedOperationException(
                            method.getName());
                });
    }

    private static StreamMetadataSnapshot snapshot(long trimOffset) {
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/topic",
                        "stream-name-hash",
                        StreamState.ACTIVE.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        7),
                new CommittedEndOffsetRecord(
                        STREAM.value(), 1, 7, 1, 7),
                new TrimRecord(
                        STREAM.value(), trimOffset, "", 1, 7));
    }

    private static GenerationIndexRecord generation(
            long generation, String publicationId, String suffix) {
        ObjectId objectId = new ObjectId("object-" + suffix);
        ObjectKey objectKey = new ObjectKey("objects/" + suffix);
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.of(objectId),
                Optional.of(objectKey),
                Optional.empty(),
                128,
                16,
                crc("05060708"));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                objectId,
                objectKey,
                ObjectType.STREAM_COMPACTED_OBJECT,
                "NEREUS_COMPACTED_PARQUET_V1",
                "OPAQUE_SLICE",
                "slice-" + suffix,
                0,
                128,
                crc("01020304"),
                index);
        var encoded = ReadTargetCodecRegistry.phase15().encode(target);
        return new GenerationIndexRecord(
                1,
                STREAM.value(),
                ReadView.COMMITTED.wireId(),
                0,
                1,
                generation,
                publicationId,
                "task-" + suffix,
                GenerationLifecycle.COMMITTED,
                sha("sources-" + suffix).value(),
                sha("policy").value(),
                encoded,
                encoded.identityChecksumValue(),
                sha("policy").value(),
                PayloadFormat.OPAQUE_RECORD_BATCH.name(),
                1,
                1,
                1,
                7,
                0,
                7,
                1,
                1,
                List.of(),
                PROJECTION_IDENTITY,
                100,
                110,
                "",
                110,
                0);
    }

    private static RecoveryCheckpointPublication publication(
            GenerationIndexRecord record) {
        byte[] canonical = new com.nereusstream.metadata.oxia.codec
                .GenerationIndexRecordCodecV1().encode(record);
        return new RecoveryCheckpointPublication(
                record.generation(),
                new PublicationId(record.publicationId()),
                new OffsetRange(record.offsetStart(), record.offsetEnd()),
                ByteBuffer.wrap(canonical),
                GenerationIndexDigests.canonicalRecordSha256(record));
    }

    private static StreamCommitTargetRecord commit(
            GenerationIndexRecord source) {
        return new StreamCommitTargetRecord(
                STREAM.value(),
                "commit-1",
                "",
                0,
                1,
                0,
                7,
                1,
                "writer",
                "writer-run",
                1,
                "fencing-hash",
                source.readTarget(),
                PayloadFormat.OPAQUE_RECORD_BATCH.name(),
                1,
                1,
                7,
                List.of(),
                PROJECTION_IDENTITY,
                1,
                1,
                1,
                0);
    }

    private static RecoveryCheckpointReferenceRecord reference(
            RecoveryCheckpointWriteRequest header,
            RecoveryCheckpointWriteResult written) {
        return new RecoveryCheckpointReferenceRecord(
                header.checkpointSequence(),
                header.checkpointAttemptId(),
                header.coverage().startOffset(),
                header.coverage().endOffset(),
                header.firstCommitVersion(),
                header.lastCommitVersion(),
                header.cumulativeSizeAtStart(),
                header.cumulativeSizeAtEnd(),
                header.firstCommitId(),
                header.lastCommitId(),
                header.sourceHeadCommitId(),
                header.sourceHeadCommitVersion(),
                header.projectionIdentitySha256().value(),
                written.objectId().value(),
                written.objectKey().value(),
                RecoveryCheckpointFormatV1.objectKeyHash(
                        written.objectKey()).value(),
                written.objectLength(),
                written.storageCrc32c().value(),
                written.contentSha256().value(),
                header.expectedEntryCount(),
                header.expectedPublicationCount());
    }

    private static RecoveryCheckpointRootRecord rootRecord(
            RecoveryCheckpointReferenceRecord reference,
            long sequence,
            long publishedAt) {
        return new RecoveryCheckpointRootRecord(
                1,
                STREAM.value(),
                sequence,
                reference.coveredStartOffset(),
                reference.coveredEndOffset(),
                reference.firstCommitVersion(),
                reference.lastCommitVersion(),
                reference.cumulativeSizeAtStart(),
                reference.cumulativeSizeAtEnd(),
                reference.firstCommitId(),
                reference.lastCommitId(),
                List.of(reference),
                RecoveryCheckpointRootDigests.checkpointSetSha256(
                        List.of(reference)).value(),
                reference.sourceHeadCommitId(),
                reference.sourceHeadCommitVersion(),
                publishedAt,
                0);
    }

    private static GenerationIndexIdentity identity(
            GenerationIndexRecord record) {
        return new GenerationIndexIdentity(
                STREAM,
                ReadView.COMMITTED,
                record.offsetEnd(),
                record.generation());
    }

    private static RecoveryCheckpointVerifier verifier() {
        return new RecoveryCheckpointVerifier() {
            @Override
            public void verifyPublication(
                    RecoveryCheckpointWriteRequest header,
                    RecoveryCheckpointPublication publication) {
            }

            @Override
            public void verifyEntry(
                    RecoveryCheckpointWriteRequest header,
                    RecoveryCheckpointEntry entry) {
            }
        };
    }

    private static StagingFileManager staging(Path directory)
            throws Exception {
        Files.createDirectory(directory);
        Files.setPosixFilePermissions(
                directory,
                PosixFilePermissions.fromString("rwx------"));
        return new StagingFileManager(
                directory,
                32L << 20,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run);
    }

    private static <T> Flow.Publisher<T> publisher(List<T> values) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean complete;

            @Override
            public void request(long count) {
                if (complete) {
                    return;
                }
                if (count != 1) {
                    complete = true;
                    subscriber.onError(new AssertionError(
                            "checkpoint writer demand must be one"));
                    return;
                }
                subscriber.onNext(values.get(index++));
                if (index == values.size()) {
                    complete = true;
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                complete = true;
            }
        });
    }

    private static String projectionIdentity(ProjectionRef projection) {
        return component("projectionRef")
                + component("present")
                + component(projection.type().name())
                + component(projection.value());
    }

    private static String component(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length + ":" + value;
    }

    private static Checksum crc(String value) {
        return new Checksum(ChecksumType.CRC32C, value);
    }

    private static Checksum sha(String value) {
        return sha(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Checksum sha(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value)));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static final class TrackingPinManager
            implements ObjectReadPinManager {
        private final AtomicInteger validationAttempts = new AtomicInteger();
        private final AtomicInteger acquired = new AtomicInteger();
        private final AtomicInteger released = new AtomicInteger();
        private final AtomicBoolean first = new AtomicBoolean(true);
        private Runnable beforeFirstValidation = () -> {
        };

        private void beforeFirstValidation(Runnable action) {
            beforeFirstValidation = action;
        }

        @Override
        public CompletableFuture<ObjectReadLease> acquire(
                PhysicalObjectIdentity object,
                long maximumReadDeadlineMillis,
                SelectionRevalidator selectionRevalidator) {
            if (first.compareAndSet(true, false)) {
                beforeFirstValidation.run();
            }
            validationAttempts.incrementAndGet();
            return selectionRevalidator.revalidate().thenApply(ignored -> {
                acquired.incrementAndGet();
                return new ObjectReadLease() {
                    private final AtomicBoolean closed = new AtomicBoolean();

                    @Override
                    public PhysicalObjectIdentity object() {
                        return object;
                    }

                    @Override
                    public String leaseId() {
                        return "checkpoint-repair-lease";
                    }

                    @Override
                    public long maximumReadDeadlineMillis() {
                        return maximumReadDeadlineMillis;
                    }

                    @Override
                    public CompletableFuture<Void> release() {
                        if (closed.compareAndSet(false, true)) {
                            released.incrementAndGet();
                        }
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public boolean isReleased() {
                        return closed.get();
                    }
                };
            });
        }

        @Override
        public void close() {
        }

        private AtomicInteger validationAttempts() {
            return validationAttempts;
        }

        private AtomicInteger acquired() {
            return acquired;
        }

        private AtomicInteger released() {
            return released;
        }
    }

    private static final class TrackingActivationGuard
            implements GenerationProtocolActivationGuard {
        private final AtomicInteger revalidations = new AtomicInteger();

        @Override
        public CompletableFuture<GenerationActivationProof> requireReady(
                com.nereusstream.core.capability.GenerationOperation operation,
                com.nereusstream.core.capability.GenerationActivationSubject subject,
                boolean activateLiveProjectionIfAbsent) {
            return CompletableFuture.completedFuture(
                    GenerationActivationProof.create(
                            operation,
                            subject,
                            1,
                            1,
                            1,
                            sha("reference-domains"),
                            true,
                            false,
                            CLOCK.millis()));
        }

        @Override
        public CompletableFuture<Void> revalidate(
                GenerationActivationProof proof) {
            revalidations.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        private AtomicInteger revalidations() {
            return revalidations;
        }
    }

    private record Fixture(
            LocalFileObjectStore objects,
            StagingFileManager staging,
            RecoveryCheckpointWriteResult written,
            GenerationMetadataStore generations,
            FakePhysicalObjectMetadataStore physical,
            DefaultObjectProtectionManager protections,
            CheckpointDerivedIndexRepairer repairer,
            TrackingPinManager pins,
            TrackingActivationGuard activation,
            AtomicLong trimOffset,
            RecoveryCheckpointReferenceRecord reference,
            VersionedRecoveryCheckpointRoot initialRoot,
            GenerationIndexRecord low,
            GenerationIndexRecord high) implements AutoCloseable {
        private GenerationIndexIdentity lowIdentity() {
            return identity(low);
        }

        private GenerationIndexIdentity highIdentity() {
            return identity(high);
        }

        private Optional<com.nereusstream.metadata.oxia.VersionedObjectProtection>
                targetProtection(VersionedGenerationIndex index) {
            ObjectSliceReadTarget target = (ObjectSliceReadTarget)
                    ReadTargetCodecRegistry.phase15().decode(
                            index.value().readTarget());
            PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                    target.objectKey(),
                    Optional.of(target.objectId()),
                    PhysicalObjectKind.COMMITTED_COMPACTED,
                    256,
                    crc("01020304"),
                    Optional.of(sha("content-" + index.value().generation())),
                    Optional.of("etag-" + index.value().generation()));
            ObjectProtectionIdentity protection = new ObjectProtectionIdentity(
                    object.objectKeyHash(),
                    ObjectProtectionType.RECOVERY_CHECKPOINT_TARGET,
                    RecoveryCheckpointProtectionIdentities
                            .checkpointTargetReferenceId(
                                    currentRoot(), index, object));
            return physical.protection(CLUSTER, protection);
        }

        private VersionedRecoveryCheckpointRoot currentRoot() {
            return generations.getRecoveryRoot(CLUSTER, STREAM)
                    .join().orElseThrow();
        }

        private void publishNextRoot() {
            generations.compareAndSetRecoveryRoot(
                    CLUSTER,
                    rootRecord(reference, 2, 3),
                    initialRoot.metadataVersion()).join();
        }

        @Override
        public void close() {
            protections.close();
            physical.close();
            generations.close();
            written.close();
            staging.close();
            objects.close();
        }
    }
}
