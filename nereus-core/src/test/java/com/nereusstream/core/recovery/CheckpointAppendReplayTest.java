/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.AppendReplayStatus;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.RecoveryCheckpointRootDigests;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointAppendReplayTest {
    private static final String CLUSTER = "checkpoint-replay-cluster";
    private static final StreamId STREAM = new StreamId("checkpoint-replay-stream");
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(10_000), ZoneOffset.UTC);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @TempDir
    Path temporaryDirectory;

    @Test
    void findsExactAppendAfterLiveCommitKeyRetirementAndReleasesReadPin()
            throws Exception {
        CommitAppendRequest request = request("writer");
        try (Fixture fixture = fixture(request, false)) {
            AppendReplayResolution result = fixture.reader().search(
                    request, 16, 4, TIMEOUT).join();

            assertThat(result.status()).isEqualTo(AppendReplayStatus.FOUND);
            assertThat(result.evidenceSource())
                    .contains(AppendReplayEvidenceSource.RECOVERY_CHECKPOINT);
            assertThat(result.committedAppend().orElseThrow().committedAppend().commitId())
                    .isEqualTo(request.commitId());
            assertThat(result.scannedLiveCommits()).isZero();
            assertThat(fixture.pins().acquired()).hasValue(1);
            assertThat(fixture.pins().released()).hasValue(1);
        }
    }

    @Test
    void restartsWhenRootChangesDuringPinAndNeverAliasesAnotherCommit()
            throws Exception {
        CommitAppendRequest committed = request("writer");
        try (Fixture fixture = fixture(committed, true)) {
            AppendReplayResolution found = fixture.reader().search(
                    committed, 16, 4, TIMEOUT).join();
            AppendReplayResolution different = fixture.reader().search(
                    request("different-writer"), 16, 4, TIMEOUT).join();

            assertThat(found.status()).isEqualTo(AppendReplayStatus.FOUND);
            assertThat(different.status())
                    .isEqualTo(AppendReplayStatus.PROVEN_NOT_COMMITTED);
            assertThat(fixture.rootReads()).hasValueGreaterThanOrEqualTo(7);
            assertThat(fixture.pins().released())
                    .hasValue(fixture.pins().acquired().get());
        }
    }

    private Fixture fixture(
            CommitAppendRequest request,
            boolean changeRootDuringFirstPin) throws Exception {
        LocalFileObjectStore objects = new LocalFileObjectStore(
                temporaryDirectory.resolve("objects-" + request.writerId()));
        StagingFileManager staging = staging(
                temporaryDirectory.resolve("staging-" + request.writerId()));
        DefaultRecoveryCheckpointCodecV1 codec = new DefaultRecoveryCheckpointCodecV1(
                objects, staging, Runnable::run, verifier());
        StreamCommitTargetRecord commit = commit(request);
        byte[] canonical = MetadataRecordCodecFactory.encodeEnvelope(
                commit, StreamCommitTargetRecord.class);
        RecoveryCheckpointWriteRequest header = new RecoveryCheckpointWriteRequest(
                CLUSTER,
                STREAM,
                1,
                "a".repeat(26),
                new OffsetRange(0, 1),
                1,
                1,
                0,
                request.logicalBytes(),
                request.commitId(),
                request.commitId(),
                request.commitId(),
                1,
                sha("projection"),
                1,
                1);
        RecoveryCheckpointPublication publication = new RecoveryCheckpointPublication(
                1,
                new PublicationId("b".repeat(26)),
                new OffsetRange(0, 1),
                ByteBuffer.wrap("generation".getBytes(StandardCharsets.UTF_8)),
                sha("generation"));
        RecoveryCheckpointEntry entry = new RecoveryCheckpointEntry(
                1,
                new OffsetRange(0, 1),
                request.logicalBytes(),
                request.commitId(),
                "",
                ByteBuffer.wrap(canonical),
                sha(canonical),
                List.of(0));
        RecoveryCheckpointWriteResult written = codec.write(
                        header,
                        publisher(List.of(publication)),
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

        RecoveryCheckpointReferenceRecord reference = reference(header, written);
        VersionedRecoveryCheckpointRoot first = root(reference, 10, "root-10");
        VersionedRecoveryCheckpointRoot second = root(reference, 11, "root-11");
        AtomicInteger rootReads = new AtomicInteger();
        GenerationMetadataStore generations = generationStore(
                first, second, rootReads, changeRootDuringFirstPin);
        OxiaMetadataStore l0 = l0Store();
        TrackingPinManager pins = new TrackingPinManager();
        CheckpointAppendReplayReader reader = new CheckpointAppendReplayReader(
                CLUSTER,
                generations,
                new AnchorAwareCommitWalker(CLUSTER, l0, generations),
                codec,
                pins,
                CLOCK);
        return new Fixture(
                objects, staging, written, reader, pins, rootReads);
    }

    private static OxiaMetadataStore l0Store() {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
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
                    case "toString" -> "checkpoint-replay-l0";
                    default -> throw new UnsupportedOperationException(
                            method.getName());
                });
    }

    private static GenerationMetadataStore generationStore(
            VersionedRecoveryCheckpointRoot first,
            VersionedRecoveryCheckpointRoot second,
            AtomicInteger reads,
            boolean changeRoot) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRecoveryRoot" -> {
                        int read = reads.incrementAndGet();
                        VersionedRecoveryCheckpointRoot value =
                                changeRoot && read >= 3 ? second : first;
                        yield CompletableFuture.completedFuture(
                                Optional.of(value));
                    }
                    case "close" -> null;
                    case "toString" -> "checkpoint-replay-generation";
                    default -> throw new UnsupportedOperationException(
                            method.getName());
                });
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

    private static VersionedRecoveryCheckpointRoot root(
            RecoveryCheckpointReferenceRecord reference,
            long metadataVersion,
            String digestSeed) {
        RecoveryCheckpointRootRecord value = new RecoveryCheckpointRootRecord(
                1,
                STREAM.value(),
                reference.checkpointSequence(),
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
                2,
                metadataVersion);
        return new VersionedRecoveryCheckpointRoot(
                new F4Keyspace(CLUSTER).recoveryRootKey(STREAM),
                value,
                metadataVersion,
                sha(digestSeed));
    }

    private static CommitAppendRequest request(String writer) {
        ObjectId objectId = new ObjectId("object-id");
        ObjectKey objectKey = new ObjectKey("objects/wal");
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.of(objectId),
                Optional.of(objectKey),
                Optional.empty(),
                64,
                16,
                crc("05060708"));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                objectId,
                objectKey,
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "slice-1",
                0,
                128,
                crc("01020304"),
                index);
        return new CommitAppendRequest(
                STREAM,
                writer,
                "writer-run",
                1,
                "fencing-token",
                0,
                target,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                7,
                List.of(),
                1,
                1,
                Optional.empty());
    }

    private static StreamCommitTargetRecord commit(
            CommitAppendRequest request) {
        return new StreamCommitTargetRecord(
                STREAM.value(),
                request.commitId(),
                "",
                0,
                1,
                0,
                request.logicalBytes(),
                1,
                request.writerId(),
                request.writerRunIdHash(),
                request.epoch(),
                request.fencingTokenHash(),
                request.readTargetRecord(),
                request.payloadFormat().name(),
                request.recordCount(),
                request.entryCount(),
                request.logicalBytes(),
                request.schemaRefs(),
                request.projectionIdentity(),
                request.minEventTimeMillis(),
                request.maxEventTimeMillis(),
                1,
                0);
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
                MetadataRecordCodecFactory.decodeEnvelope(
                        bytes(entry.canonicalCommitRecord()),
                        StreamCommitTargetRecord.class);
            }
        };
    }

    private static StagingFileManager staging(Path directory) throws Exception {
        Files.createDirectory(directory);
        Files.setPosixFilePermissions(
                directory, PosixFilePermissions.fromString("rwx------"));
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

    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
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
                            MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static final class TrackingPinManager
            implements ObjectReadPinManager {
        private final AtomicInteger acquired = new AtomicInteger();
        private final AtomicInteger released = new AtomicInteger();

        @Override
        public CompletableFuture<ObjectReadLease> acquire(
                PhysicalObjectIdentity object,
                long maximumReadDeadlineMillis,
                SelectionRevalidator selectionRevalidator) {
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
                        return "lease";
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

        private AtomicInteger acquired() {
            return acquired;
        }

        private AtomicInteger released() {
            return released;
        }
    }

    private record Fixture(
            LocalFileObjectStore objects,
            StagingFileManager staging,
            RecoveryCheckpointWriteResult written,
            CheckpointAppendReplayReader reader,
            TrackingPinManager pins,
            AtomicInteger rootReads) implements AutoCloseable {
        @Override
        public void close() {
            written.close();
            staging.close();
            objects.close();
        }
    }
}
