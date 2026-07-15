/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.recovery.AnchorAwareCommitWalker;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointBuilderTest {
    private static final String CLUSTER = "checkpoint-builder-cluster";
    private static final StreamId STREAM = new StreamId("checkpoint-builder-stream");
    private static final ProjectionRef PROJECTION = new ProjectionRef(
            ProjectionType.VIRTUAL_LEDGER, "persistent://tenant/ns/topic");
    private static final String PROJECTION_IDENTITY = projectionIdentity(PROJECTION);
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(24L * 60 * 60 * 1_000), ZoneOffset.UTC);

    @TempDir
    Path staging;

    @Test
    void buildsCanonicalOldestPrefixFromExactCommittedGeneration() {
        VersionedRecoveryCheckpointRoot root = emptyRoot();
        VersionedMaterializationStreamRegistration registration = registration();
        AppendRecoveryCommit first = commit("commit-1", "", 0, 1, 10, 1);
        AppendRecoveryCommit second = commit("commit-2", "commit-1", 1, 2, 20, 2);
        AppendRecoveryHead head = new AppendRecoveryHead(STREAM, "commit-2", 2, 20, 2, 8);
        AppendRecoveryTailPage tail = new AppendRecoveryTailPage(
                AppendRecoveryAnchor.genesis(STREAM),
                head,
                List.of(second, first),
                true,
                Optional.empty());
        VersionedGenerationIndex generation = generation();
        GenerationMetadataStore generationStore = generationStore(root, List.of(generation));
        OxiaMetadataStore l0Store = l0Store(tail, snapshot());
        RecoveryCheckpointBuilder builder = new RecoveryCheckpointBuilder(
                CLUSTER,
                l0Store,
                generationStore,
                new AnchorAwareCommitWalker(CLUSTER, l0Store, generationStore),
                MaterializationConfig.defaults(staging),
                CLOCK);

        RecoveryCheckpointBuildResult result = builder.build(
                STREAM, root, registration, "a".repeat(26)).join();

        assertThat(result.status()).isEqualTo(RecoveryCheckpointBuildStatus.READY);
        RecoveryCheckpointPlan plan = result.plan().orElseThrow();
        assertThat(plan.writeRequest().coverage()).isEqualTo(new OffsetRange(0, 2));
        assertThat(plan.writeRequest().firstCommitVersion()).isEqualTo(1);
        assertThat(plan.writeRequest().lastCommitVersion()).isEqualTo(2);
        assertThat(plan.entries()).extracting(value -> value.commitId())
                .containsExactly("commit-1", "commit-2");
        assertThat(plan.targets()).hasSize(1);
        assertThat(plan.entries()).allSatisfy(value ->
                assertThat(value.coveringPublicationIndexes()).containsExactly(0));
    }

    @Test
    void refusesRootPublicationWhenNoCommittedGenerationCoversOldCommit() {
        VersionedRecoveryCheckpointRoot root = emptyRoot();
        VersionedMaterializationStreamRegistration registration = registration();
        AppendRecoveryCommit commit = commit("commit-1", "", 0, 1, 10, 1);
        AppendRecoveryTailPage tail = new AppendRecoveryTailPage(
                AppendRecoveryAnchor.genesis(STREAM),
                new AppendRecoveryHead(STREAM, "commit-1", 1, 10, 1, 8),
                List.of(commit),
                true,
                Optional.empty());
        GenerationMetadataStore generationStore = generationStore(root, List.of());
        OxiaMetadataStore l0Store = l0Store(tail, snapshot(1, 10, 1));

        RecoveryCheckpointBuildResult result = new RecoveryCheckpointBuilder(
                CLUSTER,
                l0Store,
                generationStore,
                new AnchorAwareCommitWalker(CLUSTER, l0Store, generationStore),
                MaterializationConfig.defaults(staging),
                CLOCK)
                .build(STREAM, root, registration, "b".repeat(26))
                .join();

        assertThat(result.status()).isEqualTo(RecoveryCheckpointBuildStatus.NO_ELIGIBLE_PREFIX);
        assertThat(result.plan()).isEmpty();
    }

    private static OxiaMetadataStore l0Store(
            AppendRecoveryTailPage tail,
            StreamMetadataSnapshot snapshot) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "readAppendRecoveryTail" -> CompletableFuture.completedFuture(tail);
                    case "getStreamSnapshot" -> CompletableFuture.completedFuture(snapshot);
                    case "close" -> null;
                    case "toString" -> "recovery-builder-l0";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static GenerationMetadataStore generationStore(
            VersionedRecoveryCheckpointRoot root,
            List<VersionedGenerationIndex> candidates) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRecoveryRoot" -> CompletableFuture.completedFuture(Optional.of(root));
                    case "scanIndex" -> CompletableFuture.completedFuture(
                            new GenerationScanPage(List.copyOf(candidates), Optional.empty()));
                    case "close" -> null;
                    case "toString" -> "recovery-builder-generation";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static VersionedRecoveryCheckpointRoot emptyRoot() {
        long version = 3;
        RecoveryCheckpointRootRecord value = new RecoveryCheckpointRootRecord(
                1, STREAM.value(), 0, 0, 0, 0, 0, 0, 0,
                "", "", List.of(), "", "", 0, 0, version);
        return new VersionedRecoveryCheckpointRoot(
                new F4Keyspace(CLUSTER).recoveryRootKey(STREAM), value, version, sha("root"));
    }

    private static VersionedMaterializationStreamRegistration registration() {
        long version = 4;
        MaterializationStreamRegistrationRecord value = new MaterializationStreamRegistrationRecord(
                1,
                STREAM.value(),
                PROJECTION_IDENTITY,
                sha("projection").value(),
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                1,
                2,
                2,
                version);
        return new VersionedMaterializationStreamRegistration(
                new F4Keyspace(CLUSTER).materializationRegistryKey(STREAM),
                value,
                version,
                sha("registration"));
    }

    private static StreamMetadataSnapshot snapshot() {
        return snapshot(2, 20, 2);
    }

    private static StreamMetadataSnapshot snapshot(
            long endOffset,
            long cumulativeSize,
            long commitVersion) {
        long version = 8;
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
                        version),
                new CommittedEndOffsetRecord(
                        STREAM.value(), endOffset, cumulativeSize, commitVersion, version),
                new TrimRecord(STREAM.value(), 0, "", 1, version));
    }

    private static AppendRecoveryCommit commit(
            String id,
            String previous,
            long start,
            long end,
            long cumulative,
            long version) {
        StreamCommitTargetRecord value = new StreamCommitTargetRecord(
                STREAM.value(),
                id,
                previous,
                start,
                end,
                0,
                cumulative,
                version,
                "writer",
                "writer-run",
                1,
                "fencing-hash",
                ReadTargetCodecRegistry.phase15().encode(objectTarget("wal-object")),
                "OPAQUE_RECORD_BATCH",
                1,
                1,
                10,
                List.of(),
                PROJECTION_IDENTITY,
                1,
                1,
                1,
                0);
        byte[] bytes = MetadataRecordCodecFactory.encodeEnvelope(
                value, StreamCommitTargetRecord.class);
        Checksum digest = sha(bytes);
        return new AppendRecoveryCommit(
                "/commit/" + id,
                AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1,
                value,
                version,
                digest,
                ByteBuffer.wrap(bytes),
                digest);
    }

    private static VersionedGenerationIndex generation() {
        long metadataVersion = 10;
        GenerationIndexRecord canonical = new GenerationIndexRecord(
                1,
                STREAM.value(),
                ReadView.COMMITTED.wireId(),
                0,
                2,
                1,
                new PublicationId("c".repeat(26)).value(),
                "task-1",
                GenerationLifecycle.COMMITTED,
                sha("source-set").value(),
                sha("policy").value(),
                ReadTargetCodecRegistry.phase15().encode(objectTarget("compacted-object")),
                sha("target").value(),
                sha("policy").value(),
                "OPAQUE_RECORD_BATCH",
                2,
                2,
                2,
                20,
                0,
                20,
                1,
                2,
                List.of(),
                PROJECTION_IDENTITY,
                1,
                2,
                "",
                2,
                0);
        Checksum digest = sha(new GenerationIndexRecordCodecV1().encode(canonical));
        return new VersionedGenerationIndex(
                new F4Keyspace(CLUSTER).generationIndexKey(
                        STREAM, ReadView.COMMITTED, 2, 1),
                canonical.withMetadataVersion(metadataVersion),
                metadataVersion,
                digest);
    }

    private static ObjectSliceReadTarget objectTarget(String suffix) {
        ObjectId objectId = new ObjectId(suffix + "-id");
        ObjectKey objectKey = new ObjectKey("objects/" + suffix);
        return new ObjectSliceReadTarget(
                1,
                objectId,
                objectKey,
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                suffix + "-slice",
                0,
                128,
                crc("01020304"),
                new EntryIndexRef(
                        EntryIndexLocation.OBJECT_FOOTER,
                        Optional.of(objectId),
                        Optional.of(objectKey),
                        Optional.empty(),
                        64,
                        16,
                        crc("05060708")));
    }

    private static String projectionIdentity(ProjectionRef value) {
        return encoded("projectionRef")
                + encoded("present")
                + encoded(value.type().name())
                + encoded(value.value());
    }

    private static String encoded(String value) {
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
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
