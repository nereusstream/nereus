/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.backpressure.MaterializationLagSnapshot;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class DefaultMaterializationLagSnapshotReaderTest {
    private static final Clock CLOCK =
            Clock.fixed(Instant.ofEpochMilli(5_000), ZoneOffset.UTC);

    @Test
    void rebuildsCommittedCoverageAndMeasuresTheExactLiveTail() {
        MaterializationPolicy policy =
                MaterializationPlannerTestSupport.policy();
        GenerationMetadataStore durable =
                GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations =
                MaterializationPlannerTestSupport.generationStore(
                        List.of(MaterializationPlannerTestSupport.higher(
                                "/index/covered-1",
                                0,
                                1,
                                1,
                                0,
                                10,
                                1,
                                policy.digestSha256(),
                                policy.targetPhysicalFormat())),
                        List.of(),
                        durable);
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        try {
            MaterializationLagSnapshot snapshot = reader(
                            policy, generations, scheduler)
                    .measure(STREAM, Duration.ofSeconds(5))
                    .join();

            assertThat(snapshot.verifiedCoveredOffset()).isOne();
            assertThat(snapshot.committedEndOffset()).isEqualTo(2);
            assertThat(snapshot.lagRecords()).isOne();
            assertThat(snapshot.lagBytes()).isEqualTo(20);
            assertThat(snapshot.oldestLagMillis()).isEqualTo(3_000);
            assertThat(snapshot.observedHeadMetadataVersion())
                    .isEqualTo(5);
            assertThat(snapshot.observedAtMillis()).isEqualTo(5_000);
        } finally {
            scheduler.shutdownNow();
            generations.close();
        }
    }

    @Test
    void measuresBookKeeperAsyncObjectWithTheSharedLagAuthority() {
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        GenerationMetadataStore durable =
                GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations =
                MaterializationPlannerTestSupport.generationStore(
                        List.of(),
                        List.of(),
                        durable,
                        StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT);
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        try {
            MaterializationLagSnapshot snapshot = new DefaultMaterializationLagSnapshotReader(
                            CLUSTER,
                            l0(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT),
                            generations,
                            policy,
                            16,
                            16,
                            scheduler,
                            CLOCK)
                    .measure(STREAM, Duration.ofSeconds(5))
                    .join();

            assertThat(snapshot.committedEndOffset()).isEqualTo(2);
            assertThat(snapshot.lagRecords()).isEqualTo(2);
            assertThat(snapshot.lagBytes()).isEqualTo(30);
        } finally {
            scheduler.shutdownNow();
            generations.close();
        }
    }

    @Test
    void rejectsAnAdvisoryCheckpointAheadOfVerifiedCoverage() {
        MaterializationPolicy policy =
                MaterializationPlannerTestSupport.policy();
        GenerationMetadataStore durable =
                GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        var bootstrap = durable.getOrCreateMaterializationCheckpoint(
                        CLUSTER,
                        STREAM,
                        policy.policyId(),
                        policy.policyVersion(),
                        policy.digestSha256())
                .join();
        MaterializationCheckpointRecord value = bootstrap.value();
        durable.compareAndSetMaterializationCheckpoint(
                        CLUSTER,
                        new MaterializationCheckpointRecord(
                                value.schemaVersion(),
                                value.streamId(),
                                value.policyId(),
                                value.policyVersion(),
                                value.policySha256(),
                                2,
                                2,
                                2,
                                "ahead-task",
                                value.updatedAtMillis(),
                                0),
                        bootstrap.metadataVersion())
                .join();
        GenerationMetadataStore generations =
                MaterializationPlannerTestSupport.generationStore(
                        List.of(), List.of(), durable);
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        try {
            assertThatThrownBy(() -> reader(
                                    policy, generations, scheduler)
                            .measure(STREAM, Duration.ofSeconds(5))
                            .join())
                    .hasRootCauseMessage(
                            "materialization checkpoint is ahead of verified committed coverage");
        } finally {
            scheduler.shutdownNow();
            generations.close();
        }
    }

    @Test
    void ignoresReadObservationTimeWhenRevalidatingStableStreamHead() {
        MaterializationPolicy policy =
                MaterializationPlannerTestSupport.policy();
        GenerationMetadataStore durable =
                GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations =
                MaterializationPlannerTestSupport.generationStore(
                        List.of(), List.of(), durable);
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        AtomicLong observedAt = new AtomicLong(10_000);
        try {
            MaterializationLagSnapshot snapshot = new DefaultMaterializationLagSnapshotReader(
                            CLUSTER,
                            l0(observedAt::incrementAndGet),
                            generations,
                            policy,
                            16,
                            16,
                            scheduler,
                            CLOCK)
                    .measure(STREAM, Duration.ofSeconds(5))
                    .join();

            assertThat(snapshot.committedEndOffset()).isEqualTo(2);
            assertThat(snapshot.observedHeadMetadataVersion())
                    .isEqualTo(5);
            assertThat(observedAt).hasValueGreaterThan(10_001);
        } finally {
            scheduler.shutdownNow();
            generations.close();
        }
    }

    private static DefaultMaterializationLagSnapshotReader reader(
            MaterializationPolicy policy,
            GenerationMetadataStore generations,
            ScheduledExecutorService scheduler) {
        return new DefaultMaterializationLagSnapshotReader(
                CLUSTER,
                l0(),
                generations,
                policy,
                16,
                16,
                scheduler,
                CLOCK);
    }

    private static OxiaMetadataStore l0() {
        return l0(() -> 1);
    }

    private static OxiaMetadataStore l0(StorageProfile profile) {
        return l0(() -> 1, profile);
    }

    private static OxiaMetadataStore l0(
            LongSupplier observedAtMillis) {
        return l0(observedAtMillis, StorageProfile.OBJECT_WAL_SYNC_OBJECT);
    }

    private static OxiaMetadataStore l0(
            LongSupplier observedAtMillis,
            StorageProfile profile) {
        AppendRecoveryAnchor anchor =
                AppendRecoveryAnchor.genesis(STREAM);
        AppendRecoveryHead head =
                new AppendRecoveryHead(
                        STREAM, "commit-2", 2, 30, 2, 5);
        AppendRecoveryTailPage tail =
                new AppendRecoveryTailPage(
                        anchor,
                        head,
                        List.of(
                                commit(
                                        "commit-2",
                                        "commit-1",
                                        1,
                                        2,
                                        30,
                                        20,
                                        2,
                                        2_000),
                                commit(
                                        "commit-1",
                                        "",
                                        0,
                                        1,
                                        10,
                                        10,
                                        1,
                                        1_000)),
                        true,
                        Optional.empty());
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getStreamSnapshot" ->
                        CompletableFuture.completedFuture(
                                snapshot(observedAtMillis.getAsLong(), profile));
                    case "readAppendRecoveryTail" ->
                        CompletableFuture.completedFuture(tail);
                    case "close" -> null;
                    case "toString" -> "materialization-lag-l0";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default ->
                        throw new UnsupportedOperationException(
                                method.getName());
                });
    }

    private static StreamMetadataSnapshot snapshot() {
        return snapshot(1);
    }

    private static StreamMetadataSnapshot snapshot(
            long trimUpdatedAtMillis) {
        return snapshot(
                trimUpdatedAtMillis,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT);
    }

    private static StreamMetadataSnapshot snapshot(
            long trimUpdatedAtMillis,
            StorageProfile profile) {
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/lag-topic",
                        "stream-name-hash",
                        StreamState.ACTIVE.name(),
                        profile.canonical().name(),
                        Map.of(),
                        1,
                        1,
                        5),
                new CommittedEndOffsetRecord(
                        STREAM.value(), 2, 30, 2, 5),
                new TrimRecord(
                        STREAM.value(), 0, "", trimUpdatedAtMillis, 5));
    }

    private static AppendRecoveryCommit commit(
            String commitId,
            String previousCommitId,
            long start,
            long end,
            long cumulativeSize,
            long logicalBytes,
            long commitVersion,
            long preparedAtMillis) {
        StreamCommitTargetRecord value =
                new StreamCommitTargetRecord(
                        STREAM.value(),
                        commitId,
                        previousCommitId,
                        start,
                        end,
                        0,
                        cumulativeSize,
                        commitVersion,
                        "writer",
                        "writer-run",
                        1,
                        "fencing-hash",
                        new ReadTargetRecord(
                                "OBJECT_SLICE",
                                1,
                                "canonical-target-v1",
                                new byte[] {1},
                                ChecksumType.SHA256.name(),
                                "a".repeat(64)),
                        PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                        1,
                        1,
                        logicalBytes,
                        List.of(),
                        MaterializationRecordMapper
                                .projectionIdentity(Optional.of(
                                        MaterializationPlannerTestSupport
                                                .PROJECTION)),
                        1,
                        1,
                        preparedAtMillis,
                        0);
        byte[] bytes = MetadataRecordCodecFactory.encodeEnvelope(
                value, StreamCommitTargetRecord.class);
        Checksum digest = sha256(bytes);
        return new AppendRecoveryCommit(
                "/commit/" + commitId,
                AppendRecoveryCommitEncoding
                        .GENERIC_STREAM_COMMIT_TARGET_V1,
                value,
                commitVersion,
                digest,
                ByteBuffer.wrap(bytes),
                digest);
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(bytes)));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
