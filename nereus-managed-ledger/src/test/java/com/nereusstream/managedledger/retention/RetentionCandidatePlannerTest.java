/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorOwnerSession;
import com.nereusstream.managedledger.cursor.CursorRetentionView;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.RangeRetentionStatsScanPage;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedRangeRetentionStats;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RetentionCandidatePlannerTest {
    private static final String TOPIC =
            "persistent://tenant/ns/phase4-retention";
    private static final StreamId STREAM =
            ManagedLedgerProjectionNames.streamId(TOPIC, 1);
    private static final String OWNER =
            "0123456789abcdef0123456789abcdef";
    private static final Checksum SOURCE_SHA = sha('a');
    private static final long NOW = 10_000;

    private CursorOwnerSession owner;
    private FakeAuthority authority;
    private DefaultRetentionCandidatePlanner planner;

    @BeforeEach
    void setUp() {
        ManagedLedgerProjectionIdentity projection =
                new ManagedLedgerProjectionIdentity(
                        1,
                        1,
                        STREAM.value(),
                        ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 7);
        owner = new CursorOwnerSession(
                new CursorLedgerIdentity(
                        TOPIC,
                        ManagedLedgerProjectionNames.managedLedgerNameHash(TOPIC),
                        projection),
                OWNER);
        authority = new FakeAuthority(
                new RetentionPolicySnapshot(99, 7_000, 150),
                head(0, 30, 300, 7),
                cursor(CursorRetentionView.Lifecycle.ACTIVE, 25, 0, 11));
        authority.addStats(stats(0, 10, 1, 0, 100, 1_000, 1_000, 1));
        authority.addStats(stats(10, 20, 2, 100, 200, 2_000, 2_000, 2));
        authority.addStats(stats(20, 30, 3, 200, 300, 9_500, 9_500, 3));
        planner = new DefaultRetentionCandidatePlanner(
                authority,
                owner,
                new NereusRetentionConfig(
                        128,
                        2,
                        8,
                        Duration.ofSeconds(5),
                        Duration.ofSeconds(5)),
                Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC));
    }

    @Test
    void appliesTimeOrSizeFormulaAtVerifiedRangeBoundaries() {
        RetentionCandidate candidate = planner.plan(
                        STREAM, authority.policy)
                .join()
                .orElseThrow();

        assertThat(candidate.currentTrimOffset()).isZero();
        assertThat(candidate.committedEndOffset()).isEqualTo(30);
        assertThat(candidate.cursorCut()).isEqualTo(25);
        assertThat(candidate.timeCut()).isEqualTo(20);
        assertThat(candidate.sizeCut()).isEqualTo(10);
        assertThat(candidate.candidateTrimOffset()).isEqualTo(20);
        assertThat(candidate.statsTokens())
                .extracting(RetentionStatsToken::key)
                .containsExactly("stats/00010", "stats/00020");
        assertThat(candidate.evidenceSha256().type())
                .isEqualTo(ChecksumType.SHA256);
        assertThat(authority.headReads).isGreaterThanOrEqualTo(4);
        assertThat(authority.cursorReads).isGreaterThanOrEqualTo(4);
        assertThat(authority.policyReads).isGreaterThanOrEqualTo(4);
    }

    @Test
    void usesStrictAgeBoundaryAndStopsAtStaleSource() {
        authority.policy = new RetentionPolicySnapshot(99, 8_000, -1);

        RetentionCandidate strict = planner.plan(
                        STREAM, authority.policy)
                .join()
                .orElseThrow();
        assertThat(strict.timeCut()).isEqualTo(10);

        authority.policy = new RetentionPolicySnapshot(99, 7_000, -1);
        authority.sources.remove("source/00020");
        RetentionCandidate stale = planner.plan(
                        STREAM, authority.policy)
                .join()
                .orElseThrow();
        assertThat(stale.timeCut()).isEqualTo(10);
        assertThat(stale.statsTokens())
                .extracting(RetentionStatsToken::key)
                .containsExactly("stats/00010");
    }

    @Test
    void handlesZeroInfiniteAndPendingPoliciesConservatively() {
        authority.policy = new RetentionPolicySnapshot(99, 0, 0);
        RetentionCandidate zero = planner.plan(
                        STREAM, authority.policy)
                .join()
                .orElseThrow();
        assertThat(zero.candidateTrimOffset()).isEqualTo(25);
        assertThat(zero.timeCut()).isEqualTo(25);
        assertThat(zero.sizeCut()).isEqualTo(25);
        assertThat(zero.statsTokens()).isEmpty();

        authority.policy = new RetentionPolicySnapshot(99, -1, -1);
        assertThat(planner.plan(STREAM, authority.policy).join())
                .isEmpty();

        authority.policy = new RetentionPolicySnapshot(99, 0, 0);
        authority.cursor = cursor(
                CursorRetentionView.Lifecycle.TRIM_PENDING,
                25,
                0,
                12);
        assertThat(planner.plan(STREAM, authority.policy).join())
                .isEmpty();
    }

    @Test
    void finalRevalidationRejectsHeadPolicyOrOwnerDrift() {
        RetentionCandidate candidate = planner.plan(
                        STREAM, authority.policy)
                .join()
                .orElseThrow();
        planner.revalidate(candidate, authority.policy).join();

        authority.head = head(0, 30, 300, 8);
        assertThatThrownBy(() -> planner.revalidate(
                                candidate, authority.policy)
                        .join())
                .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class)
                .rootCause()
                .extracting("code")
                .isEqualTo(com.nereusstream.api.ErrorCode.METADATA_CONDITION_FAILED);

        authority.head = head(0, 30, 300, 7);
        RetentionPolicySnapshot changed =
                new RetentionPolicySnapshot(100, 7_000, 150);
        authority.policy = changed;
        assertThatThrownBy(() -> planner.revalidate(candidate, changed).join())
                .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);
    }

    private void addSource(
            String key,
            long metadataVersion) {
        GenerationIndexRecord record = new GenerationIndexRecord(
                1,
                STREAM.value(),
                ReadView.COMMITTED.wireId(),
                0,
                30,
                1,
                F4MetadataTestValues.PUBLICATION,
                "retention-source-task",
                GenerationLifecycle.COMMITTED,
                F4MetadataTestValues.HASH_B,
                F4MetadataTestValues.HASH_C,
                F4MetadataTestValues.readTarget(),
                F4MetadataTestValues.readTarget().identityChecksumValue(),
                F4MetadataTestValues.HASH_C,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                30,
                30,
                3,
                300,
                0,
                300,
                1,
                3,
                List.of(),
                "",
                100,
                200,
                "",
                200,
                metadataVersion);
        authority.sources.put(
                key,
                new VersionedGenerationIndex(
                        key, record, metadataVersion, SOURCE_SHA));
    }

    private VersionedRangeRetentionStats stats(
            long start,
            long end,
            long commitVersion,
            long cumulativeStart,
            long cumulativeEnd,
            long minPublishTime,
            long maxPublishTime,
            long metadataVersion) {
        String suffix = String.format("%05d", end);
        String sourceKey = "source/" + suffix;
        addSource(sourceKey, 5);
        RangeRetentionStatsRecord record = new RangeRetentionStatsRecord(
                1,
                STREAM.value(),
                start,
                end,
                commitVersion,
                cumulativeStart,
                cumulativeEnd,
                minPublishTime,
                maxPublishTime,
                sourceKey,
                SOURCE_SHA.value(),
                5,
                "retention-test-build",
                9_000,
                metadataVersion);
        return new VersionedRangeRetentionStats(
                "stats/" + suffix,
                record,
                metadataVersion,
                sha('b'));
    }

    private static StreamMetadataSnapshot head(
            long trim,
            long end,
            long cumulative,
            long metadataVersion) {
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "phase4-retention-stream",
                        "stream-name-hash",
                        "ACTIVE",
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        3,
                        metadataVersion),
                new CommittedEndOffsetRecord(
                        STREAM.value(),
                        end,
                        cumulative,
                        3,
                        metadataVersion),
                new TrimRecord(
                        STREAM.value(),
                        trim,
                        "retention-test",
                        1,
                        metadataVersion));
    }

    private CursorRetentionView cursor(
            CursorRetentionView.Lifecycle lifecycle,
            long floor,
            long completedTrim,
            long metadataVersion) {
        Optional<CursorRetentionView.PendingTrim> pendingTrim =
                lifecycle == CursorRetentionView.Lifecycle.TRIM_PENDING
                        ? Optional.of(new CursorRetentionView.PendingTrim(
                                "abcdef0123456789abcdef0123456789",
                                floor,
                                "nereus-cursor-retention/abcdef0123456789abcdef0123456789:test"))
                        : Optional.empty();
        return new CursorRetentionView(
                owner.ledger(),
                owner.ownerSessionId(),
                lifecycle,
                1,
                metadataVersion,
                floor,
                completedTrim,
                Optional.empty(),
                pendingTrim);
    }

    private static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256,
                Character.toString(value).repeat(64));
    }

    private static final class FakeAuthority
            implements DefaultRetentionCandidatePlanner.PlanningAuthority {
        private RetentionPolicySnapshot policy;
        private StreamMetadataSnapshot head;
        private CursorRetentionView cursor;
        private final List<VersionedRangeRetentionStats> stats =
                new ArrayList<>();
        private final Map<String, VersionedGenerationCandidate> sources =
                new HashMap<>();
        private int policyReads;
        private int headReads;
        private int cursorReads;

        private FakeAuthority(
                RetentionPolicySnapshot policy,
                StreamMetadataSnapshot head,
                CursorRetentionView cursor) {
            this.policy = policy;
            this.head = head;
            this.cursor = cursor;
        }

        private void addStats(VersionedRangeRetentionStats value) {
            stats.add(value);
            stats.sort(java.util.Comparator.comparing(
                    VersionedRangeRetentionStats::key));
        }

        @Override
        public CompletableFuture<RetentionPolicySnapshot> policy(
                StreamId streamId) {
            policyReads++;
            return CompletableFuture.completedFuture(policy);
        }

        @Override
        public CompletableFuture<StreamMetadataSnapshot> head(
                StreamId streamId) {
            headReads++;
            return CompletableFuture.completedFuture(head);
        }

        @Override
        public CompletableFuture<CursorRetentionView> cursor(
                CursorOwnerSession owner) {
            cursorReads++;
            return CompletableFuture.completedFuture(cursor);
        }

        @Override
        public CompletableFuture<RangeRetentionStatsScanPage> scanStats(
                StreamId streamId,
                long minOffsetEndInclusive,
                long maxOffsetEndInclusive,
                Optional<F4ScanToken> continuation,
                int limit) {
            List<VersionedRangeRetentionStats> selected = stats.stream()
                    .filter(value -> value.value().offsetEnd()
                                    >= minOffsetEndInclusive
                            && value.value().offsetEnd()
                                    <= maxOffsetEndInclusive)
                    .toList();
            if (selected.size() > limit) {
                throw new AssertionError(
                        "test authority requires one-page scans");
            }
            return CompletableFuture.completedFuture(
                    new RangeRetentionStatsScanPage(
                            selected, Optional.empty()));
        }

        @Override
        public CompletableFuture<Optional<VersionedGenerationCandidate>>
                sourceIndex(StreamId streamId, String key) {
            return CompletableFuture.completedFuture(
                    Optional.ofNullable(sources.get(key)));
        }
    }
}
