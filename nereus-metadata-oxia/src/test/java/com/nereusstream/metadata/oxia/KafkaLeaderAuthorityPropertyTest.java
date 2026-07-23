/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord;
import com.nereusstream.metadata.oxia.records.StreamHeadRecord;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaLeaderAuthorityPropertyTest {
    private static final AppendSessionOptions WRITER =
            new AppendSessionOptions("writer-a", Duration.ofSeconds(10), false);

    @Test
    void exactAuthorityAndWriterRenewsWithoutChangingSessionIdentity() {
        StreamHeadRecord head = head(session("writer-a", 4, 8, "owner-a", 9));

        AppendSessionSnapshotRecord renewed = AppendAuthoritySessionTransitions.acquire(
                head, WRITER, authority(8, "owner-a", 9), 1_500, 20_000, ignored -> "new-token");

        assertThat(renewed.epoch()).isEqualTo(4);
        assertThat(renewed.fencingToken()).isEqualTo("token");
        assertThat(renewed.leaseVersion()).isEqualTo(4);
        assertThat(renewed.authorityEpoch()).isEqualTo(8);
    }

    @Test
    void higherLeaderEpochPreemptsImmediatelyAndLowerEpochIsFenced() {
        StreamHeadRecord head = head(session("writer-a", 4, 8, "owner-a", 9));

        AppendSessionSnapshotRecord replacement = AppendAuthoritySessionTransitions.acquire(
                head,
                new AppendSessionOptions("writer-b", Duration.ofSeconds(10), false),
                authority(9, "owner-b", 1),
                1_500,
                20_000,
                epoch -> "token-" + epoch);

        assertThat(replacement.epoch()).isEqualTo(5);
        assertThat(replacement.fencingToken()).isEqualTo("token-5");
        assertThat(replacement.writerId()).isEqualTo("writer-b");
        assertNereusCode(
                () -> AppendAuthoritySessionTransitions.acquire(
                        head, WRITER, authority(7, "owner-a", 10), 1_500, 20_000, ignored -> "unused"),
                ErrorCode.FENCED_APPEND);
    }

    @Test
    void newerOwnerEpochOnlyPreemptsTheSameOwner() {
        StreamHeadRecord head = head(session("writer-a", 4, 8, "owner-a", 9));

        AppendSessionSnapshotRecord replacement = AppendAuthoritySessionTransitions.acquire(
                head, WRITER, authority(8, "owner-a", 10), 1_500, 20_000, ignored -> "restart-token");

        assertThat(replacement.epoch()).isEqualTo(5);
        assertThat(replacement.authorityOwnerEpoch()).isEqualTo(10);
        assertNereusCode(
                () -> AppendAuthoritySessionTransitions.acquire(
                        head, WRITER, authority(8, "owner-b", 100), 1_500, 20_000, ignored -> "unused"),
                ErrorCode.FENCED_APPEND);
    }

    @Test
    void mismatchedAuthorityIdentityIsInvariantViolation() {
        StreamHeadRecord head = head(session("writer-a", 4, 8, "owner-a", 9));
        AppendAuthority wrong = new AppendAuthority(
                "kafka-partition-leader-v1", "cluster/other-topic/3", 9, "owner-a", 10);

        assertNereusCode(
                () -> AppendAuthoritySessionTransitions.acquire(
                        head, WRITER, wrong, 1_500, 20_000, ignored -> "unused"),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
    }

    private static AppendSessionSnapshotRecord session(
            String writerId, long sessionEpoch, long authorityEpoch, String ownerId, long ownerEpoch) {
        return new AppendSessionSnapshotRecord(
                writerId, sessionEpoch, "token", 3, 10_000,
                "kafka-partition-leader-v1", "cluster/topic/3", authorityEpoch, ownerId, ownerEpoch);
    }

    private static AppendAuthority authority(long leaderEpoch, String ownerId, long ownerEpoch) {
        return new AppendAuthority(
                "kafka-partition-leader-v1", "cluster/topic/3", leaderEpoch, ownerId, ownerEpoch);
    }

    private static StreamHeadRecord head(AppendSessionSnapshotRecord session) {
        return new StreamHeadRecord(
                "stream", "kafka/cluster/topic/3/incarnation-1", "hash", "ACTIVE",
                "OBJECT_WAL_SYNC_OBJECT",
                Map.of(AppendAuthoritySessionTransitions.AUTHORITY_MODE_ATTRIBUTE,
                        AppendAuthoritySessionTransitions.EXTERNAL_MONOTONIC_TERM_V1),
                1_000, 1, 0, 0, 0, 0, "", session, 1);
    }

    private static void assertNereusCode(Runnable action, ErrorCode code) {
        assertThatThrownBy(action::run)
                .isInstanceOfSatisfying(NereusException.class,
                        failure -> assertThat(failure.code()).isEqualTo(code));
    }
}
