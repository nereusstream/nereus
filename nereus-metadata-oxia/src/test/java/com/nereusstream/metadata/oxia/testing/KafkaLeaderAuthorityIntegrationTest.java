/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.AppendSessionRequest;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.metadata.oxia.AppendAuthoritySessionTransitions;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class KafkaLeaderAuthorityIntegrationTest {
    @Test
    void higherKafkaTermsPreemptLiveSessionsAndLegacyCannotWaitOutLease() {
        AtomicLong clock = new AtomicLong(1_000);
        FakeOxiaMetadataStore store = new FakeOxiaMetadataStore(clock::get);
        StreamId streamId = new StreamId(store.createOrGetStream(
                "nereus",
                new StreamName("kafka/kraft/topic/3/incarnation-1"),
                new StreamCreateOptions(
                        StorageProfile.OBJECT_WAL,
                        Map.of(AppendAuthoritySessionTransitions.AUTHORITY_MODE_ATTRIBUTE,
                                AppendAuthoritySessionTransitions.EXTERNAL_MONOTONIC_TERM_V1)))
                .join().streamId());

        AppendSessionRecord first = store.acquireAppendSession(
                "nereus", streamId, request("writer-a", 7, "1", 11)).join();
        AppendSession oldPublicSession = publicSession(streamId, first);
        AppendSessionRecord takeover = store.acquireAppendSession(
                "nereus", streamId, request("writer-b", 8, "2", 3)).join();

        assertThat(takeover.epoch()).isEqualTo(first.epoch() + 1);
        assertThat(takeover.fencingToken()).isNotEqualTo(first.fencingToken());
        assertThat(takeover.authority()).contains(authority(8, "2", 3));
        assertFailure(() -> store.revalidateAppendSession("nereus", oldPublicSession).join(), ErrorCode.FENCED_APPEND);
        StableStreamHeadSnapshot beforeRenew =
                store.getStableStreamHeadSnapshot("nereus", streamId).join();
        assertThat(beforeRenew.commitVersion()).isZero();
        assertThat(beforeRenew.lastCommitId()).isEmpty();
        assertThat(beforeRenew.appendSession().orElseThrow().authority())
                .contains(authority(8, "2", 3));
        assertThat(beforeRenew.appendSession().orElseThrow().session())
                .isEqualTo(publicSession(streamId, takeover));

        AppendSessionRecord renewed = store.renewAppendSession(
                "nereus", streamId, takeover.writerId(), takeover.epoch(), takeover.fencingToken(),
                Duration.ofSeconds(2)).join();
        assertThat(renewed.authority()).isEqualTo(takeover.authority());
        StableStreamHeadSnapshot afterRenew =
                store.getStableStreamHeadSnapshot("nereus", streamId).join();
        assertThat(afterRenew.durableHeadSha256()).isNotEqualTo(beforeRenew.durableHeadSha256());
        assertThat(afterRenew.metadataVersion()).isGreaterThan(beforeRenew.metadataVersion());

        clock.set(renewed.expiresAtMillis() + 1);
        assertFailure(() -> store.acquireAppendSession(
                "nereus", streamId,
                new AppendSessionOptions("legacy", Duration.ofSeconds(1), true)).join(),
                ErrorCode.FENCED_APPEND);
    }

    @Test
    void sameBrokerRestartTermPreemptsButDifferentBrokerCannotReuseLeaderEpoch() {
        AtomicLong clock = new AtomicLong(1_000);
        FakeOxiaMetadataStore store = new FakeOxiaMetadataStore(clock::get);
        StreamId streamId = createKafkaStream(store);
        AppendSessionRecord first = store.acquireAppendSession(
                "nereus", streamId, request("run-a", 10, "4", 20)).join();

        AppendSessionRecord restarted = store.acquireAppendSession(
                "nereus", streamId, request("run-b", 10, "4", 21)).join();
        assertThat(restarted.epoch()).isEqualTo(first.epoch() + 1);
        assertFailure(() -> store.acquireAppendSession(
                "nereus", streamId, request("run-c", 10, "5", 100)).join(), ErrorCode.FENCED_APPEND);
    }

    private static StreamId createKafkaStream(FakeOxiaMetadataStore store) {
        return new StreamId(store.createOrGetStream(
                "nereus",
                new StreamName("kafka/kraft/topic/4/incarnation-1"),
                new StreamCreateOptions(
                        StorageProfile.OBJECT_WAL,
                        Map.of(AppendAuthoritySessionTransitions.AUTHORITY_MODE_ATTRIBUTE,
                                AppendAuthoritySessionTransitions.EXTERNAL_MONOTONIC_TERM_V1)))
                .join().streamId());
    }

    private static AppendSessionRequest request(
            String writerId, long leaderEpoch, String ownerId, long ownerEpoch) {
        return AppendSessionRequest.authoritative(
                new AppendSessionOptions(writerId, Duration.ofSeconds(1), false),
                authority(leaderEpoch, ownerId, ownerEpoch));
    }

    private static AppendAuthority authority(long leaderEpoch, String ownerId, long ownerEpoch) {
        return new AppendAuthority(
                "kafka-partition-leader-v1", "kraft/topic/3", leaderEpoch, ownerId, ownerEpoch);
    }

    private static AppendSession publicSession(StreamId streamId, AppendSessionRecord record) {
        return new AppendSession(
                streamId, record.writerId(), record.epoch(), record.fencingToken(),
                record.leaseVersion(), record.expiresAtMillis());
    }

    private static void assertFailure(Runnable action, ErrorCode code) {
        assertThatThrownBy(action::run)
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOfSatisfying(NereusException.class,
                        failure -> assertThat(failure.code()).isEqualTo(code));
    }
}
