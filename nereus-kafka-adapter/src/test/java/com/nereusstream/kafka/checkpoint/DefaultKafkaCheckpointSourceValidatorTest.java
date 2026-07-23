/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCommitAnchor;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultKafkaCheckpointSourceValidatorTest {
    private static final StreamId STREAM = new StreamId("stream-1");
    private static final AppendAuthority AUTHORITY =
            new AppendAuthority("kafka-partition-leader-v1", "cluster/topic/0", 7, "1", 9);
    private static final Checksum HEAD_SHA =
            new Checksum(ChecksumType.SHA256, "11".repeat(32));

    @Test
    void loadsExactAuthorityHeadAndDelegatesReachabilityFromTheSameObservation() {
        AcquiredAppendSession acquired = acquired("token", 3);
        AtomicReference<StableStreamHeadSnapshot> head = new AtomicReference<>(head(acquired, HEAD_SHA));
        AtomicReference<StreamCommitAnchor> descendant = new AtomicReference<>();
        StreamStorage streams = streams(head, descendant, true);
        DefaultKafkaCheckpointSourceValidator validator =
                new DefaultKafkaCheckpointSourceValidator(streams, STREAM, acquired);

        KafkaCheckpointSourceState current = validator.loadCurrent().join();
        boolean reachable = validator.isSourceCommitReachable(header(STREAM), current).join();

        assertThat(current.authority()).isEqualTo(AUTHORITY);
        assertThat(current.writerId()).isEqualTo("writer");
        assertThat(current.endOffset()).isEqualTo(4);
        assertThat(current.commitVersion()).isEqualTo(2);
        assertThat(current.headSha256()).isEqualTo(HEAD_SHA);
        assertThat(reachable).isTrue();
        assertThat(descendant).hasValue(head.get().commitAnchor());
    }

    @Test
    void failsClosedForHeadChangeSessionLossAndForeignCheckpoint() {
        AcquiredAppendSession acquired = acquired("token", 3);
        AtomicReference<StableStreamHeadSnapshot> head = new AtomicReference<>(head(acquired, HEAD_SHA));
        StreamStorage streams = streams(head, new AtomicReference<>(), false);
        DefaultKafkaCheckpointSourceValidator validator =
                new DefaultKafkaCheckpointSourceValidator(streams, STREAM, acquired);
        KafkaCheckpointSourceState frozen = validator.loadCurrent().join();

        head.set(head(acquired, new Checksum(ChecksumType.SHA256, "22".repeat(32))));
        assertFailure(() -> validator.isSourceCommitReachable(header(STREAM), frozen).join(), ErrorCode.FENCED_APPEND);

        head.set(head(new AcquiredAppendSession(
                new AppendSession(STREAM, "writer", 2, "other-token", 4, 20_000),
                Optional.of(AUTHORITY)), HEAD_SHA));
        assertFailure(() -> validator.loadCurrent().join(), ErrorCode.FENCED_APPEND);

        assertFailure(
                () -> validator.isSourceCommitReachable(header(new StreamId("other")), frozen).join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
    }

    private static StreamStorage streams(
            AtomicReference<StableStreamHeadSnapshot> head,
            AtomicReference<StreamCommitAnchor> descendant,
            boolean reachable) {
        return (StreamStorage) Proxy.newProxyInstance(
                StreamStorage.class.getClassLoader(),
                new Class<?>[] {StreamStorage.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getStableHeadSnapshot" -> CompletableFuture.completedFuture(head.get());
                    case "isCommitReachable" -> {
                        descendant.set((StreamCommitAnchor) arguments[0]);
                        assertThat(arguments[1]).isEqualTo("commit-1");
                        assertThat(arguments[2]).isEqualTo(1L);
                        yield CompletableFuture.completedFuture(reachable);
                    }
                    case "close" -> null;
                    case "toString" -> "checkpoint-source-streams";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static AcquiredAppendSession acquired(String token, long leaseVersion) {
        return new AcquiredAppendSession(
                new AppendSession(STREAM, "writer", 2, token, leaseVersion, 10_000),
                Optional.of(AUTHORITY));
    }

    private static StableStreamHeadSnapshot head(
            AcquiredAppendSession acquired,
            Checksum digest) {
        return new StableStreamHeadSnapshot(
                STREAM,
                StreamState.ACTIVE,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                0,
                4,
                100,
                2,
                "commit-2",
                Optional.of(acquired),
                digest,
                5);
    }

    private static KafkaCheckpointHeader header(StreamId streamId) {
        return new KafkaCheckpointHeader(
                0, "cluster", "EjRWeJq83vAAAAAAAAAAAQ", 0, 1,
                streamId, 1, 7, 2, 0, 2, 1, "commit-1", HEAD_SHA);
    }

    private static void assertFailure(Runnable action, ErrorCode code) {
        assertThatThrownBy(action::run)
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(code);
    }
}
