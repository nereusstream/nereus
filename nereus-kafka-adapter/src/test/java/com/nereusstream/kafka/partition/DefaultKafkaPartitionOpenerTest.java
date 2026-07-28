/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionRequest;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.kafka.codec.KafkaAppendBatchEncoder;
import com.nereusstream.kafka.codec.KafkaFetchAssembler;
import com.nereusstream.kafka.codec.KafkaRecordBatchCodec;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaRecoveredPartition;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultKafkaPartitionOpenerTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);
    private static final Checksum HEAD_SHA =
            new Checksum(ChecksumType.SHA256, "33".repeat(32));

    @Test
    void composesAuthorityHeadRecoveryAndWritableStorageUnderOneDeadline() {
        KafkaLeaderAuthority authority = new KafkaLeaderAuthority(
                KafkaPartitionStorageTestSupport.identity(), 1, 5, 9);
        KafkaPartitionOpenPlan plan = KafkaPartitionStorageTestSupport.openPlan(authority);
        AcquiredAppendSession acquired = new AcquiredAppendSession(
                new AppendSession(plan.binding().streamId(), "broker-run", 1, "token", 1, 40_000),
                Optional.of(authority.appendAuthority()));
        AtomicReference<AppendSessionRequest> sessionRequest = new AtomicReference<>();
        StreamStorage streams = streams(acquired, head(plan, acquired), sessionRequest);
        AtomicReference<KafkaPartitionRecoveryRequest> recoveryRequest = new AtomicReference<>();
        DefaultKafkaPartitionOpener opener = opener(streams, request -> {
            recoveryRequest.set(request);
            var source = request.checkpointRequest().currentSource();
            return CompletableFuture.completedFuture(new KafkaRecoveredPartition<>(
                    new Object(), source, 0, 0, 0, Optional.empty()));
        });

        KafkaPartitionStorage storage = opener.open(plan).join();

        assertThat(sessionRequest.get().authority()).contains(authority.appendAuthority());
        assertThat(sessionRequest.get().options().writerId()).isEqualTo("broker-run");
        assertThat(recoveryRequest.get().checkpointRequest().binding()).isEqualTo(plan.binding().durableRoot());
        assertThat(recoveryRequest.get().checkpointRequest().currentSource().commitVersion()).isZero();
        assertThat(recoveryRequest.get().timeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(storage.identity()).isEqualTo(authority.identity());
        assertThat(storage.leaderEpoch()).isEqualTo(5);
        assertThat(storage.storageProfile()).isEqualTo(plan.profilePolicy().storageProfile());
        assertThat(storage.state()).isEqualTo(KafkaPartitionState.LEADER_WRITABLE);
        assertThat(storage.stableSnapshot()).isEqualTo(KafkaStableSnapshot.nonTransactional(0, 0, 0));
        storage.resign().join();
    }

    @Test
    void publishesExactLeaderAndStableHeadObservationAfterRecovery() {
        KafkaLeaderAuthority authority = new KafkaLeaderAuthority(
                KafkaPartitionStorageTestSupport.identity(), 1, 5, 9);
        KafkaPartitionOpenPlan plan = KafkaPartitionStorageTestSupport.openPlan(authority);
        AcquiredAppendSession acquired = new AcquiredAppendSession(
                new AppendSession(plan.binding().streamId(), "broker-run", 1, "token", 1, 40_000),
                Optional.of(authority.appendAuthority()));
        AtomicReference<VersionedKafkaPartitionBinding> durable =
                new AtomicReference<>(plan.binding().durableRoot());
        AtomicReference<KafkaPartitionRecoveryRequest> recoveryRequest = new AtomicReference<>();
        AtomicBoolean recoveryCompleted = new AtomicBoolean();
        DefaultKafkaPartitionOpener opener = opener(
                streams(acquired, head(plan, acquired), new AtomicReference<>()),
                request -> {
                    recoveryRequest.set(request);
                    var source = request.checkpointRequest().currentSource();
                    recoveryCompleted.set(true);
                    return CompletableFuture.completedFuture(new KafkaRecoveredPartition<>(
                            new Object(), source, 0, 0, 0, Optional.empty()));
                },
                metadataStore(durable, recoveryCompleted));

        KafkaPartitionStorage storage = opener.open(plan).join();

        KafkaPartitionBindingRecord observed = durable.get().value();
        assertThat(observed.observedLeaderId()).isEqualTo(authority.leaderId());
        assertThat(observed.observedLeaderEpoch()).isEqualTo(authority.leaderEpoch());
        assertThat(observed.observedBrokerEpoch()).isEqualTo(authority.brokerEpoch());
        assertThat(observed.observedLogStartOffset()).isZero();
        assertThat(observed.observedStableEndOffset()).isZero();
        assertThat(recoveryRequest.get().checkpointRequest().binding())
                .isEqualTo(plan.binding().durableRoot());
        storage.resign().join();
    }

    @Test
    void refusesWrongAuthorityMissingSessionAndMismatchedRecoveryResult() {
        KafkaLeaderAuthority authority = new KafkaLeaderAuthority(
                KafkaPartitionStorageTestSupport.identity(), 1, 5, 9);
        KafkaPartitionOpenPlan plan = KafkaPartitionStorageTestSupport.openPlan(authority);
        AcquiredAppendSession wrong = new AcquiredAppendSession(
                new AppendSession(plan.binding().streamId(), "broker-run", 1, "token", 1, 40_000),
                Optional.of(new KafkaLeaderAuthority(authority.identity(), 1, 4, 9).appendAuthority()));
        AtomicInteger launches = new AtomicInteger();
        DefaultKafkaPartitionOpener wrongAuthority = opener(
                streams(wrong, head(plan, wrong), new AtomicReference<>()),
                request -> {
                    launches.incrementAndGet();
                    return CompletableFuture.failedFuture(new AssertionError("must not recover"));
                });
        assertFailure(() -> wrongAuthority.open(plan).join(), ErrorCode.FENCED_APPEND);
        assertThat(launches).hasValue(0);

        AcquiredAppendSession acquired = new AcquiredAppendSession(
                new AppendSession(plan.binding().streamId(), "broker-run", 1, "token", 1, 40_000),
                Optional.of(authority.appendAuthority()));
        StableStreamHeadSnapshot missingSession = new StableStreamHeadSnapshot(
                plan.binding().streamId(), StreamState.ACTIVE, plan.profilePolicy().storageProfile(),
                0, 0, 0, 0, "", Optional.empty(), HEAD_SHA, 1);
        DefaultKafkaPartitionOpener noSession = opener(
                streams(acquired, missingSession, new AtomicReference<>()),
                request -> CompletableFuture.failedFuture(new AssertionError("must not recover")));
        assertFailure(() -> noSession.open(plan).join(), ErrorCode.FENCED_APPEND);

        DefaultKafkaPartitionOpener mismatched = opener(
                streams(acquired, head(plan, acquired), new AtomicReference<>()),
                request -> {
                    var source = request.checkpointRequest().currentSource();
                    return CompletableFuture.completedFuture(new KafkaRecoveredPartition<>(
                            new Object(), source, 0, 1, 0, Optional.empty()));
                });
        assertFailure(() -> mismatched.open(plan).join(), ErrorCode.METADATA_INVARIANT_VIOLATION);
    }

    private static DefaultKafkaPartitionOpener opener(
            StreamStorage streams,
            com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher launcher) {
        return opener(streams, launcher, null);
    }

    private static DefaultKafkaPartitionOpener opener(
            StreamStorage streams,
            com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher launcher,
            KafkaPartitionMetadataStore metadataStore) {
        KafkaRecordBatchCodec codec = new KafkaRecordBatchCodec();
        if (metadataStore != null) {
            return new DefaultKafkaPartitionOpener(
                    streams,
                    "broker-run",
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(10),
                    Executors.newSingleThreadScheduledExecutor(runnable -> {
                        Thread thread = new Thread(runnable, "opener-test-renewal");
                        thread.setDaemon(true);
                        return thread;
                    }),
                    launcher,
                    new KafkaAppendBatchEncoder(codec),
                    new KafkaFetchAssembler(codec),
                    metadataStore,
                    CLOCK);
        }
        return new DefaultKafkaPartitionOpener(
                streams,
                "broker-run",
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "opener-test-renewal");
                    thread.setDaemon(true);
                    return thread;
                }),
                launcher,
                new KafkaAppendBatchEncoder(codec),
                new KafkaFetchAssembler(codec),
                CLOCK);
    }

    private static KafkaPartitionMetadataStore metadataStore(
            AtomicReference<VersionedKafkaPartitionBinding> durable,
            AtomicBoolean recoveryCompleted) {
        return (KafkaPartitionMetadataStore)
                Proxy.newProxyInstance(
                        KafkaPartitionMetadataStore.class.getClassLoader(),
                        new Class<?>[] {KafkaPartitionMetadataStore.class},
                        (proxy, method, arguments) -> switch (method.getName()) {
                            case "get" ->
                                    CompletableFuture.completedFuture(
                                            Optional.of(durable.get()));
                            case "compareAndSet" -> {
                                if (!recoveryCompleted.get()) {
                                    throw new AssertionError(
                                            "leader observation was published before recovery completed");
                                }
                                VersionedKafkaPartitionBinding expected =
                                        (VersionedKafkaPartitionBinding) arguments[0];
                                KafkaPartitionBindingRecord update =
                                        (KafkaPartitionBindingRecord) arguments[1];
                                if (!durable.get().equals(expected)) {
                                    throw new AssertionError("unexpected binding CAS");
                                }
                                VersionedKafkaPartitionBinding published =
                                        new VersionedKafkaPartitionBinding(
                                                expected.key(),
                                                update.withMetadataVersion(
                                                        expected.metadataVersion() + 1),
                                                expected.metadataVersion() + 1,
                                                new Checksum(
                                                        ChecksumType.SHA256,
                                                        "44".repeat(32)));
                                durable.set(published);
                                yield CompletableFuture.completedFuture(published);
                            }
                            case "close" -> null;
                            case "toString" -> "opener-metadata";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == arguments[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static StreamStorage streams(
            AcquiredAppendSession acquired,
            StableStreamHeadSnapshot head,
            AtomicReference<AppendSessionRequest> requested) {
        return (StreamStorage) Proxy.newProxyInstance(
                StreamStorage.class.getClassLoader(),
                new Class<?>[] {StreamStorage.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "acquireAppendSession" -> {
                        if (arguments.length != 2 || !(arguments[1] instanceof AppendSessionRequest request)) {
                            throw new AssertionError("authoritative overload required");
                        }
                        requested.set(request);
                        yield CompletableFuture.completedFuture(acquired);
                    }
                    case "getStableHeadSnapshot" -> CompletableFuture.completedFuture(head);
                    case "close" -> null;
                    case "toString" -> "opener-streams";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static StableStreamHeadSnapshot head(
            KafkaPartitionOpenPlan plan,
            AcquiredAppendSession acquired) {
        return new StableStreamHeadSnapshot(
                plan.binding().streamId(),
                StreamState.ACTIVE,
                plan.profilePolicy().storageProfile(),
                0,
                0,
                0,
                0,
                "",
                Optional.of(acquired),
                HEAD_SHA,
                1);
    }

    private static void assertFailure(Runnable action, ErrorCode code) {
        assertThatThrownBy(action::run)
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(code);
    }
}
