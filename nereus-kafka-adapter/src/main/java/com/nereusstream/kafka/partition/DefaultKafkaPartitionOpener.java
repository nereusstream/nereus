/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.AppendSessionRequest;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.kafka.checkpoint.DefaultKafkaCheckpointSourceValidator;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.codec.KafkaAppendBatchEncoder;
import com.nereusstream.kafka.codec.KafkaFetchAssembler;
import com.nereusstream.kafka.compaction.KafkaActivatedGenerationAuthority;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaRecoveredPartition;
import com.nereusstream.kafka.retention.KafkaPartitionMaintenance;
import com.nereusstream.kafka.retention.KafkaPartitionMaintenanceFactory;
import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Product-owned composition of authority acquisition, exact source recovery, and leader storage
 * construction.
 */
public final class DefaultKafkaPartitionOpener implements KafkaPartitionOpener {
    private static final int MAX_OBSERVATION_RETRIES = 32;

    private final StreamStorage streams;
    private final String writerId;
    private final Duration sessionTtl;
    private final Duration renewalInterval;
    private final ScheduledExecutorService renewalScheduler;
    private final KafkaPartitionRecoveryLauncher recoveryLauncher;
    private final KafkaAppendBatchEncoder appendEncoder;
    private final KafkaFetchAssembler fetchAssembler;
    private final Optional<KafkaPartitionMetadataStore> partitionMetadataStore;
    private final Optional<KafkaActivatedGenerationAuthority> activatedGenerations;
    private final Optional<KafkaPartitionMaintenanceFactory> maintenanceFactory;
    private final Clock clock;

    public DefaultKafkaPartitionOpener(
            StreamStorage streams,
            String writerId,
            Duration sessionTtl,
            Duration renewalInterval,
            ScheduledExecutorService renewalScheduler,
            KafkaPartitionRecoveryLauncher recoveryLauncher,
            KafkaAppendBatchEncoder appendEncoder,
            KafkaFetchAssembler fetchAssembler,
            Clock clock) {
        this(
                streams,
                writerId,
                sessionTtl,
                renewalInterval,
                renewalScheduler,
                recoveryLauncher,
                appendEncoder,
                fetchAssembler,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                clock);
    }

    public DefaultKafkaPartitionOpener(
            StreamStorage streams,
            String writerId,
            Duration sessionTtl,
            Duration renewalInterval,
            ScheduledExecutorService renewalScheduler,
            KafkaPartitionRecoveryLauncher recoveryLauncher,
            KafkaAppendBatchEncoder appendEncoder,
            KafkaFetchAssembler fetchAssembler,
            KafkaPartitionMetadataStore partitionMetadataStore,
            Clock clock) {
        this(
                streams,
                writerId,
                sessionTtl,
                renewalInterval,
                renewalScheduler,
                recoveryLauncher,
                appendEncoder,
                fetchAssembler,
                Optional.of(
                        Objects.requireNonNull(partitionMetadataStore, "partitionMetadataStore")),
                Optional.empty(),
                Optional.empty(),
                clock);
    }

    public DefaultKafkaPartitionOpener(
            StreamStorage streams,
            String writerId,
            Duration sessionTtl,
            Duration renewalInterval,
            ScheduledExecutorService renewalScheduler,
            KafkaPartitionRecoveryLauncher recoveryLauncher,
            KafkaAppendBatchEncoder appendEncoder,
            KafkaFetchAssembler fetchAssembler,
            KafkaPartitionMetadataStore partitionMetadataStore,
            KafkaActivatedGenerationAuthority activatedGenerations,
            Clock clock) {
        this(
                streams,
                writerId,
                sessionTtl,
                renewalInterval,
                renewalScheduler,
                recoveryLauncher,
                appendEncoder,
                fetchAssembler,
                Optional.of(
                        Objects.requireNonNull(partitionMetadataStore, "partitionMetadataStore")),
                Optional.of(
                        Objects.requireNonNull(activatedGenerations, "activatedGenerations")),
                Optional.empty(),
                clock);
    }

    public DefaultKafkaPartitionOpener(
            StreamStorage streams,
            String writerId,
            Duration sessionTtl,
            Duration renewalInterval,
            ScheduledExecutorService renewalScheduler,
            KafkaPartitionRecoveryLauncher recoveryLauncher,
            KafkaAppendBatchEncoder appendEncoder,
            KafkaFetchAssembler fetchAssembler,
            KafkaPartitionMetadataStore partitionMetadataStore,
            KafkaActivatedGenerationAuthority activatedGenerations,
            KafkaPartitionMaintenanceFactory maintenanceFactory,
            Clock clock) {
        this(
                streams,
                writerId,
                sessionTtl,
                renewalInterval,
                renewalScheduler,
                recoveryLauncher,
                appendEncoder,
                fetchAssembler,
                Optional.of(
                        Objects.requireNonNull(partitionMetadataStore, "partitionMetadataStore")),
                Optional.of(
                        Objects.requireNonNull(activatedGenerations, "activatedGenerations")),
                Optional.of(
                        Objects.requireNonNull(maintenanceFactory, "maintenanceFactory")),
                clock);
    }

    private DefaultKafkaPartitionOpener(
            StreamStorage streams,
            String writerId,
            Duration sessionTtl,
            Duration renewalInterval,
            ScheduledExecutorService renewalScheduler,
            KafkaPartitionRecoveryLauncher recoveryLauncher,
            KafkaAppendBatchEncoder appendEncoder,
            KafkaFetchAssembler fetchAssembler,
            Optional<KafkaPartitionMetadataStore> partitionMetadataStore,
            Optional<KafkaActivatedGenerationAuthority> activatedGenerations,
            Optional<KafkaPartitionMaintenanceFactory> maintenanceFactory,
            Clock clock) {
        this.streams = Objects.requireNonNull(streams, "streams");
        this.writerId = requireText(writerId, "writerId");
        this.sessionTtl = positive(sessionTtl, "sessionTtl");
        this.renewalInterval = positive(renewalInterval, "renewalInterval");
        if (renewalInterval.compareTo(sessionTtl) >= 0) {
            throw new IllegalArgumentException("renewalInterval must be shorter than sessionTtl");
        }
        this.renewalScheduler = Objects.requireNonNull(renewalScheduler, "renewalScheduler");
        this.recoveryLauncher = Objects.requireNonNull(recoveryLauncher, "recoveryLauncher");
        this.appendEncoder = Objects.requireNonNull(appendEncoder, "appendEncoder");
        this.fetchAssembler = Objects.requireNonNull(fetchAssembler, "fetchAssembler");
        this.partitionMetadataStore =
                Objects.requireNonNull(partitionMetadataStore, "partitionMetadataStore");
        this.activatedGenerations =
                Objects.requireNonNull(activatedGenerations, "activatedGenerations");
        this.maintenanceFactory =
                Objects.requireNonNull(maintenanceFactory, "maintenanceFactory");
        if (this.activatedGenerations.isPresent() && this.partitionMetadataStore.isEmpty()) {
            throw new IllegalArgumentException(
                    "activated generations require Kafka partition metadata");
        }
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<KafkaPartitionStorage> open(KafkaPartitionOpenPlan plan) {
        Objects.requireNonNull(plan, "plan");
        long deadline;
        try {
            deadline = Math.addExact(clock.millis(), plan.timeout().toMillis());
        } catch (ArithmeticException failure) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException(
                            "Kafka partition open deadline overflows", failure));
        }
        AppendSessionRequest request =
                AppendSessionRequest.authoritative(
                        new AppendSessionOptions(writerId, sessionTtl, false),
                        plan.authority().appendAuthority());
        try {
            remaining(deadline);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return streams.acquireAppendSession(plan.binding().streamId(), request)
                .thenCompose(acquired -> recover(plan, acquired, deadline))
                .thenApply(recovered -> storage(plan, recovered));
    }

    private CompletableFuture<RecoveredOpen> recover(
            KafkaPartitionOpenPlan plan, AcquiredAppendSession acquired, long deadline) {
        requireExactAuthority(plan, acquired);
        DefaultKafkaCheckpointSourceValidator validator =
                new DefaultKafkaCheckpointSourceValidator(
                        streams,
                        plan.binding().streamId(),
                        acquired,
                        plan.profilePolicy().storageProfile());
        return validator
                .loadCurrent()
                .thenCompose(
                        source -> {
                            return currentBinding(plan, source, deadline)
                                    .thenCompose(
                                            current -> {
                                                Duration remaining = remaining(deadline);
                                                KafkaCheckpointRecoveryRequest checkpointRequest =
                                                        new KafkaCheckpointRecoveryRequest(
                                                                plan.authority().identity(),
                                                                current,
                                                                source,
                                                                validator,
                                                                remaining);
                                                return recoveryLauncher
                                                        .recover(
                                                                new KafkaPartitionRecoveryRequest(
                                                                        checkpointRequest,
                                                                        remaining))
                                                        .thenCompose(
                                                                recovered -> {
                                                                    RecoveredOpen validated =
                                                                            validateRecovered(
                                                                                    plan,
                                                                                    acquired,
                                                                                    source,
                                                                                    recovered,
                                                                                    validator,
                                                                                    deadline);
                                                                    return observeBinding(
                                                                                    plan,
                                                                                    source,
                                                                                    deadline,
                                                                                    0)
                                                                            .thenApply(
                                                                                    ignored ->
                                                                                            validated);
                                                                });
                                            });
                        });
    }

    private CompletableFuture<VersionedKafkaPartitionBinding> currentBinding(
            KafkaPartitionOpenPlan plan,
            KafkaCheckpointSourceState source,
            long deadline) {
        if (partitionMetadataStore.isEmpty()) {
            return CompletableFuture.completedFuture(plan.binding().durableRoot());
        }
        remaining(deadline);
        return partitionMetadataStore
                .orElseThrow()
                .get(plan.authority().identity().durableId())
                .thenApply(
                        optional -> {
                            VersionedKafkaPartitionBinding current =
                                    optional.orElseThrow(
                                            () ->
                                                    invariant(
                                                            "Kafka binding disappeared before"
                                                                    + " partition recovery"));
                            validateObservation(plan, source, current);
                            return current;
                        });
    }

    private CompletableFuture<VersionedKafkaPartitionBinding> observeBinding(
            KafkaPartitionOpenPlan plan,
            KafkaCheckpointSourceState source,
            long deadline,
            int attempt) {
        if (partitionMetadataStore.isEmpty()) {
            return CompletableFuture.completedFuture(plan.binding().durableRoot());
        }
        if (attempt >= MAX_OBSERVATION_RETRIES) {
            return CompletableFuture.failedFuture(
                    new NereusException(
                            ErrorCode.METADATA_CONDITION_FAILED,
                            true,
                            "Kafka leader observation CAS retry budget exhausted"));
        }
        remaining(deadline);
        KafkaPartitionMetadataStore store = partitionMetadataStore.orElseThrow();
        return store
                .get(plan.authority().identity().durableId())
                .thenCompose(
                        optional -> {
                            VersionedKafkaPartitionBinding current =
                                    optional.orElseThrow(
                                            () ->
                                                    invariant(
                                                            "Kafka binding disappeared while"
                                                                    + " publishing leader"
                                                                    + " observation"));
                            validateObservation(plan, source, current);
                            KafkaPartitionBindingRecord root = current.value();
                            KafkaLeaderAuthority authority = plan.authority();
                            if (root.observedTopicName()
                                            .equals(authority.identity().observedTopicName())
                                    && root.observedLeaderId() == authority.leaderId()
                                    && root.observedLeaderEpoch() == authority.leaderEpoch()
                                    && root.observedBrokerEpoch() == authority.brokerEpoch()
                                    && root.observedLogStartOffset() == source.trimOffset()
                                    && root.observedStableEndOffset() == source.endOffset()) {
                                return CompletableFuture.completedFuture(current);
                            }
                            KafkaPartitionBindingRecord update =
                                    KafkaPartitionMetadataTransitions.observe(
                                            root,
                                            authority.identity().observedTopicName(),
                                            root.lastAppliedMetadataOffset(),
                                            authority.leaderId(),
                                            authority.leaderEpoch(),
                                            authority.brokerEpoch(),
                                            source.trimOffset(),
                                            source.endOffset(),
                                            Math.max(clock.millis(), root.updatedAtMillis()));
                            return store
                                    .compareAndSet(current, update)
                                    .exceptionallyCompose(
                                            failure ->
                                                    conditionFailure(failure)
                                                            ? observeBinding(
                                                                    plan,
                                                                    source,
                                                                    deadline,
                                                                    attempt + 1)
                                                            : CompletableFuture.failedFuture(
                                                                    unwrap(failure)));
                        });
    }

    private static void validateObservation(
            KafkaPartitionOpenPlan plan,
            KafkaCheckpointSourceState source,
            VersionedKafkaPartitionBinding current) {
        KafkaPartitionBindingRecord expected = plan.binding().durableRoot().value();
        KafkaPartitionBindingRecord actual = current.value();
        KafkaLeaderAuthority authority = plan.authority();
        if (!actual.identity().equals(expected.identity())
                || actual.lifecycle() != KafkaPartitionLifecycle.ACTIVE
                || actual.incarnation() != expected.incarnation()
                || !actual.streamId().equals(expected.streamId())
                || actual.payloadMappingId() != expected.payloadMappingId()
                || !actual.storageProfile().equals(expected.storageProfile())) {
            throw invariant("Kafka binding changed before leader observation publication");
        }
        if (actual.observedLeaderEpoch() > authority.leaderEpoch()
                || (actual.observedLeaderEpoch() == authority.leaderEpoch()
                        && actual.observedLeaderId() >= 0
                        && actual.observedLeaderId() != authority.leaderId())
                || (actual.observedLeaderEpoch() == authority.leaderEpoch()
                        && actual.observedLeaderId() == authority.leaderId()
                        && actual.observedBrokerEpoch() > authority.brokerEpoch())) {
            throw fenced("Kafka binding already records a newer or conflicting leader authority");
        }
        if (actual.observedLogStartOffset() > source.trimOffset()
                || actual.observedStableEndOffset() > source.endOffset()) {
            throw invariant("Kafka binding observation is ahead of the durable stream head");
        }
    }

    private KafkaPartitionStorage storage(KafkaPartitionOpenPlan plan, RecoveredOpen recovered) {
        Optional<KafkaPartitionMaintenance> maintenance =
                maintenanceFactory.map(
                        factory ->
                                Objects.requireNonNull(
                                        factory.create(
                                                plan.authority().identity(),
                                                plan.authority().leaderEpoch(),
                                                plan.binding().streamId(),
                                                recovered.sourceValidator()),
                                        "Kafka maintenance factory returned null"));
        return partitionMetadataStore
                .<KafkaPartitionStorage>map(
                        store ->
                                maintenance
                                        .<KafkaPartitionStorage>map(
                                                exact ->
                                                        new DefaultKafkaPartitionStorage(
                                                                plan.authority().identity(),
                                                                streams,
                                                                plan.binding().streamId(),
                                                                recovered.acquiredSession(),
                                                                recovered.recovered().frozenSource(),
                                                                plan.profilePolicy(),
                                                                appendEncoder,
                                                                fetchAssembler,
                                                                store,
                                                                activatedGenerations.orElse(
                                                                        KafkaActivatedGenerationAuthority
                                                                                .unavailable()),
                                                                exact,
                                                                renewalScheduler,
                                                                sessionTtl,
                                                                renewalInterval))
                                        .orElseGet(
                                                () ->
                                                        new DefaultKafkaPartitionStorage(
                                                                plan.authority().identity(),
                                                                streams,
                                                                plan.binding().streamId(),
                                                                recovered.acquiredSession(),
                                                                recovered.recovered().frozenSource(),
                                                                plan.profilePolicy(),
                                                                appendEncoder,
                                                                fetchAssembler,
                                                                store,
                                                                activatedGenerations.orElse(
                                                                        KafkaActivatedGenerationAuthority
                                                                                .unavailable()),
                                                                renewalScheduler,
                                                                sessionTtl,
                                                                renewalInterval)))
                .orElseGet(
                        () ->
                                new DefaultKafkaPartitionStorage(
                                        plan.authority().identity(),
                                        streams,
                                        plan.binding().streamId(),
                                        recovered.acquiredSession(),
                                        recovered.recovered().frozenSource(),
                                        plan.profilePolicy(),
                                        appendEncoder,
                                        fetchAssembler,
                                        renewalScheduler,
                                        sessionTtl,
                                        renewalInterval));
    }

    private RecoveredOpen validateRecovered(
            KafkaPartitionOpenPlan plan,
            AcquiredAppendSession acquired,
            KafkaCheckpointSourceState source,
            KafkaRecoveredPartition<?> recovered,
            DefaultKafkaCheckpointSourceValidator sourceValidator,
            long deadline) {
        Objects.requireNonNull(recovered, "recovered");
        remaining(deadline);
        if (!recovered.frozenSource().equals(source)
                || recovered.replayEndOffset() != source.endOffset()
                || recovered.replayStartOffset() < source.trimOffset()
                || recovered.replayStartOffset() > recovered.replayEndOffset()
                || source.authority().authorityEpoch() != plan.authority().leaderEpoch()) {
            throw invariant("Kafka recovery result does not match the exact frozen open source");
        }
        return new RecoveredOpen(acquired, recovered, sourceValidator);
    }

    private static void requireExactAuthority(
            KafkaPartitionOpenPlan plan, AcquiredAppendSession acquired) {
        if (!acquired.session().streamId().equals(plan.binding().streamId())
                || !acquired.authority().equals(Optional.of(plan.authority().appendAuthority()))) {
            throw fenced("Nereus returned a different Kafka append authority/session");
        }
    }

    private Duration remaining(long deadline) {
        long millis = deadline - clock.millis();
        if (millis <= 0) {
            throw new NereusException(
                    ErrorCode.TIMEOUT, true, "Kafka partition open deadline expired");
        }
        return Duration.ofMillis(millis);
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private static NereusException fenced(String message) {
        return new NereusException(ErrorCode.FENCED_APPEND, false, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static boolean conditionFailure(Throwable failure) {
        return unwrap(failure) instanceof KafkaMetadataConditionFailedException;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = Objects.requireNonNull(failure, "failure");
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record RecoveredOpen(
            AcquiredAppendSession acquiredSession,
            KafkaRecoveredPartition<?> recovered,
            DefaultKafkaCheckpointSourceValidator sourceValidator) {
        private RecoveredOpen {
            Objects.requireNonNull(acquiredSession, "acquiredSession");
            Objects.requireNonNull(recovered, "recovered");
            Objects.requireNonNull(sourceValidator, "sourceValidator");
        }
    }
}
