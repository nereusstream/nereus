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
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaRecoveredPartition;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Product-owned composition of authority acquisition, exact source recovery, and leader storage construction. */
public final class DefaultKafkaPartitionOpener implements KafkaPartitionOpener {
    private final StreamStorage streams;
    private final String writerId;
    private final Duration sessionTtl;
    private final KafkaPartitionRecoveryLauncher recoveryLauncher;
    private final KafkaAppendBatchEncoder appendEncoder;
    private final KafkaFetchAssembler fetchAssembler;
    private final Clock clock;

    public DefaultKafkaPartitionOpener(
            StreamStorage streams,
            String writerId,
            Duration sessionTtl,
            KafkaPartitionRecoveryLauncher recoveryLauncher,
            KafkaAppendBatchEncoder appendEncoder,
            KafkaFetchAssembler fetchAssembler,
            Clock clock) {
        this.streams = Objects.requireNonNull(streams, "streams");
        this.writerId = requireText(writerId, "writerId");
        this.sessionTtl = positive(sessionTtl, "sessionTtl");
        this.recoveryLauncher = Objects.requireNonNull(recoveryLauncher, "recoveryLauncher");
        this.appendEncoder = Objects.requireNonNull(appendEncoder, "appendEncoder");
        this.fetchAssembler = Objects.requireNonNull(fetchAssembler, "fetchAssembler");
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
                    new IllegalArgumentException("Kafka partition open deadline overflows", failure));
        }
        AppendSessionRequest request = AppendSessionRequest.authoritative(
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
            KafkaPartitionOpenPlan plan,
            AcquiredAppendSession acquired,
            long deadline) {
        requireExactAuthority(plan, acquired);
        DefaultKafkaCheckpointSourceValidator validator = new DefaultKafkaCheckpointSourceValidator(
                streams,
                plan.binding().streamId(),
                acquired,
                plan.profilePolicy().storageProfile());
        return validator.loadCurrent().thenCompose(source -> {
            Duration remaining = remaining(deadline);
            KafkaCheckpointRecoveryRequest checkpointRequest = new KafkaCheckpointRecoveryRequest(
                    plan.authority().identity(),
                    plan.binding().durableRoot(),
                    source,
                    validator,
                    remaining);
            return recoveryLauncher.recover(new KafkaPartitionRecoveryRequest(
                            checkpointRequest, remaining))
                    .thenApply(recovered -> validateRecovered(plan, acquired, source, recovered, deadline));
        });
    }

    private KafkaPartitionStorage storage(KafkaPartitionOpenPlan plan, RecoveredOpen recovered) {
        return new DefaultKafkaPartitionStorage(
                plan.authority().identity(),
                streams,
                plan.binding().streamId(),
                recovered.acquiredSession(),
                recovered.recovered().frozenSource(),
                plan.profilePolicy(),
                appendEncoder,
                fetchAssembler);
    }

    private RecoveredOpen validateRecovered(
            KafkaPartitionOpenPlan plan,
            AcquiredAppendSession acquired,
            KafkaCheckpointSourceState source,
            KafkaRecoveredPartition<?> recovered,
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
        return new RecoveredOpen(acquired, recovered);
    }

    private static void requireExactAuthority(
            KafkaPartitionOpenPlan plan,
            AcquiredAppendSession acquired) {
        if (!acquired.session().streamId().equals(plan.binding().streamId())
                || !acquired.authority().equals(Optional.of(plan.authority().appendAuthority()))) {
            throw fenced("Nereus returned a different Kafka append authority/session");
        }
    }

    private Duration remaining(long deadline) {
        long millis = deadline - clock.millis();
        if (millis <= 0) {
            throw new NereusException(ErrorCode.TIMEOUT, true, "Kafka partition open deadline expired");
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
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private static NereusException fenced(String message) {
        return new NereusException(ErrorCode.FENCED_APPEND, false, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private record RecoveredOpen(
            AcquiredAppendSession acquiredSession,
            KafkaRecoveredPartition<?> recovered) {
        private RecoveredOpen {
            Objects.requireNonNull(acquiredSession, "acquiredSession");
            Objects.requireNonNull(recovered, "recovered");
        }
    }
}
