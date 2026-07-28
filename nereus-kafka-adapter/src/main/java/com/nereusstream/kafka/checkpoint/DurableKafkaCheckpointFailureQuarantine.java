/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.checkpoint;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.KafkaCheckpointFailureMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaCheckpointFailure;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureRecord;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureSource;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Oxia-backed immutable quarantine and redacted first-failure audit. */
public final class DurableKafkaCheckpointFailureQuarantine
        implements KafkaCheckpointFailureQuarantine {
    private static final byte[] REFERENCE_DOMAIN =
            "nereus-kafka-checkpoint-reference-v1\0".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FAILURE_DOMAIN =
            "nereus-kafka-checkpoint-failure-v1\0".getBytes(StandardCharsets.US_ASCII);

    private final KafkaCheckpointFailureMetadataStore failures;
    private final Clock clock;

    public DurableKafkaCheckpointFailureQuarantine(
            KafkaCheckpointFailureMetadataStore failures, Clock clock) {
        this.failures = Objects.requireNonNull(failures, "failures");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<Boolean> isQuarantined(
            KafkaPartitionId identity,
            long partitionIncarnation,
            KafkaCheckpointReferenceRecord reference) {
        KafkaPartitionId exactIdentity = Objects.requireNonNull(identity, "identity");
        KafkaCheckpointReferenceRecord exactReference =
                Objects.requireNonNull(reference, "reference");
        byte[] expectedSha = referenceSha256(exactReference);
        return failures.get(exactIdentity, partitionIncarnation, exactReference.objectId())
                .thenApply(
                        optional -> {
                            if (optional.isEmpty()) {
                                return false;
                            }
                            requireExact(
                                    exactIdentity,
                                    partitionIncarnation,
                                    exactReference,
                                    expectedSha,
                                    optional.orElseThrow());
                            return true;
                        });
    }

    @Override
    public CompletableFuture<Void> quarantine(
            KafkaPartitionId identity,
            long partitionIncarnation,
            KafkaCheckpointReferenceRecord reference,
            KafkaCheckpointFailureSource source,
            Throwable failure) {
        KafkaPartitionId exactIdentity = Objects.requireNonNull(identity, "identity");
        KafkaCheckpointReferenceRecord exactReference =
                Objects.requireNonNull(reference, "reference");
        KafkaCheckpointFailureSource exactSource = Objects.requireNonNull(source, "source");
        NereusException exactFailure = requireQuarantinable(failure);
        long now = clock.millis();
        KafkaCheckpointFailureRecord requested =
                new KafkaCheckpointFailureRecord(
                        1,
                        exactIdentity.kafkaClusterId(),
                        exactIdentity.topicId(),
                        exactIdentity.partitionId(),
                        partitionIncarnation,
                        exactReference.objectId(),
                        referenceSha256(exactReference),
                        exactSource.wireId(),
                        exactFailure.code().name(),
                        failureSha256(exactFailure),
                        now,
                        0);
        return failures.putIfAbsent(requested)
                .thenAccept(
                        stored ->
                                requireExact(
                                        exactIdentity,
                                        partitionIncarnation,
                                        exactReference,
                                        requested.referenceSha256(),
                                        stored));
    }

    private static void requireExact(
            KafkaPartitionId identity,
            long partitionIncarnation,
            KafkaCheckpointReferenceRecord reference,
            byte[] referenceSha256,
            VersionedKafkaCheckpointFailure stored) {
        KafkaCheckpointFailureRecord value = stored.value();
        if (!value.identity().equals(identity)
                || value.partitionIncarnation() != partitionIncarnation
                || !value.objectId().equals(reference.objectId())
                || !Arrays.equals(value.referenceSha256(), referenceSha256)) {
            throw invariant("durable Kafka checkpoint quarantine conflicts with exact reference");
        }
    }

    private static NereusException requireQuarantinable(Throwable supplied) {
        Throwable exact = unwrap(Objects.requireNonNull(supplied, "failure"));
        if (!(exact instanceof NereusException nereus)) {
            throw new IllegalArgumentException(
                    "checkpoint quarantine requires a classified Nereus failure", exact);
        }
        boolean allowed =
                switch (nereus.code()) {
                    case OBJECT_NOT_FOUND,
                            OBJECT_CHECKSUM_MISMATCH,
                            UNSUPPORTED_FORMAT,
                            METADATA_INVARIANT_VIOLATION ->
                            true;
                    default -> false;
                };
        if (!allowed) {
            throw new IllegalArgumentException(
                    "checkpoint failure is not quarantine-eligible: " + nereus.code(), nereus);
        }
        return nereus;
    }

    static byte[] referenceSha256(KafkaCheckpointReferenceRecord reference) {
        MessageDigest digest = sha256();
        digest.update(REFERENCE_DOMAIN);
        updateInt(digest, reference.referenceVersion());
        updateText(digest, reference.objectId());
        updateText(digest, reference.objectKey());
        updateLong(digest, reference.objectLength());
        updateBytes(digest, reference.objectSha256());
        updateLong(digest, reference.checkpointOffset());
        updateLong(digest, reference.logStartOffsetAtCheckpoint());
        updateLong(digest, reference.sourceCommitVersion());
        updateBytes(digest, reference.sourceHeadSha256());
        updateText(digest, reference.writerBuild());
        updateLong(digest, reference.createdAtMillis());
        return digest.digest();
    }

    private static byte[] failureSha256(NereusException failure) {
        MessageDigest digest = sha256();
        digest.update(FAILURE_DOMAIN);
        updateText(digest, failure.getClass().getName());
        updateText(digest, failure.code().name());
        digest.update((byte) (failure.retriable() ? 1 : 0));
        updateText(digest, failure.getMessage() == null ? "" : failure.getMessage());
        return digest.digest();
    }

    private static void updateText(MessageDigest digest, String value) {
        updateBytes(digest, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void updateBytes(MessageDigest digest, byte[] value) {
        updateInt(digest, value.length);
        digest.update(value);
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
