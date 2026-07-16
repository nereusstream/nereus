/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.codec.MetadataCodecException;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.GenerationSequenceRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared exact-byte codec/CAS helpers for the two focused Phase 4 metadata stores. */
final class F4MetadataStoreSupport {
    private final PartitionedOxiaClient client;
    private final Clock clock;
    private final AtomicBoolean closed = new AtomicBoolean();

    F4MetadataStoreSupport(PartitionedOxiaClient client, Clock clock) {
        this.client = Objects.requireNonNull(client, "client");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    PartitionedOxiaClient client() {
        ensureOpen();
        return client;
    }

    long now() {
        ensureOpen();
        return clock.millis();
    }

    <T> CompletableFuture<Optional<Decoded<T>>> get(
            String key, PartitionKey partition, Class<T> type) {
        ensureOpen();
        return client.get(key, partition).thenApply(optional -> optional.map(value -> decode(value, type)));
    }

    <T> CompletableFuture<Decoded<T>> create(
            String key, PartitionKey partition, T value, Class<T> type) {
        ensureOpen();
        byte[] bytes = encodeForWrite(value, type);
        return client.putIfAbsent(key, bytes, partition)
                .thenApply(result -> new Decoded<>(key, hydrate(value, result.version()), result.version(), sha256(bytes)));
    }

    <T> CompletableFuture<Decoded<T>> compareAndSet(
            String key, PartitionKey partition, T value, Class<T> type, long expectedVersion) {
        ensureOpen();
        requireVersion(expectedVersion);
        byte[] bytes = encodeForWrite(value, type);
        return client.putIfVersion(key, bytes, expectedVersion, partition)
                .thenApply(result -> new Decoded<>(key, hydrate(value, result.version()), result.version(), sha256(bytes)));
    }

    CompletableFuture<Void> delete(String key, PartitionKey partition, long expectedVersion) {
        ensureOpen();
        requireVersion(expectedVersion);
        return client.deleteIfVersion(key, expectedVersion, partition);
    }

    <T> Decoded<T> decode(PartitionedOxiaClient.VersionedValue stored, Class<T> type) {
        try {
            T decoded = MetadataRecordCodecFactory.decodeEnvelope(stored.value(), type);
            return new Decoded<>(
                    stored.key(), hydrate(decoded, stored.version()), stored.version(), sha256(stored.value()));
        } catch (MetadataCodecException | IllegalArgumentException failure) {
            throw invariant("invalid F4 metadata at an authoritative key", failure);
        }
    }

    String scopeSha256(String canonicalScope) {
        return sha256(canonicalScope.getBytes(StandardCharsets.UTF_8)).value();
    }

    Checksum decodeDigest(byte[] exactStoredValue) {
        return sha256(Objects.requireNonNull(exactStoredValue, "exactStoredValue"));
    }

    F4ScanToken validateToken(
            Optional<F4ScanToken> continuation,
            String cluster,
            F4ScanKind kind,
            String scopeSha256,
            String prefix) {
        Objects.requireNonNull(continuation, "continuation");
        if (continuation.isEmpty()) {
            return null;
        }
        F4ScanToken token = continuation.orElseThrow();
        if (!token.cluster().equals(cluster) || token.kind() != kind
                || !token.scopeIdentitySha256().equals(scopeSha256)
                || !token.scanPrefix().equals(prefix)
                || !token.exclusiveLastKey().startsWith(prefix)) {
            throw new IllegalArgumentException("F4 scan continuation belongs to another scope");
        }
        return token;
    }

    static String prefixStart(String prefix) {
        return fixedDepthStart(prefix, 1);
    }

    static String prefixEnd(String prefix) {
        return fixedDepthEnd(prefix, 1);
    }

    static String fixedDepthStart(String prefix, int descendantSegments) {
        Objects.requireNonNull(prefix, "prefix");
        if (descendantSegments <= 0) {
            throw new IllegalArgumentException("descendantSegments must be positive");
        }
        if (descendantSegments == 1) {
            return prefix + "/";
        }
        return prefix + "/!".repeat(descendantSegments - 1) + "/";
    }

    static String fixedDepthEnd(String prefix, int descendantSegments) {
        Objects.requireNonNull(prefix, "prefix");
        if (descendantSegments <= 0) {
            throw new IllegalArgumentException("descendantSegments must be positive");
        }
        if (descendantSegments == 1) {
            return prefix + "/~";
        }
        return prefix + "/~".repeat(descendantSegments - 1) + "/";
    }

    static void requirePageLimit(int limit) {
        if (limit <= 0 || limit > 1_000) {
            throw new IllegalArgumentException("scan limit must be in [1, 1000]");
        }
    }

    static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    static boolean isConditionFailure(Throwable failure) {
        Throwable cause = unwrap(failure);
        return cause instanceof KeyAlreadyExistsException
                || cause instanceof UnexpectedVersionIdException
                || cause instanceof F4MetadataConditionFailedException
                || cause instanceof CursorMetadataConditionFailedException
                || cause instanceof ProjectionMetadataConditionFailedException;
    }

    static F4MetadataConditionFailedException condition(String operation, Throwable failure) {
        return new F4MetadataConditionFailedException(operation + " lost its exact single-key condition", unwrap(failure));
    }

    static NereusException invariant(String message) {
        return invariant(message, null);
    }

    static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    static <T> CompletableFuture<T> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }

    void close() {
        closed.set(true);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new NereusException(ErrorCode.STORAGE_CLOSED, false, "F4 metadata store is closed");
        }
    }

    private static void requireVersion(long version) {
        if (version < 0) {
            throw new IllegalArgumentException("expectedVersion must be non-negative");
        }
    }

    private static <T> byte[] encodeForWrite(T value, Class<T> type) {
        Objects.requireNonNull(value, "value");
        requireEncodedMetadataVersionZero(value);
        return MetadataRecordCodecFactory.encodeEnvelope(value, type);
    }

    private static void requireEncodedMetadataVersionZero(Object value) {
        long metadataVersion;
        if (value instanceof GenerationSequenceRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof GenerationIndexRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof GenerationProtocolActivationRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof MaterializationStreamRegistrationRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof MaterializationTaskRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof MaterializationCheckpointRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof RangeRetentionStatsRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof RecoveryCheckpointRootRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof PhysicalObjectRootRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof ObjectReaderLeaseRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof ObjectProtectionRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof GcRetirementManifestRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof GcRetirementProtectionRecord record) {
            metadataVersion = record.metadataVersion();
        } else if (value instanceof GcRetirementRemovalRecord record) {
            metadataVersion = record.metadataVersion();
        } else {
            throw new IllegalArgumentException("unsupported F4 record type: " + value.getClass());
        }
        if (metadataVersion != 0) {
            throw new IllegalArgumentException("encoded F4 metadataVersion must be zero");
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T hydrate(T value, long version) {
        Object hydrated;
        if (value instanceof GenerationSequenceRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof GenerationIndexRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof GenerationProtocolActivationRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof MaterializationStreamRegistrationRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof MaterializationTaskRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof MaterializationCheckpointRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof RangeRetentionStatsRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof RecoveryCheckpointRootRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof PhysicalObjectRootRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof ObjectReaderLeaseRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof ObjectProtectionRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof GcRetirementManifestRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof GcRetirementProtectionRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else if (value instanceof GcRetirementRemovalRecord record) {
            hydrated = record.withMetadataVersion(version);
        } else {
            throw new IllegalArgumentException("unsupported F4 record type: " + value.getClass());
        }
        return (T) hydrated;
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    record Decoded<T>(String key, T value, long version, Checksum durableSha256) {
        Decoded {
            F4ValueValidation.text(key, "key");
            Objects.requireNonNull(value, "value");
            F4ValueValidation.version(version);
            F4ValueValidation.sha256(durableSha256, "durableSha256");
        }
    }
}
