/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.PartitionKey;
import com.nereusstream.metadata.oxia.codec.MetadataCodecException;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared exact-byte validation for the two destructive, read-before-delete adapters. */
final class RetirementMetadataSupport {
    private final RetirementMetadataClient client;
    private final AtomicBoolean closed = new AtomicBoolean();

    RetirementMetadataSupport(RetirementMetadataClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    CompletableFuture<Optional<RetirementMetadataValue>> get(
            String key, PartitionKey partitionKey) {
        ensureOpen();
        RetirementMetadataKey exactKey = new RetirementMetadataKey(key, partitionKey);
        return client.get(exactKey).thenApply(optional -> optional.map(value -> {
            if (!value.key().equals(key)) {
                throw invariant("retirement metadata read returned a different key");
            }
            return value;
        }));
    }

    CompletableFuture<Void> delete(
            String key, long expectedVersion, PartitionKey partitionKey) {
        ensureOpen();
        requireVersion(expectedVersion);
        return client.deleteIfVersion(
                new RetirementMetadataKey(key, partitionKey), expectedVersion);
    }

    void requireExpected(
            RetirementMetadataValue value,
            long expectedVersion,
            Checksum expectedDurableValueSha256) {
        requireVersion(expectedVersion);
        Checksum expected = requireSha256(expectedDurableValueSha256, "expectedDurableValueSha256");
        if (value.version() != expectedVersion) {
            throw new F4MetadataConditionFailedException(
                    "retirement metadata version changed before conditional delete");
        }
        if (!sha256(value.value()).equals(expected)) {
            throw new F4MetadataConditionFailedException(
                    "retirement metadata bytes changed before conditional delete");
        }
    }

    <T> T decode(RetirementMetadataValue value, Class<T> recordType) {
        try {
            return MetadataRecordCodecFactory.decodeEnvelope(value.value(), recordType);
        } catch (MetadataCodecException | IllegalArgumentException failure) {
            throw invariant("invalid metadata at an authoritative retirement key", failure);
        }
    }

    String recordType(RetirementMetadataValue value) {
        try {
            return MetadataRecordCodecFactory.recordType(value.value());
        } catch (MetadataCodecException | IllegalArgumentException failure) {
            throw invariant("invalid metadata envelope at an authoritative retirement key", failure);
        }
    }

    Checksum digest(RetirementMetadataValue value) {
        return sha256(value.value());
    }

    void close() {
        closed.set(true);
    }

    static F4MetadataConditionFailedException missing(String kind) {
        return new F4MetadataConditionFailedException(kind + " is absent during exact retirement");
    }

    static NereusException invariant(String message) {
        return invariant(message, null);
    }

    static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    static Checksum requireSha256(Checksum value, String field) {
        Objects.requireNonNull(value, field);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must use SHA256");
        }
        return value;
    }

    static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new NereusException(ErrorCode.STORAGE_CLOSED, false, "retirement metadata store is closed");
        }
    }

    private static void requireVersion(long version) {
        if (version < 0) {
            throw new IllegalArgumentException("expectedVersion must be non-negative");
        }
    }
}
