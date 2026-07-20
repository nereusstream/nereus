/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationCodecV1;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationKeys;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationStore;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationValue;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.metadata.oxia.CapabilityMetadataClient;
import com.nereusstream.metadata.oxia.CapabilityMetadataValue;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Production exact-key/CAS adapter for the BookKeeper rollout activation authority. */
final class OxiaBookKeeperProtocolActivationStore
        implements BookKeeperProtocolActivationStore {
    private final CapabilityMetadataClient client;

    OxiaBookKeeperProtocolActivationStore(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime) {
        this(Objects.requireNonNull(runtime, "runtime")
                .capabilityMetadataClient(Objects.requireNonNull(configuration, "configuration")));
    }

    OxiaBookKeeperProtocolActivationStore(CapabilityMetadataClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public CompletableFuture<Optional<BookKeeperProtocolActivation>> read(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        BookKeeperWalConfiguration exact = Objects.requireNonNull(configuration, "configuration");
        BookKeeperLedgerIdNamespaceReservation reservation = Objects.requireNonNull(
                namespace, "namespace");
        String key = BookKeeperProtocolActivationKeys.key(
                exact.clusterAlias(),
                exact.configurationBindingSha256().value(),
                reservation.ledgerIdNamespaceSha256().value());
        return deadline.bound(client.get(
                        key,
                        BookKeeperProtocolActivationKeys.partitionKey(exact.clusterAlias())))
                .thenApply(optional -> optional.map(stored -> materialize(key, stored)));
    }

    @Override
    public CompletableFuture<BookKeeperProtocolActivation> create(
            BookKeeperProtocolActivationValue value,
            Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        BookKeeperProtocolActivationValue exact = Objects.requireNonNull(value, "value");
        String key = BookKeeperProtocolActivationKeys.key(
                exact.clusterAlias(),
                exact.configurationBindingSha256(),
                exact.ledgerIdNamespaceSha256());
        String partition = BookKeeperProtocolActivationKeys.partitionKey(exact.clusterAlias());
        byte[] encoded = BookKeeperProtocolActivationCodecV1.encode(exact);
        CompletableFuture<BookKeeperProtocolActivation> write = deadline
                .bound(client.putIfAbsent(key, encoded, partition))
                .thenApply(stored -> materialize(key, stored));
        return recoverWrite(write, deadline, key, partition, encoded, -1);
    }

    @Override
    public CompletableFuture<BookKeeperProtocolActivation> compareAndSet(
            BookKeeperProtocolActivationValue replacement,
            long expectedMetadataVersion,
            Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        if (expectedMetadataVersion < 0) {
            throw new IllegalArgumentException("expectedMetadataVersion must be non-negative");
        }
        BookKeeperProtocolActivationValue exact = Objects.requireNonNull(
                replacement, "replacement");
        String key = BookKeeperProtocolActivationKeys.key(
                exact.clusterAlias(),
                exact.configurationBindingSha256(),
                exact.ledgerIdNamespaceSha256());
        String partition = BookKeeperProtocolActivationKeys.partitionKey(exact.clusterAlias());
        byte[] encoded = BookKeeperProtocolActivationCodecV1.encode(exact);
        CompletableFuture<BookKeeperProtocolActivation> write = deadline
                .bound(client.putIfVersion(key, encoded, expectedMetadataVersion, partition))
                .thenApply(stored -> materialize(key, stored));
        return recoverWrite(
                write, deadline, key, partition, encoded, expectedMetadataVersion);
    }

    private CompletableFuture<BookKeeperProtocolActivation> recoverWrite(
            CompletableFuture<BookKeeperProtocolActivation> write,
            BookKeeperOperationDeadline deadline,
            String key,
            String partition,
            byte[] desired,
            long expectedVersion) {
        return write.handle((result, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(result);
            }
            Throwable original = unwrap(failure);
            return deadline.bound(client.get(key, partition)).thenCompose(optional -> {
                if (optional.isPresent()) {
                    CapabilityMetadataValue stored = optional.orElseThrow();
                    if (Arrays.equals(stored.value(), desired)
                            && (expectedVersion < 0 || stored.version() > expectedVersion)) {
                        return CompletableFuture.completedFuture(materialize(key, stored));
                    }
                }
                return CompletableFuture.failedFuture(original);
            });
        }).thenCompose(java.util.function.Function.identity());
    }

    private static BookKeeperProtocolActivation materialize(
            String expectedKey,
            CapabilityMetadataValue stored) {
        if (!stored.key().equals(expectedKey)) {
            throw new IllegalArgumentException("BookKeeper activation metadata key drifted");
        }
        BookKeeperProtocolActivationValue value =
                BookKeeperProtocolActivationCodecV1.decode(stored.value());
        BookKeeperProtocolActivationKeys.requireExact(
                stored.key(),
                value.clusterAlias(),
                value.configurationBindingSha256(),
                value.ledgerIdNamespaceSha256());
        return value.materialize(
                stored.key(),
                stored.version(),
                sha256(stored.value()));
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

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
