/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationAdminStore;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationCodecV1;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationKeys;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationValue;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.metadata.oxia.CapabilityMetadataClient;
import com.nereusstream.metadata.oxia.CapabilityMetadataValue;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Read-only broker adapter over the separately provisioned global Oxia namespace authority. */
final class OxiaBookKeeperLedgerIdNamespaceReservationStore
        implements BookKeeperLedgerIdNamespaceReservationAdminStore {
    private final CapabilityMetadataClient client;

    OxiaBookKeeperLedgerIdNamespaceReservationStore(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime) {
        this(Objects.requireNonNull(runtime, "runtime")
                .capabilityMetadataClient(Objects.requireNonNull(configuration, "configuration")));
    }

    OxiaBookKeeperLedgerIdNamespaceReservationStore(CapabilityMetadataClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public CompletableFuture<Optional<BookKeeperLedgerIdNamespaceReservation>> read(
            String providerScopeSha256,
            int prefixBits,
            long prefixValue,
            Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        String key = BookKeeperLedgerIdNamespaceReservationKeys.key(
                providerScopeSha256,
                prefixBits,
                prefixValue);
        return deadline.bound(client.get(
                        key,
                        BookKeeperLedgerIdNamespaceReservationKeys.partitionKey(providerScopeSha256)))
                .thenApply(optional -> optional.map(stored -> materialize(key, stored)));
    }

    @Override
    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> create(
            BookKeeperLedgerIdNamespaceReservationValue value,
            Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        BookKeeperLedgerIdNamespaceReservationValue exact = Objects.requireNonNull(value, "value");
        String key = BookKeeperLedgerIdNamespaceReservationKeys.key(
                exact.bookKeeperProviderScopeSha256(),
                exact.ledgerIdPrefixBits(),
                exact.ledgerIdPrefixValue());
        String partition = BookKeeperLedgerIdNamespaceReservationKeys.partitionKey(
                exact.bookKeeperProviderScopeSha256());
        byte[] encoded = BookKeeperLedgerIdNamespaceReservationCodecV1.encode(exact);
        CompletableFuture<BookKeeperLedgerIdNamespaceReservation> write = deadline
                .bound(client.putIfAbsent(key, encoded, partition))
                .thenApply(stored -> materialize(key, stored));
        return recoverWrite(
                write,
                () -> deadline.bound(client.get(key, partition)),
                key,
                encoded,
                -1);
    }

    @Override
    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> compareAndSet(
            BookKeeperLedgerIdNamespaceReservationValue replacement,
            long expectedMetadataVersion,
            Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        if (expectedMetadataVersion < 0) {
            throw new IllegalArgumentException("expectedMetadataVersion must be non-negative");
        }
        BookKeeperLedgerIdNamespaceReservationValue exact = Objects.requireNonNull(
                replacement, "replacement");
        String key = BookKeeperLedgerIdNamespaceReservationKeys.key(
                exact.bookKeeperProviderScopeSha256(),
                exact.ledgerIdPrefixBits(),
                exact.ledgerIdPrefixValue());
        String partition = BookKeeperLedgerIdNamespaceReservationKeys.partitionKey(
                exact.bookKeeperProviderScopeSha256());
        byte[] encoded = BookKeeperLedgerIdNamespaceReservationCodecV1.encode(exact);
        CompletableFuture<BookKeeperLedgerIdNamespaceReservation> write = deadline
                .bound(client.putIfVersion(key, encoded, expectedMetadataVersion, partition))
                .thenApply(stored -> materialize(key, stored));
        return recoverWrite(
                write,
                () -> deadline.bound(client.get(key, partition)),
                key,
                encoded,
                expectedMetadataVersion);
    }

    private CompletableFuture<BookKeeperLedgerIdNamespaceReservation> recoverWrite(
            CompletableFuture<BookKeeperLedgerIdNamespaceReservation> write,
            java.util.function.Supplier<CompletableFuture<Optional<CapabilityMetadataValue>>> reload,
            String key,
            byte[] desired,
            long expectedVersion) {
        return write.handle((result, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(result);
            }
            Throwable original = unwrap(failure);
            return reload.get().thenCompose(optional -> {
                if (optional.isPresent()) {
                    CapabilityMetadataValue stored = optional.orElseThrow();
                    if (java.util.Arrays.equals(stored.value(), desired)
                            && (expectedVersion < 0 || stored.version() > expectedVersion)) {
                        return CompletableFuture.completedFuture(materialize(key, stored));
                    }
                }
                return CompletableFuture.failedFuture(original);
            });
        }).thenCompose(java.util.function.Function.identity());
    }

    private static BookKeeperLedgerIdNamespaceReservation materialize(
            String expectedKey,
            CapabilityMetadataValue stored) {
        if (!stored.key().equals(expectedKey)) {
            throw new IllegalArgumentException(
                    "namespace reservation metadata returned a non-canonical key");
        }
        BookKeeperLedgerIdNamespaceReservationValue value =
                BookKeeperLedgerIdNamespaceReservationCodecV1.decode(stored.value());
        BookKeeperLedgerIdNamespaceReservationKeys.requireExact(
                stored.key(),
                value.bookKeeperProviderScopeSha256(),
                value.ledgerIdPrefixBits(),
                value.ledgerIdPrefixValue());
        return value.materialize(
                stored.key(),
                stored.version(),
                sha256(stored.value()));
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

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
