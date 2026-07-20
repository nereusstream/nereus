/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationCodecV1;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationKeys;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationStore;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationValue;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Read-only broker adapter over the separately provisioned global Oxia namespace authority. */
final class OxiaBookKeeperLedgerIdNamespaceReservationStore
        implements BookKeeperLedgerIdNamespaceReservationStore {
    private final OxiaClientConfiguration configuration;
    private final SharedOxiaClientRuntime runtime;

    OxiaBookKeeperLedgerIdNamespaceReservationStore(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    @Override
    public CompletableFuture<Optional<BookKeeperLedgerIdNamespaceReservation>> read(
            String providerScopeSha256,
            int prefixBits,
            long prefixValue,
            Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("namespace reservation read timeout must be positive");
        }
        String key = BookKeeperLedgerIdNamespaceReservationKeys.key(
                providerScopeSha256,
                prefixBits,
                prefixValue);
        return runtime.readCapability(
                        configuration,
                        key,
                        BookKeeperLedgerIdNamespaceReservationKeys.partitionKey(providerScopeSha256))
                .thenApply(optional -> optional.map(stored -> {
            if (!stored.key().equals(key)) {
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
                }));
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
