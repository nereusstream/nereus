/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class BookKeeperProtocolAdministrationTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(1);
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(100), ZoneOffset.UTC);

    @Test
    void namespaceProvisionIsIdempotentAndRevokeIsTerminalVersionedCas() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        NamespaceStore store = new NamespaceStore();
        BookKeeperLedgerIdNamespaceProvisioningCoordinator coordinator =
                new BookKeeperLedgerIdNamespaceProvisioningCoordinator(store, CLOCK);

        BookKeeperLedgerIdNamespaceReservation provisioned = coordinator.provision(
                configuration, "deployment-a", "22".repeat(32), TIMEOUT).join();
        assertThat(provisioned.lifecycle())
                .isEqualTo(BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE);
        assertThat(coordinator.provision(
                        configuration, "deployment-a", "22".repeat(32), TIMEOUT).join())
                .isEqualTo(provisioned);
        assertThatThrownBy(() -> coordinator.provision(
                        configuration, "deployment-b", "22".repeat(32), TIMEOUT).join())
                .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);

        BookKeeperLedgerIdNamespaceReservation revoked = coordinator.revoke(
                configuration,
                "deployment-a",
                "44".repeat(32),
                provisioned.metadataVersion(),
                TIMEOUT).join();
        assertThat(revoked.lifecycle())
                .isEqualTo(BookKeeperLedgerIdNamespaceReservation.Lifecycle.REVOKED);
        assertThat(revoked.reservationEpoch()).isEqualTo(2);
        assertThatThrownBy(() -> coordinator.provision(
                        configuration, "deployment-a", "22".repeat(32), TIMEOUT).join())
                .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);
    }

    @Test
    void activationKeepsPublicationIdentityStableButRebindsEveryDeletionRecord() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerIdNamespaceReservation namespace =
                BookKeeperProtocolActivationCodecV1Test.namespace(configuration);
        ActivationStore store = new ActivationStore(configuration, namespace);
        BookKeeperProtocolActivationCoordinator coordinator =
                new BookKeeperProtocolActivationCoordinator(store, CLOCK);

        BookKeeperProtocolActivation prepared = coordinator.prepare(
                configuration, namespace, 7, "55".repeat(32), TIMEOUT).join();
        assertThat(prepared.value().lifecycle())
                .isEqualTo(BookKeeperProtocolActivationLifecycle.PREPARED);
        assertThat(coordinator.prepare(
                        configuration, namespace, 7, "55".repeat(32), TIMEOUT).join())
                .isEqualTo(prepared);

        BookKeeperProtocolActivation walOnly = coordinator.activate(
                configuration,
                namespace,
                BookKeeperProtocolActivationUpdate.publications(
                        7, "55".repeat(32), false, false, prepared.metadataVersion()),
                TIMEOUT).join();
        assertThat(walOnly.value().walOnlyPublicationEnabled()).isTrue();
        assertThat(walOnly.supportsAllPublications()).isFalse();

        BookKeeperProtocolActivation allProfiles = coordinator.activate(
                configuration,
                namespace,
                BookKeeperProtocolActivationUpdate.publications(
                        8, "66".repeat(32), true, true, walOnly.metadataVersion()),
                TIMEOUT).join();
        assertThat(allProfiles.supportsAllPublications()).isTrue();

        BookKeeperProtocolActivation deletion = coordinator.activate(
                configuration,
                namespace,
                new BookKeeperProtocolActivationUpdate(
                        8,
                        "66".repeat(32),
                        true,
                        true,
                        true,
                        "77".repeat(32),
                        "88".repeat(32),
                        "99".repeat(32),
                        allProfiles.metadataVersion()),
                TIMEOUT).join();
        assertThat(deletion.publicationActivationSha256())
                .isEqualTo(allProfiles.publicationActivationSha256());
        assertThat(deletion.activationRecordSha256())
                .isNotEqualTo(allProfiles.activationRecordSha256());
        assertThat(deletion.deletionProof().activationRecordSha256())
                .isEqualTo(deletion.activationRecordSha256());
        assertThat(new DefaultBookKeeperProtocolActivationVerifier(
                        store,
                        configuration,
                        namespace,
                        readiness(8, "66".repeat(32), 2))
                .requireActive(TIMEOUT).join())
                .isEqualTo(deletion.deletionProof());
        assertThatThrownBy(() -> new DefaultBookKeeperProtocolActivationVerifier(
                        store,
                        configuration,
                        namespace,
                        readiness(9, "aa".repeat(32), 2))
                .requireActive(TIMEOUT).join())
                .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class)
                .hasRootCauseMessage("BookKeeper deletion activation broker readiness is stale");
        assertThatThrownBy(() -> new DefaultBookKeeperProtocolActivationVerifier(
                        store,
                        configuration,
                        namespace,
                        readiness(8, "66".repeat(32), 64))
                .requireActive(TIMEOUT).join())
                .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class)
                .hasRootCauseMessage(
                        "BookKeeper reader lease capacity cannot cover the broker set "
                                + "plus one rolling-restart overlap");

        assertThatThrownBy(() -> coordinator.activate(
                        configuration,
                        namespace,
                        BookKeeperProtocolActivationUpdate.publications(
                                8,
                                "66".repeat(32),
                                true,
                                true,
                                deletion.metadataVersion()),
                        TIMEOUT).join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("BookKeeper activation bits are monotonic");
    }

    private static BookKeeperBrokerReadinessProvider readiness(
            long epoch, String sha256, int brokerCount) {
        BookKeeperBrokerReadiness readiness = new BookKeeperBrokerReadiness(
                epoch,
                new Checksum(ChecksumType.SHA256, sha256),
                brokerCount);
        return new BookKeeperBrokerReadinessProvider() {
            @Override
            public CompletableFuture<BookKeeperBrokerReadiness>
                    requireBookKeeperPrimaryWalReadiness() {
                return CompletableFuture.completedFuture(readiness);
            }

            @Override
            public Optional<BookKeeperBrokerReadiness>
                    currentBookKeeperPrimaryWalReadiness() {
                return Optional.of(readiness);
            }
        };
    }

    private static final class NamespaceStore
            implements BookKeeperLedgerIdNamespaceReservationAdminStore {
        private BookKeeperLedgerIdNamespaceReservation current;

        @Override
        public CompletableFuture<Optional<BookKeeperLedgerIdNamespaceReservation>> read(
                String providerScopeSha256,
                int prefixBits,
                long prefixValue,
                Duration timeout) {
            return CompletableFuture.completedFuture(Optional.ofNullable(current));
        }

        @Override
        public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> create(
                BookKeeperLedgerIdNamespaceReservationValue value,
                Duration timeout) {
            if (current != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("exists"));
            }
            current = materialize(value, 0);
            return CompletableFuture.completedFuture(current);
        }

        @Override
        public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> compareAndSet(
                BookKeeperLedgerIdNamespaceReservationValue replacement,
                long expectedMetadataVersion,
                Duration timeout) {
            if (current == null || current.metadataVersion() != expectedMetadataVersion) {
                return CompletableFuture.failedFuture(new IllegalStateException("version"));
            }
            current = materialize(replacement, expectedMetadataVersion + 1);
            return CompletableFuture.completedFuture(current);
        }

        private static BookKeeperLedgerIdNamespaceReservation materialize(
                BookKeeperLedgerIdNamespaceReservationValue value, long version) {
            byte[] bytes = BookKeeperLedgerIdNamespaceReservationCodecV1.encode(value);
            return value.materialize(
                    BookKeeperLedgerIdNamespaceReservationKeys.key(
                            value.bookKeeperProviderScopeSha256(),
                            value.ledgerIdPrefixBits(),
                            value.ledgerIdPrefixValue()),
                    version,
                    sha(bytes));
        }
    }

    private static final class ActivationStore implements BookKeeperProtocolActivationStore {
        private final BookKeeperWalConfiguration configuration;
        private final BookKeeperLedgerIdNamespaceReservation namespace;
        private BookKeeperProtocolActivation current;

        private ActivationStore(
                BookKeeperWalConfiguration configuration,
                BookKeeperLedgerIdNamespaceReservation namespace) {
            this.configuration = configuration;
            this.namespace = namespace;
        }

        @Override
        public CompletableFuture<Optional<BookKeeperProtocolActivation>> read(
                BookKeeperWalConfiguration ignoredConfiguration,
                BookKeeperLedgerIdNamespaceReservation ignoredNamespace,
                Duration timeout) {
            return CompletableFuture.completedFuture(Optional.ofNullable(current));
        }

        @Override
        public CompletableFuture<BookKeeperProtocolActivation> create(
                BookKeeperProtocolActivationValue value,
                Duration timeout) {
            if (current != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("exists"));
            }
            current = materialize(value, 0);
            return CompletableFuture.completedFuture(current);
        }

        @Override
        public CompletableFuture<BookKeeperProtocolActivation> compareAndSet(
                BookKeeperProtocolActivationValue replacement,
                long expectedMetadataVersion,
                Duration timeout) {
            if (current == null || current.metadataVersion() != expectedMetadataVersion) {
                return CompletableFuture.failedFuture(new IllegalStateException("version"));
            }
            current = materialize(replacement, expectedMetadataVersion + 1);
            return CompletableFuture.completedFuture(current);
        }

        private BookKeeperProtocolActivation materialize(
                BookKeeperProtocolActivationValue value, long version) {
            byte[] bytes = BookKeeperProtocolActivationCodecV1.encode(value);
            return value.materialize(
                    BookKeeperProtocolActivationKeys.key(
                            configuration.clusterAlias(),
                            configuration.configurationBindingSha256().value(),
                            namespace.ledgerIdNamespaceSha256().value()),
                    version,
                    sha(bytes));
        }
    }

    private static Checksum sha(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    java.util.HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
