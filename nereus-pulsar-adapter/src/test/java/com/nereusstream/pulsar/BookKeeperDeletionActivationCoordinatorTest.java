/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadinessProvider;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerGcConfiguration;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceProvisioningCoordinator;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationAdminStore;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationKeys;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationStore;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationVerifier;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationValue;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationCodecV1;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationCoordinator;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationKeys;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationLifecycle;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationStore;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationUpdate;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationValue;
import com.nereusstream.bookkeeper.BookKeeperRootCoverageProof;
import com.nereusstream.bookkeeper.BookKeeperScopeCapabilityProof;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class BookKeeperDeletionActivationCoordinatorTest {
    private static final String ZERO = "0".repeat(64);
    private static final BookKeeperBrokerReadiness READY = new BookKeeperBrokerReadiness(
            11, sha('8'), 2);

    @Test
    void producesAndInstallsAllProofsInOneCasAndThenReadsIdempotently() {
        Fixture fixture = new Fixture(new FixedReadinessProvider(READY));

        BookKeeperDeletionActivationResult first = fixture.coordinator()
                .activate(new BookKeeperDeletionActivationRequest(
                        "rollout-0001", 7, Duration.ofSeconds(10)))
                .join();

        assertThat(first.newlyActivated()).isTrue();
        assertThat(first.activation().metadataVersion()).isEqualTo(8);
        assertThat(first.rootCoverageProofSha256()).isEqualTo(sha('a'));
        assertThat(first.streamCoverageProofSha256()).isEqualTo(sha('b'));
        assertThat(first.bookKeeperScopeProofSha256()).isEqualTo(sha('d'));
        assertThat(fixture.store.compareAndSetCalls).isOne();
        assertThat(fixture.rootCalls).hasValue(1);
        assertThat(fixture.streamCalls).hasValue(1);
        assertThat(fixture.scopeCalls).hasValue(1);

        BookKeeperDeletionActivationResult second = fixture.coordinator()
                .activate(new BookKeeperDeletionActivationRequest(
                        "rollout-0001", 7, Duration.ofSeconds(10)))
                .join();

        assertThat(second.activation()).isEqualTo(first.activation());
        assertThat(second.newlyActivated()).isFalse();
        assertThat(fixture.store.compareAndSetCalls).isOne();
        assertThat(fixture.rootCalls).hasValue(1);
        assertThat(fixture.streamCalls).hasValue(1);
        assertThat(fixture.scopeCalls).hasValue(1);
    }

    @Test
    void refusesToInstallWhenBrokerReadinessChangesDuringProofProduction() {
        AtomicInteger reads = new AtomicInteger();
        BookKeeperBrokerReadiness changed = new BookKeeperBrokerReadiness(
                12, sha('9'), 2);
        Fixture fixture = new Fixture(new BookKeeperBrokerReadinessProvider() {
            @Override
            public CompletableFuture<BookKeeperBrokerReadiness>
                    requireBookKeeperPrimaryWalReadiness() {
                return CompletableFuture.completedFuture(
                        reads.getAndIncrement() == 0 ? READY : changed);
            }

            @Override
            public Optional<BookKeeperBrokerReadiness>
                    currentBookKeeperPrimaryWalReadiness() {
                return Optional.of(changed);
            }
        });

        assertThatThrownBy(() -> fixture.coordinator()
                        .activate(new BookKeeperDeletionActivationRequest(
                                "rollout-0002", 7, Duration.ofSeconds(10)))
                        .join())
                .hasRootCauseMessage(
                        "BookKeeper broker readiness changed during deletion proof production");
        assertThat(fixture.store.compareAndSetCalls).isZero();
        assertThat(fixture.rootCalls).hasValue(0);
        assertThat(fixture.streamCalls).hasValue(0);
        assertThat(fixture.scopeCalls).hasValue(1);
    }

    @Test
    void rejectsAProducerProofBoundToAnotherBrokerSet() {
        Fixture fixture = new Fixture(new FixedReadinessProvider(READY));
        fixture.scopeReadiness = new BookKeeperBrokerReadiness(READY.brokerReadinessEpoch(), sha('9'), 2);

        assertThatThrownBy(() -> fixture.coordinator()
                        .activate(new BookKeeperDeletionActivationRequest(
                                "rollout-0003", 7, Duration.ofSeconds(10)))
                        .join())
                .hasRootCauseMessage("BookKeeper deletion proof does not match broker readiness");
        assertThat(fixture.store.compareAndSetCalls).isZero();
        assertThat(fixture.rootCalls).hasValue(0);
        assertThat(fixture.streamCalls).hasValue(0);
        assertThat(fixture.scopeCalls).hasValue(1);
    }

    @Test
    void publicAdministrationCannotInstallCallerSuppliedDeletionProofs() {
        Fixture fixture = new Fixture(new FixedReadinessProvider(READY));
        BookKeeperLedgerIdNamespaceReservationAdminStore namespaces =
                new BookKeeperLedgerIdNamespaceReservationAdminStore() {
                    @Override
                    public CompletableFuture<Optional<BookKeeperLedgerIdNamespaceReservation>> read(
                            String scope, int bits, long value, Duration timeout) {
                        return CompletableFuture.completedFuture(Optional.of(fixture.namespace));
                    }

                    @Override
                    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> create(
                            BookKeeperLedgerIdNamespaceReservationValue value,
                            Duration timeout) {
                        return CompletableFuture.failedFuture(new AssertionError("unexpected create"));
                    }

                    @Override
                    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> compareAndSet(
                            BookKeeperLedgerIdNamespaceReservationValue replacement,
                            long expectedMetadataVersion,
                            Duration timeout) {
                        return CompletableFuture.failedFuture(new AssertionError("unexpected CAS"));
                    }
                };
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        BookKeeperPrimaryWalAdministration administration =
                new BookKeeperPrimaryWalAdministration(
                        new NereusBookKeeperRuntimeConfiguration(
                                "deployment-1", fixture.configuration, gcConfiguration()),
                        new BookKeeperLedgerIdNamespaceReservationVerifier(
                                namespaces, "deployment-1"),
                        new BookKeeperLedgerIdNamespaceProvisioningCoordinator(namespaces, clock),
                        fixture.store,
                        new BookKeeperProtocolActivationCoordinator(fixture.store, clock),
                        Optional.of(fixture.coordinator()));
        BookKeeperProtocolActivationUpdate injected = new BookKeeperProtocolActivationUpdate(
                READY.brokerReadinessEpoch(),
                READY.brokerSetSha256().value(),
                true,
                true,
                true,
                sha('a').value(),
                sha('b').value(),
                sha('d').value(),
                7);

        assertThatThrownBy(() -> administration.activate(injected, Duration.ofSeconds(10)).join())
                .hasRootCauseMessage(
                        "ledger deletion must use activateDeletion so proof digests are producer-owned");
        assertThat(fixture.store.compareAndSetCalls).isZero();
    }

    private static final class Fixture {
        private final BookKeeperWalConfiguration configuration = configuration();
        private final BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        private final ActivationStore store = new ActivationStore(configuration, namespace);
        private final BookKeeperBrokerReadinessProvider readinessProvider;
        private final AtomicInteger rootCalls = new AtomicInteger();
        private final AtomicInteger streamCalls = new AtomicInteger();
        private final AtomicInteger scopeCalls = new AtomicInteger();
        private BookKeeperBrokerReadiness scopeReadiness = READY;

        private Fixture(BookKeeperBrokerReadinessProvider readinessProvider) {
            this.readinessProvider = readinessProvider;
        }

        private BookKeeperDeletionActivationCoordinator coordinator() {
            BookKeeperLedgerIdNamespaceReservationStore namespaces =
                    (scope, bits, value, timeout) -> CompletableFuture.completedFuture(
                            Optional.of(namespace));
            return new BookKeeperDeletionActivationCoordinator(
                    configuration,
                    gcConfiguration(),
                    namespace,
                    new BookKeeperLedgerIdNamespaceReservationVerifier(
                            namespaces, "deployment-1"),
                    readinessProvider,
                    store,
                    new BookKeeperProtocolActivationCoordinator(
                            store,
                            Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC)),
                    (readiness, timeout) -> {
                        rootCalls.incrementAndGet();
                        return CompletableFuture.completedFuture(new BookKeeperRootCoverageProof(
                                readiness.brokerReadinessEpoch(),
                                readiness.brokerSetSha256(),
                                256,
                                2,
                                1,
                                3,
                                4,
                                sha('a')));
                    },
                    (readiness, timeout) -> {
                        streamCalls.incrementAndGet();
                        return CompletableFuture.completedFuture(new BookKeeperStreamCoverageProof(
                                readiness.brokerReadinessEpoch(),
                                readiness.brokerSetSha256(),
                                64,
                                2,
                                1,
                                sha('b')));
                    },
                    request -> {
                        scopeCalls.incrementAndGet();
                        return CompletableFuture.completedFuture(new BookKeeperScopeCapabilityProof(
                                request.runId(),
                                scopeReadiness.brokerReadinessEpoch(),
                                scopeReadiness.brokerSetSha256(),
                                77,
                                sha('c'),
                                sha('d')));
                    });
        }
    }

    private static final class FixedReadinessProvider
            implements BookKeeperBrokerReadinessProvider {
        private final BookKeeperBrokerReadiness readiness;

        private FixedReadinessProvider(BookKeeperBrokerReadiness readiness) {
            this.readiness = readiness;
        }

        @Override
        public CompletableFuture<BookKeeperBrokerReadiness>
                requireBookKeeperPrimaryWalReadiness() {
            return CompletableFuture.completedFuture(readiness);
        }

        @Override
        public Optional<BookKeeperBrokerReadiness> currentBookKeeperPrimaryWalReadiness() {
            return Optional.of(readiness);
        }
    }

    private static final class ActivationStore implements BookKeeperProtocolActivationStore {
        private final BookKeeperWalConfiguration configuration;
        private final BookKeeperLedgerIdNamespaceReservation namespace;
        private BookKeeperProtocolActivation current;
        private int compareAndSetCalls;

        private ActivationStore(
                BookKeeperWalConfiguration configuration,
                BookKeeperLedgerIdNamespaceReservation namespace) {
            this.configuration = configuration;
            this.namespace = namespace;
            current = materialize(new BookKeeperProtocolActivationValue(
                    1,
                    BookKeeperProtocolActivationLifecycle.ACTIVE,
                    1,
                    configuration.clusterAlias(),
                    configuration.providerScopeSha256(),
                    READY.brokerReadinessEpoch(),
                    READY.brokerSetSha256().value(),
                    configuration.configurationBindingSha256().value(),
                    namespace.ledgerIdNamespaceSha256().value(),
                    true,
                    true,
                    true,
                    false,
                    ZERO,
                    ZERO,
                    ZERO,
                    100), 7);
        }

        @Override
        public CompletableFuture<Optional<BookKeeperProtocolActivation>> read(
                BookKeeperWalConfiguration ignoredConfiguration,
                BookKeeperLedgerIdNamespaceReservation ignoredNamespace,
                Duration timeout) {
            return CompletableFuture.completedFuture(Optional.of(current));
        }

        @Override
        public CompletableFuture<BookKeeperProtocolActivation> create(
                BookKeeperProtocolActivationValue value, Duration timeout) {
            return CompletableFuture.failedFuture(new IllegalStateException("already exists"));
        }

        @Override
        public CompletableFuture<BookKeeperProtocolActivation> compareAndSet(
                BookKeeperProtocolActivationValue replacement,
                long expectedMetadataVersion,
                Duration timeout) {
            compareAndSetCalls++;
            if (current.metadataVersion() != expectedMetadataVersion) {
                return CompletableFuture.failedFuture(new IllegalStateException("version mismatch"));
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

    private static BookKeeperLedgerIdNamespaceReservation namespace(
            BookKeeperWalConfiguration configuration) {
        return new BookKeeperLedgerIdNamespaceReservation(
                1,
                configuration.ledgerIdNamespaceReservationId(),
                "deployment-1",
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                configuration.ledgerIdPrefixBits(),
                configuration.ledgerIdPrefixValue(),
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                1,
                100,
                0,
                "e".repeat(64),
                3,
                sha('f'),
                BookKeeperLedgerIdNamespaceReservationKeys.key(
                        configuration.providerScopeSha256(),
                        configuration.ledgerIdPrefixBits(),
                        configuration.ledgerIdPrefixValue()));
    }

    private static BookKeeperLedgerGcConfiguration gcConfiguration() {
        return new BookKeeperLedgerGcConfiguration(
                1,
                Duration.ofSeconds(30),
                Duration.ofMinutes(3),
                Duration.ofDays(7),
                true,
                false);
    }

    private static BookKeeperWalConfiguration configuration() {
        return new BookKeeperWalConfiguration(
                "primary",
                "1".repeat(64),
                12,
                0x801,
                "reservation-1",
                3,
                3,
                2,
                BookKeeperDigestType.CRC32C,
                new BookKeeperSecretRef("secret://bookkeeper/password", "v7"),
                100_000,
                256L * 1024 * 1024,
                1_000,
                8,
                64,
                32,
                Duration.ofHours(1),
                1,
                8,
                64L * 1024 * 1024,
                Duration.ofSeconds(30),
                Duration.ofSeconds(20),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofMinutes(2),
                Duration.ofSeconds(30),
                Duration.ofMinutes(1),
                256);
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, String.valueOf(value).repeat(64));
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
