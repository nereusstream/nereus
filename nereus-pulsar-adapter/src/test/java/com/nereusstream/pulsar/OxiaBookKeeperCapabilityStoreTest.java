/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationValue;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationLifecycle;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationValue;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.metadata.oxia.CapabilityMetadataClient;
import com.nereusstream.metadata.oxia.CapabilityMetadataValue;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class OxiaBookKeeperCapabilityStoreTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    @Test
    void namespaceCreateAndRevokeRecoverAppliedResponseLoss() {
        FakeCapabilityClient client = new FakeCapabilityClient();
        OxiaBookKeeperLedgerIdNamespaceReservationStore store =
                new OxiaBookKeeperLedgerIdNamespaceReservationStore(client);
        BookKeeperWalConfiguration configuration = configuration();
        BookKeeperLedgerIdNamespaceReservationValue active = namespaceValue(
                configuration,
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                1,
                0,
                "22".repeat(32));

        client.failNextAfterApply = true;
        BookKeeperLedgerIdNamespaceReservation created = store.create(active, TIMEOUT).join();
        assertThat(created.metadataVersion()).isZero();

        BookKeeperLedgerIdNamespaceReservationValue revoked = namespaceValue(
                configuration,
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.REVOKED,
                2,
                200,
                "33".repeat(32));
        client.failNextAfterApply = true;
        BookKeeperLedgerIdNamespaceReservation updated = store.compareAndSet(
                revoked, created.metadataVersion(), TIMEOUT).join();
        assertThat(updated.metadataVersion()).isEqualTo(1);
        assertThat(store.read(
                        configuration.providerScopeSha256(),
                        configuration.ledgerIdPrefixBits(),
                        configuration.ledgerIdPrefixValue(),
                        TIMEOUT).join().orElseThrow())
                .isEqualTo(updated);
    }

    @Test
    void activationCreateAndCasRecoverAppliedResponseLossButNotForeignConflict() {
        FakeCapabilityClient client = new FakeCapabilityClient();
        OxiaBookKeeperProtocolActivationStore store =
                new OxiaBookKeeperProtocolActivationStore(client);
        BookKeeperWalConfiguration configuration = configuration();
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        BookKeeperProtocolActivationValue prepared = BookKeeperProtocolActivationValue.prepared(
                configuration, namespace, 3, "44".repeat(32));

        client.failNextAfterApply = true;
        var created = store.create(prepared, TIMEOUT).join();
        assertThat(created.metadataVersion()).isZero();

        BookKeeperProtocolActivationValue active = new BookKeeperProtocolActivationValue(
                1,
                BookKeeperProtocolActivationLifecycle.ACTIVE,
                1,
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                3,
                "44".repeat(32),
                configuration.configurationBindingSha256().value(),
                namespace.ledgerIdNamespaceSha256().value(),
                true,
                true,
                true,
                false,
                "0".repeat(64),
                "0".repeat(64),
                "0".repeat(64),
                100);
        client.failNextAfterApply = true;
        var updated = store.compareAndSet(active, created.metadataVersion(), TIMEOUT).join();
        assertThat(updated.metadataVersion()).isEqualTo(1);
        assertThat(updated.supportsAllPublications()).isTrue();

        FakeCapabilityClient conflicting = new FakeCapabilityClient();
        OxiaBookKeeperProtocolActivationStore conflictStore =
                new OxiaBookKeeperProtocolActivationStore(conflicting);
        conflictStore.create(prepared, TIMEOUT).join();
        BookKeeperProtocolActivationValue drifted = BookKeeperProtocolActivationValue.prepared(
                configuration, namespace, 4, "55".repeat(32));
        assertThatThrownBy(() -> conflictStore.create(drifted, TIMEOUT).join())
                .hasRootCauseMessage("condition failed");
    }

    private static BookKeeperLedgerIdNamespaceReservation namespace(
            BookKeeperWalConfiguration configuration) {
        FakeCapabilityClient client = new FakeCapabilityClient();
        return new OxiaBookKeeperLedgerIdNamespaceReservationStore(client)
                .create(namespaceValue(
                        configuration,
                        BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                        1,
                        0,
                        "22".repeat(32)), TIMEOUT)
                .join();
    }

    private static BookKeeperLedgerIdNamespaceReservationValue namespaceValue(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation.Lifecycle lifecycle,
            long epoch,
            long revokedAt,
            String evidence) {
        return new BookKeeperLedgerIdNamespaceReservationValue(
                1,
                configuration.ledgerIdNamespaceReservationId(),
                "deployment-a",
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                configuration.ledgerIdPrefixBits(),
                configuration.ledgerIdPrefixValue(),
                lifecycle,
                epoch,
                100,
                revokedAt,
                evidence);
    }

    private static BookKeeperWalConfiguration configuration() {
        return new BookKeeperWalConfiguration(
                "primary",
                "11".repeat(32),
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

    private static final class FakeCapabilityClient implements CapabilityMetadataClient {
        private final Map<String, CapabilityMetadataValue> values = new HashMap<>();
        private boolean failNextAfterApply;

        @Override
        public CompletableFuture<Optional<CapabilityMetadataValue>> get(
                String key, String partitionKey) {
            return CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        @Override
        public CompletableFuture<CapabilityMetadataValue> putIfAbsent(
                String key, byte[] value, String partitionKey) {
            if (values.containsKey(key)) {
                return CompletableFuture.failedFuture(new IllegalStateException("condition failed"));
            }
            return applied(key, value, 0);
        }

        @Override
        public CompletableFuture<CapabilityMetadataValue> putIfVersion(
                String key,
                byte[] value,
                long expectedVersion,
                String partitionKey) {
            CapabilityMetadataValue current = values.get(key);
            if (current == null || current.version() != expectedVersion) {
                return CompletableFuture.failedFuture(new IllegalStateException("condition failed"));
            }
            return applied(key, value, expectedVersion + 1);
        }

        private CompletableFuture<CapabilityMetadataValue> applied(
                String key, byte[] value, long version) {
            CapabilityMetadataValue stored = new CapabilityMetadataValue(key, value, version);
            values.put(key, stored);
            if (failNextAfterApply) {
                failNextAfterApply = false;
                return CompletableFuture.failedFuture(new IllegalStateException("response lost"));
            }
            return CompletableFuture.completedFuture(stored);
        }
    }
}
