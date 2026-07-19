/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class BookKeeperLedgerIdNamespaceReservationStoreContractTest {
    @Test
    void acceptsOnlyExactActiveProvisionedAuthority() {
        BookKeeperWalConfiguration config = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerIdNamespaceReservation active = reservation(
                config, BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE, 0);
        var verifier = new BookKeeperLedgerIdNamespaceReservationVerifier(
                (scope, bits, value, timeout) -> CompletableFuture.completedFuture(Optional.of(active)),
                "deployment-a");

        var verified = verifier.requireActive(config, Duration.ofSeconds(1)).join();
        assertThat(verified).isEqualTo(active);
        assertThat(verified.ledgerIdNamespaceSha256().value()).hasSize(64);

        BookKeeperLedgerIdNamespaceReservation revoked = reservation(
                config, BookKeeperLedgerIdNamespaceReservation.Lifecycle.REVOKED, 11);
        var revokedVerifier = new BookKeeperLedgerIdNamespaceReservationVerifier(
                (scope, bits, value, timeout) -> CompletableFuture.completedFuture(Optional.of(revoked)),
                "deployment-a");
        assertThatThrownBy(() -> revokedVerifier.requireActive(config, Duration.ofSeconds(1)).join())
                .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);
    }

    private static BookKeeperLedgerIdNamespaceReservation reservation(
            BookKeeperWalConfiguration config,
            BookKeeperLedgerIdNamespaceReservation.Lifecycle lifecycle,
            long revokedAt) {
        return new BookKeeperLedgerIdNamespaceReservation(
                1, config.ledgerIdNamespaceReservationId(), "deployment-a", config.clusterAlias(),
                config.providerScopeSha256(), config.ledgerIdPrefixBits(), config.ledgerIdPrefixValue(),
                lifecycle, lifecycle == BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE ? 1 : 2,
                10, revokedAt, "22".repeat(32), 7,
                new Checksum(ChecksumType.SHA256, "33".repeat(32)),
                "/nereus/bookkeeper/provider-scope/prefix");
    }
}
