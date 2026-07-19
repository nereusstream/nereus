/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Random;
import org.junit.jupiter.api.Test;

class BookKeeperWalConfigurationTest {
    @Test
    void bindsNonSecretSemantics() {
        BookKeeperWalConfiguration config = BookKeeperTestConfigurations.valid();
        assertThat(config.configurationBindingSha256().value()).hasSize(64);
        assertThat(config.toString()).doesNotContain("password bytes").doesNotContain("super-secret");
        assertThat(config.passwordRef().toString()).doesNotContain(config.passwordRef().reference());

        BookKeeperLedgerIdNamespace namespace = config.ledgerIdNamespace();
        for (int seed = 0; seed < 1_000; seed++) {
            long candidate = namespace.candidate(new Random(seed));
            assertThat(candidate).isPositive();
            assertThat(namespace.contains(candidate)).isTrue();
            assertThat(candidate >>> namespace.suffixBits()).isEqualTo(config.ledgerIdPrefixValue());
        }
        assertThat(namespace.contains(0)).isFalse();
        assertThat(namespace.contains(Long.MAX_VALUE)).isFalse();
    }

    @Test
    void rejectsUnsafeBoundsAndDeletionDefaultsClosed() {
        BookKeeperWalConfiguration valid = BookKeeperTestConfigurations.valid();
        assertThatThrownBy(() -> new BookKeeperLedgerIdNamespace(7, 0x40))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BookKeeperLedgerIdNamespace(12, 0x7ff))
                .isInstanceOf(IllegalArgumentException.class);
        BookKeeperLedgerGcConfiguration gc = BookKeeperLedgerGcConfiguration.safeDefault();
        assertThat(gc.enabled()).isFalse();
        assertThat(gc.dryRun()).isTrue();
        gc.validateAgainst(valid);
    }
}
