/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class NereusManagedLedgerOwnershipGuardTest {
    @Test
    void checkedGuardAcceptsOnlyExplicitTrue() {
        AtomicInteger checks = new AtomicInteger();
        NereusManagedLedgerOwnershipGuard guard = NereusManagedLedgerOwnershipGuard.checked(
                () -> {
                    checks.incrementAndGet();
                    return CompletableFuture.completedFuture(true);
                },
                Duration.ofSeconds(1));

        guard.requireOwned("before claim").join();
        guard.requireOwned("before publication").join();

        assertThat(checks).hasValue(2);
        assertThat(guard.isTrustedDirect()).isFalse();
    }

    @Test
    void checkedGuardMapsEveryNegativeShapeToFenced() {
        assertFenced(() -> CompletableFuture.completedFuture(false));
        assertFenced(() -> CompletableFuture.completedFuture(null));
        assertFenced(() -> CompletableFuture.failedFuture(new IllegalStateException("checker failed")));
        assertFenced(() -> null);
        assertFenced(() -> {
            throw new IllegalStateException("supplier failed");
        });
    }

    @Test
    void timeoutDoesNotCompleteOrCancelTheBrokerFuture() {
        CompletableFuture<Boolean> brokerFuture = new CompletableFuture<>();
        NereusManagedLedgerOwnershipGuard guard = NereusManagedLedgerOwnershipGuard.checked(
                () -> brokerFuture,
                Duration.ofMillis(10));

        assertThatThrownBy(() -> guard.requireOwned("timed check").join())
                .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);

        assertThat(brokerFuture).isNotDone();
    }

    @Test
    void trustedDirectIsNamedAndDoesNotInvokeAnExternalChecker() {
        NereusManagedLedgerOwnershipGuard guard =
                NereusManagedLedgerOwnershipGuard.trustedDirect(Duration.ofSeconds(1));

        guard.requireOwned("embedded open").join();

        assertThat(guard.isTrustedDirect()).isTrue();
    }

    private static void assertFenced(
            java.util.function.Supplier<CompletableFuture<Boolean>> checker) {
        NereusManagedLedgerOwnershipGuard guard = NereusManagedLedgerOwnershipGuard.checked(
                checker,
                Duration.ofSeconds(1));
        assertThatThrownBy(() -> guard.requireOwned("test").join())
                .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
    }
}
