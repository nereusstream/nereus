/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class BookKeeperProtocolActivationCodecV1Test {
    @Test
    void roundTripsPreparedAndActiveValuesWithStableWireIds() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        BookKeeperProtocolActivationValue prepared =
                BookKeeperProtocolActivationValue.prepared(
                        configuration, namespace, 7, "44".repeat(32));

        byte[] encoded = BookKeeperProtocolActivationCodecV1.encode(prepared);
        assertThat(BookKeeperProtocolActivationCodecV1.decode(encoded)).isEqualTo(prepared);

        BookKeeperProtocolActivationValue active = active(
                prepared, true, true, false, "0".repeat(64));
        assertThat(BookKeeperProtocolActivationCodecV1.decode(
                        BookKeeperProtocolActivationCodecV1.encode(active)))
                .isEqualTo(active);
        assertThat(BookKeeperProtocolActivationLifecycle.PREPARED.wireId()).isEqualTo(1);
        assertThat(BookKeeperProtocolActivationLifecycle.ACTIVE.wireId()).isEqualTo(2);
    }

    @Test
    void rejectsMalformedNoncanonicalAndIllegalTransitionValues() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperProtocolActivationValue prepared =
                BookKeeperProtocolActivationValue.prepared(
                        configuration, namespace(configuration), 7, "44".repeat(32));
        byte[] encoded = BookKeeperProtocolActivationCodecV1.encode(prepared);

        assertThatThrownBy(() -> BookKeeperProtocolActivationCodecV1.decode(
                        Arrays.copyOf(encoded, encoded.length - 1)))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        assertThatThrownBy(() -> BookKeeperProtocolActivationCodecV1.decode(trailing))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BookKeeperProtocolActivationValue(
                        1,
                        BookKeeperProtocolActivationLifecycle.ACTIVE,
                        1,
                        prepared.clusterAlias(),
                        prepared.providerScopeSha256(),
                        7,
                        prepared.brokerReadinessSha256(),
                        prepared.configurationBindingSha256(),
                        prepared.ledgerIdNamespaceSha256(),
                        true,
                        true,
                        true,
                        true,
                        "55".repeat(32),
                        "0".repeat(64),
                        "77".repeat(32),
                        10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nonzero coverage proofs");

        BookKeeperProtocolActivationValue active = active(
                prepared, true, true, false, "0".repeat(64));
        BookKeeperProtocolActivationValue downgraded = active(
                prepared, false, false, false, "0".repeat(64));
        assertThatThrownBy(() -> BookKeeperProtocolActivationValue.requireValidReplacement(
                        active, downgraded))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("monotonic");
    }

    @Test
    void keyAndNbka1IdentityBindConfigurationNamespaceVersionAndBytes() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        BookKeeperProtocolActivationValue prepared =
                BookKeeperProtocolActivationValue.prepared(
                        configuration, namespace, 7, "44".repeat(32));
        String key = BookKeeperProtocolActivationKeys.key(
                configuration.clusterAlias(),
                configuration.configurationBindingSha256().value(),
                namespace.ledgerIdNamespaceSha256().value());
        BookKeeperProtocolActivation activation = prepared.materialize(
                key,
                9,
                new Checksum(ChecksumType.SHA256, "55".repeat(32)));

        assertThat(activation.activationRecordSha256().value()).hasSize(64);
        assertThatThrownBy(() -> BookKeeperProtocolActivationKeys.requireExact(
                        key + "-drift",
                        configuration.clusterAlias(),
                        configuration.configurationBindingSha256().value(),
                        namespace.ledgerIdNamespaceSha256().value()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static BookKeeperProtocolActivationValue active(
            BookKeeperProtocolActivationValue prepared,
            boolean async,
            boolean sync,
            boolean deletion,
            String proof) {
        return new BookKeeperProtocolActivationValue(
                1,
                BookKeeperProtocolActivationLifecycle.ACTIVE,
                1,
                prepared.clusterAlias(),
                prepared.providerScopeSha256(),
                prepared.brokerReadinessEpoch(),
                prepared.brokerReadinessSha256(),
                prepared.configurationBindingSha256(),
                prepared.ledgerIdNamespaceSha256(),
                true,
                async,
                sync,
                deletion,
                proof,
                proof,
                proof,
                10);
    }

    static BookKeeperLedgerIdNamespaceReservation namespace(
            BookKeeperWalConfiguration configuration) {
        BookKeeperLedgerIdNamespaceReservationValue value =
                new BookKeeperLedgerIdNamespaceReservationValue(
                        1,
                        configuration.ledgerIdNamespaceReservationId(),
                        "deployment-a",
                        configuration.clusterAlias(),
                        configuration.providerScopeSha256(),
                        configuration.ledgerIdPrefixBits(),
                        configuration.ledgerIdPrefixValue(),
                        BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                        1,
                        1,
                        0,
                        "22".repeat(32));
        return value.materialize(
                BookKeeperLedgerIdNamespaceReservationKeys.key(
                        configuration.providerScopeSha256(),
                        configuration.ledgerIdPrefixBits(),
                        configuration.ledgerIdPrefixValue()),
                3,
                new Checksum(ChecksumType.SHA256, "33".repeat(32)));
    }
}
