/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class BookKeeperLedgerIdNamespaceReservationCodecV1Test {
    private static final String SCOPE = "ab".repeat(32);

    @Test
    void roundTripsExactProvisionedValueAndMaterializesNbln1Identity() {
        BookKeeperLedgerIdNamespaceReservationValue value = active();
        byte[] encoded = BookKeeperLedgerIdNamespaceReservationCodecV1.encode(value);

        assertThat(BookKeeperLedgerIdNamespaceReservationCodecV1.decode(encoded)).isEqualTo(value);
        String key = BookKeeperLedgerIdNamespaceReservationKeys.key(SCOPE, 12, 0x801);
        BookKeeperLedgerIdNamespaceReservation materialized = value.materialize(
                key,
                7,
                new Checksum(ChecksumType.SHA256, "22".repeat(32)));
        assertThat(materialized.canonicalKey()).isEqualTo(key);
        assertThat(materialized.metadataVersion()).isEqualTo(7);
        assertThat(materialized.ledgerIdNamespaceSha256().value()).hasSize(64);
    }

    @Test
    void rejectsNonCanonicalKeyScopeAndTrailingBytes() {
        assertThatThrownBy(() -> BookKeeperLedgerIdNamespaceReservationKeys.key(
                        SCOPE.toUpperCase(),
                        12,
                        0x801))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lowercase");
        byte[] encoded = BookKeeperLedgerIdNamespaceReservationCodecV1.encode(active());
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        assertThatThrownBy(() -> BookKeeperLedgerIdNamespaceReservationCodecV1.decode(trailing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing");
    }

    private static BookKeeperLedgerIdNamespaceReservationValue active() {
        return new BookKeeperLedgerIdNamespaceReservationValue(
                1,
                "reservation-1",
                "deployment-1",
                "cluster-1",
                SCOPE,
                12,
                0x801,
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                1,
                100,
                0,
                "33".repeat(32));
    }
}
