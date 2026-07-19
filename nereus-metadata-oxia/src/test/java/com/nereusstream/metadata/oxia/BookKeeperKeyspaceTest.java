/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StreamId;
import org.junit.jupiter.api.Test;

class BookKeeperKeyspaceTest {
    private static final String SCOPE = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private final BookKeeperKeyspace keys = new BookKeeperKeyspace("cluster/a", 100, 8, 32, 64);

    @Test
    void strictlyInvertsEveryWriterPrefix() {
        StreamId stream = new StreamId("tenant/ns/topic/0");
        assertThat(keys.writerStateKey(stream)).contains("/streams/b32-").endsWith("/bookkeeper/v1/writer-state");
        assertThat(keys.allocationKey(stream, "allocation/1")).contains("/allocations/b32-");
        assertThat(keys.appendReservationKey(stream, "reservation/1")).contains("/append-reservations/b32-");

        String root = keys.ledgerRootKey(SCOPE, 123);
        assertThat(keys.parseRootKey(root, SCOPE, 123).ledgerId()).isEqualTo(123);

        String protection = keys.protectionKey(SCOPE, 123, 9, 7);
        assertThat(keys.parseProtectionKey(protection, SCOPE, 123))
                .extracting(BookKeeperKeyspace.ProtectionKeyIdentity::rangeSlot,
                        BookKeeperKeyspace.ProtectionKeyIdentity::protectionSlot)
                .containsExactly(9, 7);

        String reader = keys.readerLeaseKey(SCOPE, 123, 31);
        assertThat(keys.parseReaderLeaseKey(reader, SCOPE, 123).readerSlot()).isEqualTo(31);

        for (int slot = 0; slot < 64; slot++) {
            String key = keys.allocationSlotKey(slot);
            assertThat(keys.parseAllocationSlotKey(key).slot()).isEqualTo(slot);
        }
    }

    @Test
    void rejectsWrongClusterShardDepthAndNoncanonicalComponents() {
        String protection = keys.protectionKey(SCOPE, 123, 9, 7);
        assertThatThrownBy(() -> keys.parseProtectionKey(protection.replace("/00009/", "/0009/"), SCOPE, 123))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> keys.parseProtectionKey(protection + "/extra", SCOPE, 123))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> keys.parseReaderLeaseKey(keys.readerLeaseKey(SCOPE, 123, 0), SCOPE, 124))
                .isInstanceOf(IllegalArgumentException.class);
        String slot = keys.allocationSlotKey(17);
        assertThatThrownBy(() -> keys.parseAllocationSlotKey(slot.replace("/01/", "/02/")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> keys.parseRootKey(keys.ledgerRootKey(SCOPE, 123)
                        .replace("/cluster", "/other"), SCOPE, 123))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> keys.protectionKey(SCOPE, 123, 100, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
