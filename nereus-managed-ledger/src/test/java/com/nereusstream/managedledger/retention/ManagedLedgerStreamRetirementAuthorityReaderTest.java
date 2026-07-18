/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationProjectionRefV1;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.FakeCursorMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ManagedLedgerStreamRetirementAuthorityReaderTest {
    private static final String CLUSTER = "cluster/retirement";
    private static final String NAME = "tenant/ns/persistent/retirement";
    private static final String OWNER = "00112233445566778899aabbccddeeff";
    private static final String ATTEMPT = "0123456789abcdeffedcba9876543210";

    @Test
    void exactDeletedCursorInventoryIsReferenceFreeAndRecapturable() {
        try (var store = new FakeCursorMetadataStore()) {
            LiveProjectionSubject subject = subject();
            var reader = reader(store, 100);

            var empty = reader.capture(subject).join();
            assertThat(empty.complete()).isTrue();
            assertThat(empty.referenceFree()).isTrue();
            assertThat(empty.authorities()).hasSize(1);

            store.createRetention(CLUSTER, retention()).join();
            var active = store.createCursor(
                    CLUSTER, cursor(CursorRecordLifecycle.ACTIVE, 1)).join();
            var blocked = reader.capture(subject).join();
            assertThat(blocked.complete()).isTrue();
            assertThat(blocked.referenceFree()).isFalse();
            assertThat(blocked.liveReferenceCount()).isOne();

            store.compareAndSetCursor(
                    CLUSTER,
                    cursor(CursorRecordLifecycle.DELETED, 2),
                    active.metadataVersion()).join();
            var clear = reader.capture(subject).join();
            assertThat(clear.complete()).isTrue();
            assertThat(clear.referenceFree()).isTrue();
            assertThat(clear.authorities()).hasSize(2);
            assertThat(reader.capture(subject).join()).isEqualTo(clear);
        }
    }

    @Test
    void authorityLimitAndTransitionalRetentionFailClosed() {
        try (var store = new FakeCursorMetadataStore()) {
            var created = store.createRetention(CLUSTER, retention()).join();
            store.createCursor(
                    CLUSTER, cursor(CursorRecordLifecycle.DELETED, 2)).join();

            var bounded = reader(store, 1).capture(subject()).join();
            assertThat(bounded.complete()).isFalse();
            assertThat(bounded.referenceFree()).isFalse();
            assertThat(bounded.authorityCount()).isEqualTo(2);
            assertThat(bounded.authorities()).hasSize(1);

            CursorRetentionRecord pending = new CursorRetentionRecord(
                    0,
                    projection(),
                    OWNER,
                    CursorRetentionLifecycle.TRIM_PENDING,
                    2,
                    1,
                    0,
                    Optional.empty(),
                    Optional.of(ATTEMPT),
                    OptionalLong.of(1),
                    Optional.of("nereus-cursor-retention/" + ATTEMPT + ":test"),
                    3);
            store.compareAndSetRetention(
                    CLUSTER, pending, created.metadataVersion()).join();
            var transitional = reader(store, 100).capture(subject()).join();
            assertThat(transitional.complete()).isTrue();
            assertThat(transitional.referenceFree()).isFalse();
        }
    }

    private static ManagedLedgerStreamRetirementAuthorityReader reader(
            FakeCursorMetadataStore store,
            int maxAuthorities) {
        return new ManagedLedgerStreamRetirementAuthorityReader(
                CLUSTER,
                store,
                new GcReferenceDomainConfig(1, maxAuthorities, 100));
    }

    private static LiveProjectionSubject subject() {
        ManagedLedgerGenerationProjectionRefV1 ref =
                new ManagedLedgerGenerationProjectionRefV1(NAME, projection());
        return new LiveProjectionSubject(
                new com.nereusstream.api.StreamId(projection().streamId()),
                ref.toProjectionRef(),
                ref.projectionIdentitySha256());
    }

    private static ManagedLedgerProjectionIdentity projection() {
        return new ManagedLedgerProjectionIdentity(
                3,
                1,
                ManagedLedgerProjectionNames.streamId(NAME, 1).value(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID);
    }

    private static CursorRetentionRecord retention() {
        return new CursorRetentionRecord(
                0,
                projection(),
                OWNER,
                CursorRetentionLifecycle.ACTIVE,
                1,
                0,
                0,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                1);
    }

    private static CursorStateRecord cursor(
            CursorRecordLifecycle lifecycle,
            long sequence) {
        String name = "subscription-a";
        boolean deleted = lifecycle == CursorRecordLifecycle.DELETED;
        return new CursorStateRecord(
                0,
                projection(),
                OWNER,
                name,
                CursorNames.cursorNameHash(name),
                1,
                lifecycle,
                sequence,
                sequence,
                ATTEMPT,
                0,
                Optional.empty(),
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                1,
                sequence,
                deleted ? OptionalLong.of(sequence) : OptionalLong.empty());
    }
}
