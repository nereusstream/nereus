/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.FakeCursorMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class CursorSnapshotReferenceDomainTest {
    private static final String CLUSTER = "cluster/a";
    private static final String NAME = "tenant/ns/persistent/cursor-domain";
    private static final String OWNER_ONE = "00112233445566778899aabbccddeeff";
    private static final String OWNER_TWO = "ffeeddccbbaa99887766554433221100";
    private static final String ATTEMPT = "0123456789abcdeffedcba9876543210";
    private static final String SNAPSHOT_ID = "11112222333344445555666677778888";
    private static final String LIVE_KEY = "cursor-snapshots/live.ncs";
    private static final GcReferenceDomainConfig CONFIG =
            new GcReferenceDomainConfig(1, 100, 100);

    @Test
    void completePagedRootScanVetoesLiveReferenceAndRevalidatesEveryAuthority() {
        try (var store = new FakeCursorMetadataStore()) {
            ManagedLedgerProjectionIdentity projection = projection();
            var retention = store.createRetention(
                    CLUSTER, activeRetention(projection, OWNER_ONE, 1)).join();
            store.createCursor(CLUSTER, cursor(projection, "subscription-a", Optional.of(snapshot()))).join();
            store.createCursor(CLUSTER, cursor(projection, "subscription-b", Optional.empty())).join();
            var domain = new CursorSnapshotReferenceDomain(CLUSTER, store, CONFIG);

            var live = domain.snapshot(query(LIVE_KEY)).join();
            assertThat(live.complete()).isTrue();
            assertThat(live.veto()).isTrue();
            assertThat(live.authorities()).hasSize(3);
            assertThat(live.references()).singleElement()
                    .satisfies(reference -> {
                        assertThat(reference.referenceType()).isEqualTo("cursor-snapshot-root");
                        assertThat(reference.ownerKey()).contains("/by-hash/");
                    });

            GcReferenceQuery oldQuery = query("cursor-snapshots/old.ncs");
            var clear = domain.snapshot(oldQuery).join();
            assertThat(clear.complete()).isTrue();
            assertThat(clear.veto()).isFalse();
            assertThat(clear.references()).isEmpty();
            assertThat(domain.stillMatches(oldQuery, clear).join()).isTrue();

            store.compareAndSetRetention(
                    CLUSTER,
                    activeRetention(projection, OWNER_TWO, 2),
                    retention.metadataVersion()).join();
            assertThat(domain.stillMatches(oldQuery, clear).join()).isFalse();
        }
    }

    @Test
    void pendingRetentionAndSnapshotBoundsFailClosed() {
        try (var store = new FakeCursorMetadataStore()) {
            ManagedLedgerProjectionIdentity projection = projection();
            var active = store.createRetention(
                    CLUSTER, activeRetention(projection, OWNER_ONE, 1)).join();
            store.createCursor(CLUSTER, cursor(projection, "subscription-a", Optional.empty())).join();
            store.compareAndSetRetention(
                    CLUSTER,
                    trimPendingRetention(projection),
                    active.metadataVersion()).join();

            var domain = new CursorSnapshotReferenceDomain(CLUSTER, store, CONFIG);
            var pending = domain.snapshot(query("cursor-snapshots/old.ncs")).join();
            assertThat(pending.complete()).isTrue();
            assertThat(pending.veto()).isTrue();

            var bounded = new CursorSnapshotReferenceDomain(
                    CLUSTER,
                    store,
                    new GcReferenceDomainConfig(1, 1, 100))
                    .snapshot(query("cursor-snapshots/old.ncs")).join();
            assertThat(bounded.complete()).isFalse();
            assertThat(bounded.veto()).isTrue();
            assertThat(bounded.authorityCount()).isEqualTo(2);
            assertThat(bounded.authorities()).hasSize(1);
        }
    }

    @Test
    void ownerlessQueryDoesNotTurnAffectedStreamInventoryIntoGlobalProof() {
        try (var store = new FakeCursorMetadataStore()) {
            var domain = new CursorSnapshotReferenceDomain(CLUSTER, store, CONFIG);
            var ownerless = domain.snapshot(GcReferenceQuery.create(
                    GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                    object("cursor-snapshots/orphan.ncs"),
                    List.of(),
                    sha256('2'))).join();
            assertThat(ownerless.complete()).isFalse();
            assertThat(ownerless.veto()).isTrue();
            assertThat(ownerless.authorities()).isEmpty();
        }
    }

    @Test
    void ownerlessGlobalScopePagesTheExactCursorNamespaceAndDetectsDrift() {
        try (var store = new FakeCursorMetadataStore()) {
            StreamId streamId = new StreamId(projection().streamId());
            var domain = new CursorSnapshotReferenceDomain(
                    CLUSTER,
                    store,
                    CONFIG,
                    ManagedLedgerGlobalScopeTestSupport.complete(streamId));
            GcReferenceQuery ownerless = GcReferenceQuery.create(
                    GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                    object("cursor-snapshots/orphan.ncs"),
                    List.of(),
                    sha256('2'));

            var clear = domain.snapshot(ownerless).join();

            assertThat(clear.complete()).isTrue();
            assertThat(clear.veto()).isFalse();
            assertThat(clear.authorities()).hasSize(2);
            assertThat(clear.authorities())
                    .extracting(value -> value.authorityKey())
                    .contains("/global/reference-scope");
            assertThat(domain.stillMatches(ownerless, clear).join()).isTrue();

            store.createCursor(
                    CLUSTER,
                    cursor(
                            projection(),
                            "subscription-global-drift",
                            Optional.empty())).join();
            assertThat(domain.stillMatches(ownerless, clear).join()).isFalse();
        }
    }

    private static GcReferenceQuery query(String objectKey) {
        return GcReferenceQuery.create(
                GcReferenceQueryKind.CURSOR_SNAPSHOT_CANDIDATE,
                object(objectKey),
                List.of(new StreamId(projection().streamId())),
                sha256('1'));
    }

    private static PhysicalObjectIdentity object(String objectKey) {
        return PhysicalObjectIdentity.create(
                new ObjectKey(objectKey),
                Optional.empty(),
                PhysicalObjectKind.CURSOR_SNAPSHOT,
                8,
                new Checksum(ChecksumType.CRC32C, "00000000"),
                Optional.empty(),
                Optional.empty());
    }

    private static ManagedLedgerProjectionIdentity projection() {
        return new ManagedLedgerProjectionIdentity(
                3,
                1,
                ManagedLedgerProjectionNames.streamId(NAME, 1).value(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID);
    }

    private static CursorRetentionRecord activeRetention(
            ManagedLedgerProjectionIdentity projection,
            String owner,
            long sequence) {
        return new CursorRetentionRecord(
                0,
                projection,
                owner,
                CursorRetentionLifecycle.ACTIVE,
                sequence,
                0,
                0,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                sequence);
    }

    private static CursorRetentionRecord trimPendingRetention(
            ManagedLedgerProjectionIdentity projection) {
        return new CursorRetentionRecord(
                0,
                projection,
                OWNER_ONE,
                CursorRetentionLifecycle.TRIM_PENDING,
                2,
                1,
                0,
                Optional.empty(),
                Optional.of(ATTEMPT),
                OptionalLong.of(1),
                Optional.of("nereus-cursor-retention/" + ATTEMPT + ":test"),
                2);
    }

    private static CursorStateRecord cursor(
            ManagedLedgerProjectionIdentity projection,
            String name,
            Optional<CursorSnapshotReferenceRecord> snapshot) {
        return new CursorStateRecord(
                0,
                projection,
                OWNER_ONE,
                name,
                CursorNames.cursorNameHash(name),
                1,
                CursorRecordLifecycle.ACTIVE,
                1,
                1,
                ATTEMPT,
                0,
                snapshot,
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                1,
                1,
                OptionalLong.empty());
    }

    private static CursorSnapshotReferenceRecord snapshot() {
        return new CursorSnapshotReferenceRecord(
                LIVE_KEY,
                SNAPSHOT_ID,
                1,
                1,
                0,
                8,
                ChecksumType.CRC32C.name(),
                "00000000",
                123,
                1,
                1);
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, String.valueOf(value).repeat(64));
    }
}
