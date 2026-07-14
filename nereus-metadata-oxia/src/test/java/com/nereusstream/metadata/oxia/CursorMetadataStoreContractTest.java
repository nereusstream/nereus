/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.codec.F3MetadataCodecs;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CursorMetadataStoreContractTest {
    private static final String CLUSTER = "cluster/a";
    private static final String TOPIC = "persistent://tenant/ns/cursor-store";
    private static final String OWNER = "00112233445566778899aabbccddeeff";
    private static final String ATTEMPT = "102132435465768798a9bacbdcedfe0f";

    @Test
    void absentCreateExactCasAndRestartUseDecodedValuesAndSeparateVersions() {
        FakeCursorMetadataStore.DurableState state = new FakeCursorMetadataStore.DurableState();
        VersionedCursorState created;
        try (FakeCursorMetadataStore store = new FakeCursorMetadataStore(state, config(2))) {
            CursorStateRecord initial = cursor("sub-a", 1, 1);
            created = store.createCursor(CLUSTER, initial).join();
            assertThat(created.value()).isEqualTo(initial);
            assertThat(created.value().metadataVersion()).isZero();
            assertThat(created.metadataVersion()).isNotNegative();
            long initialMetadataVersion = created.metadataVersion();
            assertThat(store.getCursor(CLUSTER, streamId(), "sub-a").join())
                    .contains(created);
            assertConditionFailure(() -> store.createCursor(CLUSTER, initial).join());

            CursorStateRecord next = cursor("sub-a", 1, 2);
            VersionedCursorState replaced = store.compareAndSetCursor(
                    CLUSTER, next, initialMetadataVersion).join();
            assertThat(replaced.value()).isEqualTo(next);
            assertThat(replaced.metadataVersion()).isGreaterThan(initialMetadataVersion);
            assertConditionFailure(() -> store.compareAndSetCursor(
                    CLUSTER, cursor("sub-a", 1, 3), initialMetadataVersion).join());
            created = replaced;
        }
        try (FakeCursorMetadataStore restarted = new FakeCursorMetadataStore(state, config(2))) {
            assertThat(restarted.getCursor(CLUSTER, streamId(), "sub-a").join())
                    .contains(created);
        }
    }

    @Test
    void pageTokensAreStableOrderedOpaqueAndScopeBound() {
        try (FakeCursorMetadataStore store = new FakeCursorMetadataStore(
                new FakeCursorMetadataStore.DurableState(), config(2))) {
            for (String name : List.of("sub-c", "sub-a", "sub-b")) {
                store.createCursor(CLUSTER, cursor(name, 1, 1)).join();
            }
            List<VersionedCursorState> all = new ArrayList<>();
            Optional<CursorScanToken> continuation = Optional.empty();
            do {
                CursorScanPage page = store.scanCursors(CLUSTER, streamId(), continuation, 2).join();
                all.addAll(page.records());
                continuation = page.continuation();
            } while (continuation.isPresent());
            assertThat(all).hasSize(3);
            assertThat(all.stream().map(value -> value.value().cursorNameHash()).toList()).isSorted();

            CursorScanToken otherScope = store.scanCursors(
                    CLUSTER, streamId(), Optional.empty(), 1).join().continuation().orElseThrow();
            StreamId another = ManagedLedgerProjectionNames.streamId(TOPIC + "-other", 1);
            assertThatThrownBy(() -> store.scanCursors(
                    CLUSTER, another, Optional.of(otherScope), 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("another scope");
            assertThatThrownBy(() -> store.scanCursors(
                    CLUSTER, streamId(), Optional.empty(), 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void watchesAreInvalidationOnlyAndMayBeDisabledWithoutChangingAuthoritativeReads() {
        FakeCursorMetadataStore.DurableState state = new FakeCursorMetadataStore.DurableState();
        AtomicInteger invalidations = new AtomicInteger();
        try (FakeCursorMetadataStore store = new FakeCursorMetadataStore(state, config(2));
                WatchRegistration ignored = store.watchStreamCursors(
                        CLUSTER, streamId(), invalidations::incrementAndGet)) {
            store.createCursor(CLUSTER, cursor("sub-a", 1, 1)).join();
            assertThat(invalidations).hasValue(1);
            state.setWatchDeliveryEnabled(false);
            store.createCursor(CLUSTER, cursor("sub-b", 1, 1)).join();
            assertThat(invalidations).hasValue(1);
            assertThat(store.getCursor(CLUSTER, streamId(), "sub-b").join()).isPresent();
        }
    }

    @Test
    void retentionUsesTheSameAbsentAndExactVersionContract() {
        try (FakeCursorMetadataStore store = new FakeCursorMetadataStore()) {
            CursorRetentionRecord initial = retention(1, 5);
            VersionedCursorRetention created = store.createRetention(CLUSTER, initial).join();
            assertThat(store.getRetention(CLUSTER, streamId()).join()).contains(created);
            assertConditionFailure(() -> store.createRetention(CLUSTER, initial).join());
            CursorRetentionRecord next = retention(2, 6);
            VersionedCursorRetention replaced = store.compareAndSetRetention(
                    CLUSTER, next, created.metadataVersion()).join();
            assertThat(replaced.value()).isEqualTo(next);
            assertConditionFailure(() -> store.compareAndSetRetention(
                    CLUSTER, retention(3, 7), created.metadataVersion()).join());
        }
    }

    @Test
    void injectedKeyRecordIdentityMismatchFailsClosed() {
        FakeCursorMetadataStore.DurableState state = new FakeCursorMetadataStore.DurableState();
        CursorStateRecord other = cursor("other", 1, 1);
        CursorKeyspace keyspace = new CursorKeyspace(CLUSTER);
        state.inject(
                keyspace.cursorStateKey(streamId(), "requested"),
                keyspace.streamPartitionKey(streamId()).value(),
                F3MetadataCodecs.encodeEnvelope(other, CursorStateRecord.class));
        try (FakeCursorMetadataStore store = new FakeCursorMetadataStore(state, config(2))) {
            assertThatThrownBy(() -> store.getCursor(CLUSTER, streamId(), "requested").join())
                    .satisfies(error -> assertThat(rootCause(error))
                            .isInstanceOf(com.nereusstream.api.NereusException.class));
        }
    }

    private static CursorMetadataStoreConfig config(int pageSize) {
        return new CursorMetadataStoreConfig(
                java.time.Duration.ofSeconds(5), 128, 65_536, pageSize);
    }

    private static StreamId streamId() {
        return new StreamId(projection().streamId());
    }

    private static ManagedLedgerProjectionIdentity projection() {
        return new ManagedLedgerProjectionIdentity(
                1,
                1,
                ManagedLedgerProjectionNames.streamId(TOPIC, 1).value(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 2);
    }

    private static CursorStateRecord cursor(String name, long generation, long sequence) {
        return new CursorStateRecord(
                0,
                projection(),
                OWNER,
                name,
                CursorNames.cursorNameHash(name),
                generation,
                CursorRecordLifecycle.ACTIVE,
                sequence,
                1,
                ATTEMPT,
                0,
                Optional.empty(),
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                100,
                100 + sequence,
                OptionalLong.empty());
    }

    private static CursorRetentionRecord retention(long sequence, long floor) {
        return new CursorRetentionRecord(
                0,
                projection(),
                OWNER,
                CursorRetentionLifecycle.ACTIVE,
                sequence,
                floor,
                0,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                100 + sequence);
    }

    private static void assertConditionFailure(Runnable operation) {
        assertThatThrownBy(operation::run).satisfies(error -> assertThat(rootCause(error))
                .isInstanceOf(CursorMetadataConditionFailedException.class));
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
