/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ManagedLedgerCursorProtocolTest {
    private static final String CLUSTER = "cluster/a";
    private static final String NAME = "tenant/ns/persistent/f3-protocol";
    private static final String OWNER = "00112233445566778899aabbccddeeff";
    private static final String ATTEMPT = "ffeeddccbbaa99887766554433221100";
    private static final ProjectionPublishGuard ALLOW = () -> CompletableFuture.completedFuture(null);

    @Test
    void canonicalMarkerIsInternalMonotonicAndUnknownReservedValuesFail() {
        Map<String, String> activated = ManagedLedgerCursorProtocol.activate(Map.of("owner", "one"));
        assertThat(activated).containsEntry(ManagedLedgerCursorProtocol.PROPERTY, "1");
        assertThat(ManagedLedgerCursorProtocol.externalProperties(activated))
                .containsExactly(Map.entry("owner", "one"));
        assertThat(ManagedLedgerCursorProtocol.replaceExternalProperties(
                activated, Map.of("owner", "two")))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "owner", "two", ManagedLedgerCursorProtocol.PROPERTY, "1"));
        assertThatThrownBy(() -> ManagedLedgerCursorProtocol.canonicalDurableProperties(
                Map.of(ManagedLedgerCursorProtocol.PROPERTY, "2")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ManagedLedgerCursorProtocol.canonicalDurableProperties(
                Map.of("nereus.unknown", "1")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void activationAndEveryPropertyLifecycleMutationPreserveTheMarker() {
        var state = new FakeManagedLedgerProjectionMetadataStore.DurableState();
        try (var store = new FakeManagedLedgerProjectionMetadataStore(
                state, ProjectionMetadataStoreConfig.defaults(), Clock.systemUTC())) {
            TopicProjectionRecord created = store.createFirstProjection(
                    CLUSTER, request(NAME, 3, 1), ALLOW).join();
            assertThat(ManagedLedgerCursorProtocol.isActivated(created)).isFalse();

            TopicProjectionRecord activated = store.activateCursorProtocol(
                    CLUSTER,
                    NAME,
                    created.projectionIdentity(),
                    created.metadataVersion()).join();
            assertThat(ManagedLedgerCursorProtocol.isActivated(activated)).isTrue();
            assertThatThrownBy(() -> ProjectionCreateRequest.canonicalProperties(activated.properties()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("reserved");

            TopicProjectionRecord updated = store.updateProperties(
                    CLUSTER,
                    NAME,
                    activated.projectionIdentity(),
                    activated.metadataVersion(),
                    Map.of("owner", "two")).join();
            assertThat(updated.properties())
                    .containsEntry(ManagedLedgerCursorProtocol.PROPERTY, "1")
                    .containsEntry("owner", "two");

            TopicProjectionRecord deleting = store.mirrorFacadeState(
                    CLUSTER, NAME, updated.projectionIdentity(), updated.metadataVersion(),
                    ManagedLedgerFacadeState.DELETING).join();
            TopicProjectionRecord deleted = store.mirrorFacadeState(
                    CLUSTER, NAME, deleting.projectionIdentity(), deleting.metadataVersion(),
                    ManagedLedgerFacadeState.DELETED).join();
            TopicProjectionRecord recreated = store.recreateDeletedProjection(
                    CLUSTER,
                    deleted.projectionIdentity(),
                    deleted.metadataVersion(),
                    request(NAME, 4, 2),
                    ALLOW).join();
            assertThat(recreated.properties())
                    .containsEntry(ManagedLedgerCursorProtocol.PROPERTY, "1")
                    .containsEntry("owner", "one");
        }
    }

    @Test
    void topicRecreationUsesANewCursorNamespaceThatCannotAliasTheOldIncarnation() {
        var projectionState = new FakeManagedLedgerProjectionMetadataStore.DurableState();
        try (var projectionStore = new FakeManagedLedgerProjectionMetadataStore(
                        projectionState, ProjectionMetadataStoreConfig.defaults(), Clock.systemUTC());
                var cursorStore = new FakeCursorMetadataStore()) {
            TopicProjectionRecord original = projectionStore
                    .createFirstProjection(CLUSTER, request(NAME, 3, 1), ALLOW)
                    .join();
            original = projectionStore
                    .activateCursorProtocol(
                            CLUSTER,
                            NAME,
                            original.projectionIdentity(),
                            original.metadataVersion())
                    .join();
            cursorStore.createCursor(
                            CLUSTER,
                            cursor(original.projectionIdentity(), "subscription-a"))
                    .join();

            TopicProjectionRecord deleting = projectionStore
                    .mirrorFacadeState(
                            CLUSTER,
                            NAME,
                            original.projectionIdentity(),
                            original.metadataVersion(),
                            ManagedLedgerFacadeState.DELETING)
                    .join();
            TopicProjectionRecord deleted = projectionStore
                    .mirrorFacadeState(
                            CLUSTER,
                            NAME,
                            deleting.projectionIdentity(),
                            deleting.metadataVersion(),
                            ManagedLedgerFacadeState.DELETED)
                    .join();
            TopicProjectionRecord recreated = projectionStore
                    .recreateDeletedProjection(
                            CLUSTER,
                            deleted.projectionIdentity(),
                            deleted.metadataVersion(),
                            request(NAME, 4, 2),
                            ALLOW)
                    .join();

            assertThat(recreated.incarnation()).isEqualTo(2);
            assertThat(recreated.projectionIdentity()).isNotEqualTo(original.projectionIdentity());
            assertThat(recreated.streamId()).isNotEqualTo(original.streamId());
            assertThat(recreated.virtualLedgerId()).isNotEqualTo(original.virtualLedgerId());
            assertThat(cursorStore
                            .getCursor(
                                    CLUSTER,
                                    new StreamId(original.streamId()),
                                    "subscription-a")
                            .join())
                    .isPresent();
            assertThat(cursorStore
                            .getCursor(
                                    CLUSTER,
                                    new StreamId(recreated.streamId()),
                                    "subscription-a")
                            .join())
                    .isEmpty();

            cursorStore.createCursor(
                            CLUSTER,
                            cursor(recreated.projectionIdentity(), "subscription-a"))
                    .join();
            assertThat(cursorStore
                            .getCursor(
                                    CLUSTER,
                                    new StreamId(original.streamId()),
                                    "subscription-a")
                            .join()
                            .orElseThrow()
                            .value()
                            .projection())
                    .isEqualTo(original.projectionIdentity());
            assertThat(cursorStore
                            .getCursor(
                                    CLUSTER,
                                    new StreamId(recreated.streamId()),
                                    "subscription-a")
                            .join()
                            .orElseThrow()
                            .value()
                            .projection())
                    .isEqualTo(recreated.projectionIdentity());
        }
    }

    private static CursorStateRecord cursor(
            ManagedLedgerProjectionIdentity projection, String name) {
        return new CursorStateRecord(
                0,
                projection,
                OWNER,
                name,
                CursorNames.cursorNameHash(name),
                1,
                CursorRecordLifecycle.ACTIVE,
                1,
                1,
                ATTEMPT,
                0,
                Optional.empty(),
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                100,
                100,
                OptionalLong.empty());
    }

    private static ProjectionCreateRequest request(String name, long binding, long incarnation) {
        return new ProjectionCreateRequest(
                name,
                binding,
                incarnation,
                new StreamMetadata(
                        ManagedLedgerProjectionNames.streamId(name, incarnation),
                        ManagedLedgerProjectionNames.streamName(name, incarnation),
                        StreamState.ACTIVE,
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        Map.of(ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1),
                        100,
                        7,
                        0,
                        0,
                        0),
                Map.of("owner", "one"));
    }
}
