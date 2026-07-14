/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ManagedLedgerCursorProtocolTest {
    private static final String CLUSTER = "cluster/a";
    private static final String NAME = "tenant/ns/persistent/f3-protocol";
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
