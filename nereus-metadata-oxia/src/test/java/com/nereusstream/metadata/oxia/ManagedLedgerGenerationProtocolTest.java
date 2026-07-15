/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ManagedLedgerGenerationProtocolTest {
    private static final String CLUSTER = "cluster/a";
    private static final String NAME = "tenant/ns/persistent/f4-generation-protocol";
    private static final ProjectionPublishGuard ALLOW =
            () -> CompletableFuture.completedFuture(null);

    @Test
    void composedMarkersAreInternalMonotonicAndUnknownReservedValuesFail() {
        Map<String, String> cursor = ManagedLedgerCursorProtocol.activate(Map.of("owner", "one"));
        Map<String, String> both = ManagedLedgerGenerationProtocol.activate(cursor);

        assertThat(both)
                .containsEntry(ManagedLedgerCursorProtocol.PROPERTY, "1")
                .containsEntry(ManagedLedgerGenerationProtocol.PROPERTY, "1");
        assertThat(ManagedLedgerProtocolProperties.externalProperties(both))
                .containsExactly(Map.entry("owner", "one"));
        assertThat(ManagedLedgerProtocolProperties.replaceExternalProperties(
                        both, Map.of("owner", "two")))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "owner", "two",
                        ManagedLedgerCursorProtocol.PROPERTY, "1",
                        ManagedLedgerGenerationProtocol.PROPERTY, "1"));
        assertThatThrownBy(() -> ManagedLedgerProtocolProperties.canonicalDurableProperties(
                        Map.of(ManagedLedgerGenerationProtocol.PROPERTY, "2")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("generation");
        assertThatThrownBy(() -> ManagedLedgerProtocolProperties.canonicalDurableProperties(
                        Map.of("nereus.unknown", "1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
    }

    @Test
    void exactStreamLookupPreservesEnvelopeAuthoritiesAcrossActivationAndRecreation() {
        var state = new FakeManagedLedgerProjectionMetadataStore.DurableState();
        TopicProjectionRecord original;
        try (var store = new FakeManagedLedgerProjectionMetadataStore(
                state, ProjectionMetadataStoreConfig.defaults(), Clock.systemUTC())) {
            original = store.createFirstProjection(
                    CLUSTER, request(NAME, 3, 1), ALLOW).join();
            TopicProjectionRecord cursorActivated = store.activateCursorProtocol(
                    CLUSTER,
                    NAME,
                    original.projectionIdentity(),
                    original.metadataVersion()).join();
            TopicProjectionRecord activated = store.activateGenerationProtocol(
                    CLUSTER,
                    NAME,
                    cursorActivated.projectionIdentity(),
                    cursorActivated.metadataVersion()).join();
            assertThat(ManagedLedgerCursorProtocol.isActivated(activated)).isTrue();
            assertThat(ManagedLedgerGenerationProtocol.isActivated(activated)).isTrue();

            ManagedLedgerStreamProjection view = store.getProjectionByStream(
                    CLUSTER, new StreamId(original.streamId())).join();
            VersionedVirtualLedgerProjection binding = view.streamBinding().orElseThrow();
            VersionedTopicProjection topic = view.currentTopic().orElseThrow();
            assertThat(binding.value().metadataVersion()).isZero();
            assertThat(topic.value().metadataVersion()).isZero();
            assertStoredIdentity(state, binding.key(), binding.metadataVersion(),
                    binding.durableValueSha256());
            assertStoredIdentity(state, topic.key(), topic.metadataVersion(),
                    topic.durableValueSha256());

            TopicProjectionRecord updated = store.updateProperties(
                    CLUSTER,
                    NAME,
                    activated.projectionIdentity(),
                    activated.metadataVersion(),
                    Map.of("owner", "two")).join();
            assertThat(updated.properties())
                    .containsEntry(ManagedLedgerCursorProtocol.PROPERTY, "1")
                    .containsEntry(ManagedLedgerGenerationProtocol.PROPERTY, "1");
            TopicProjectionRecord deleting = store.mirrorFacadeState(
                    CLUSTER,
                    NAME,
                    updated.projectionIdentity(),
                    updated.metadataVersion(),
                    ManagedLedgerFacadeState.DELETING).join();
            TopicProjectionRecord deleted = store.mirrorFacadeState(
                    CLUSTER,
                    NAME,
                    deleting.projectionIdentity(),
                    deleting.metadataVersion(),
                    ManagedLedgerFacadeState.DELETED).join();
            TopicProjectionRecord recreated = store.recreateDeletedProjection(
                    CLUSTER,
                    deleted.projectionIdentity(),
                    deleted.metadataVersion(),
                    request(NAME, 4, 2),
                    ALLOW).join();

            ManagedLedgerStreamProjection oldView = store.getProjectionByStream(
                    CLUSTER, new StreamId(original.streamId())).join();
            assertThat(oldView.streamBinding().orElseThrow().value().identity())
                    .isEqualTo(original.projectionIdentity());
            assertThat(oldView.currentTopic().orElseThrow().value().projectionIdentity())
                    .isEqualTo(recreated.projectionIdentity());
            assertThat(recreated.properties())
                    .containsEntry(ManagedLedgerCursorProtocol.PROPERTY, "1")
                    .containsEntry(ManagedLedgerGenerationProtocol.PROPERTY, "1");
        }
    }

    @Test
    void lostGenerationActivationResponseConvergesFromTheExactTopicAuthority() {
        var state = new FakeManagedLedgerProjectionMetadataStore.DurableState();
        try (var store = new FakeManagedLedgerProjectionMetadataStore(
                state, ProjectionMetadataStoreConfig.defaults(), Clock.systemUTC())) {
            TopicProjectionRecord created = store.createFirstProjection(
                    CLUSTER, request(NAME, 3, 1), ALLOW).join();
            store.failNext(
                    FakeManagedLedgerProjectionMetadataStore.FailurePoint.AFTER_TOPIC_WRITE);

            assertThatThrownBy(() -> store.activateGenerationProtocol(
                            CLUSTER,
                            NAME,
                            created.projectionIdentity(),
                            created.metadataVersion()).join())
                    .isInstanceOf(RuntimeException.class);

            TopicProjectionRecord recovered = store.activateGenerationProtocol(
                    CLUSTER,
                    NAME,
                    created.projectionIdentity(),
                    created.metadataVersion()).join();
            assertThat(ManagedLedgerGenerationProtocol.isActivated(recovered)).isTrue();
            assertThat(recovered.metadataVersion()).isGreaterThan(created.metadataVersion());
        }
    }

    private static void assertStoredIdentity(
            FakeManagedLedgerProjectionMetadataStore.DurableState state,
            String key,
            long version,
            Checksum digest) {
        var stored = state.storedValue(key).orElseThrow();
        assertThat(stored.version()).isEqualTo(version);
        assertThat(digest).isEqualTo(sha256(stored.envelope()));
    }

    private static ProjectionCreateRequest request(
            String name, long binding, long incarnation) {
        return new ProjectionCreateRequest(
                name,
                binding,
                incarnation,
                new StreamMetadata(
                        ManagedLedgerProjectionNames.streamId(name, incarnation),
                        ManagedLedgerProjectionNames.streamName(name, incarnation),
                        StreamState.ACTIVE,
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        Map.of(
                                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1),
                        100,
                        7,
                        0,
                        0,
                        0),
                Map.of("owner", "one"));
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
