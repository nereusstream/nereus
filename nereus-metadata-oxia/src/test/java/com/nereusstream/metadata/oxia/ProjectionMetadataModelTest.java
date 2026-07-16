/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.PositionIndexRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.metadata.oxia.records.VirtualLedgerProjectionRecord;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProjectionMetadataModelTest {
    private static final String NAME = "tenant/ns/persistent/topic";

    @Test
    @SuppressWarnings("deprecation")
    void acceptsOnlyTheCanonicalEmptyObjectWalCandidate() {
        LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        properties.put("z", "2");
        properties.put("a", "1");

        ProjectionCreateRequest request = new ProjectionCreateRequest(NAME, 3, 1, emptyStream(), properties);

        assertThat(request.initialProperties()).containsExactly(
                Map.entry("a", "1"), Map.entry("z", "2"));
        assertThatThrownBy(() -> request.initialProperties().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new ProjectionCreateRequest(
                        NAME, 3, 1, stream(StreamState.ACTIVE, StorageProfile.OBJECT_WAL_SYNC_OBJECT, 1, 0, 0,
                                payloadAttributes()), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical empty");
        ProjectionCreateRequest async = new ProjectionCreateRequest(
                NAME,
                3,
                1,
                stream(
                        StreamState.ACTIVE,
                        StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                        0,
                        0,
                        0,
                        payloadAttributes()),
                Map.of());
        assertThat(async.emptyStream().profile())
                .isEqualTo(StorageProfile.OBJECT_WAL_ASYNC_OBJECT);
        assertThatThrownBy(() -> new ProjectionCreateRequest(
                        NAME, 3, 1, stream(StreamState.ACTIVE, StorageProfile.OBJECT_WAL, 0, 0, 0,
                                payloadAttributes()), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical empty");
        assertThatThrownBy(() -> new ProjectionCreateRequest(
                        NAME, 3, 1, stream(StreamState.ACTIVE, StorageProfile.OBJECT_WAL_SYNC_OBJECT, 0, 0, 0,
                                Map.of()), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical empty");
    }

    @Test
    void rejectsReservedPropertiesOnCreateAndRecordDecode() {
        assertThatThrownBy(() -> new ProjectionCreateRequest(
                        NAME, 1, 1, emptyStream(), Map.of("nereus.internal", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
        assertThatThrownBy(() -> new ProjectionCreateRequest(
                        NAME, 1, 1, emptyStream(), Map.of("PULSAR.SHADOW_SOURCE", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
    }

    @Test
    void validatesFullAuthorityAndDerivedIdentity() {
        TopicProjectionRecord topic = topicRecord();
        ManagedLedgerProjectionIdentity identity = topic.projectionIdentity();
        VirtualLedgerProjectionRecord virtual = new VirtualLedgerProjectionRecord(
                NAME, topic.managedLedgerNameHash(), identity, 0,
                ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION, 0);
        PositionIndexRecord positions = new PositionIndexRecord(
                NAME, topic.managedLedgerNameHash(), identity,
                ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                ManagedLedgerProjectionNames.POSITION_FORMULA_V1, 0);

        assertThat(topic.parsedFacadeState()).isEqualTo(ManagedLedgerFacadeState.OPEN);
        assertThat(virtual.identity()).isEqualTo(identity);
        assertThat(positions.identity()).isEqualTo(identity);
        assertThatThrownBy(() -> new VirtualLedgerProjectionRecord(
                        "tenant/ns/other", topic.managedLedgerNameHash(), identity, 0, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PositionIndexRecord(
                        NAME, topic.managedLedgerNameHash(), identity, 1, "OFFSET_PLUS_ONE", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TopicProjectionRecord(
                        NAME, topic.managedLedgerNameHash(), 3, 1, topic.streamName(), topic.streamId(),
                        "nereus", StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(), topic.virtualLedgerId(), 1,
                        ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1, "UNKNOWN", Map.of(), 100, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown");
    }

    @Test
    void projectionStoreConfigurationIsClosedForF2() {
        assertThat(ProjectionMetadataStoreConfig.defaults().maxValueBytes()).isEqualTo(64 * 1024);
        assertThatThrownBy(() -> new ProjectionMetadataStoreConfig(Duration.ZERO, 1, 64 * 1024))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProjectionMetadataStoreConfig(Duration.ofSeconds(1), 0, 64 * 1024))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProjectionMetadataStoreConfig(Duration.ofSeconds(1), 1, 32 * 1024))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static TopicProjectionRecord topicRecord() {
        return new TopicProjectionRecord(
                NAME,
                ManagedLedgerProjectionNames.managedLedgerNameHash(NAME),
                3,
                1,
                ManagedLedgerProjectionNames.streamName(NAME, 1).value(),
                ManagedLedgerProjectionNames.streamId(NAME, 1).value(),
                ManagedLedgerProjectionNames.STORAGE_CLASS,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID,
                ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1,
                ManagedLedgerFacadeState.OPEN.name(),
                Map.of("owner", "nereus"),
                100,
                0,
                0);
    }

    private static StreamMetadata emptyStream() {
        return stream(StreamState.ACTIVE, StorageProfile.OBJECT_WAL_SYNC_OBJECT, 0, 0, 0, payloadAttributes());
    }

    private static StreamMetadata stream(
            StreamState state,
            StorageProfile profile,
            long endOffset,
            long cumulativeSize,
            long trimOffset,
            Map<String, String> attributes) {
        return new StreamMetadata(
                ManagedLedgerProjectionNames.streamId(NAME, 1),
                ManagedLedgerProjectionNames.streamName(NAME, 1),
                state,
                profile,
                attributes,
                100,
                7,
                endOffset,
                cumulativeSize,
                trimOffset);
    }

    private static Map<String, String> payloadAttributes() {
        return Map.of(ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1);
    }
}
