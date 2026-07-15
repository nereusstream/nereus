/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerGenerationProtocol;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.ProjectionCreateRequest;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.metadata.oxia.ProjectionPublishGuard;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ProjectionGenerationReferenceDomainTest {
    private static final String CLUSTER = "cluster/a";
    private static final String NAME = "tenant/ns/persistent/projection-domain";
    private static final ProjectionPublishGuard ALLOW =
            () -> CompletableFuture.completedFuture(null);
    private static final GcReferenceDomainConfig CONFIG =
            new GcReferenceDomainConfig(1, 100, 100);

    @Test
    void liveProjectionRequiresMarkerAndQueryBoundRevalidationDetectsTopicDrift() {
        var state = new FakeManagedLedgerProjectionMetadataStore.DurableState();
        try (var store = projectionStore(state)) {
            TopicProjectionRecord created = store.createFirstProjection(
                    CLUSTER, request(NAME, 3, 1), ALLOW).join();
            GcReferenceQuery query = query(new StreamId(created.streamId()));
            var domain = new ProjectionGenerationReferenceDomain(CLUSTER, store, CONFIG);

            var blocked = domain.snapshot(query).join();
            assertThat(blocked.complete()).isTrue();
            assertThat(blocked.veto()).isTrue();
            assertThat(blocked.authorities()).hasSize(2);

            TopicProjectionRecord activated = store.activateGenerationProtocol(
                    CLUSTER,
                    NAME,
                    created.projectionIdentity(),
                    created.metadataVersion()).join();
            assertThat(ManagedLedgerGenerationProtocol.isActivated(activated)).isTrue();
            var clear = domain.snapshot(query).join();
            assertThat(clear.complete()).isTrue();
            assertThat(clear.veto()).isFalse();
            assertThat(domain.stillMatches(query, clear).join()).isTrue();

            store.updateProperties(
                    CLUSTER,
                    NAME,
                    activated.projectionIdentity(),
                    activated.metadataVersion(),
                    Map.of("owner", "changed")).join();
            assertThat(domain.stillMatches(query, clear).join()).isFalse();
        }
    }

    @Test
    void strictlyNewerCurrentIncarnationProvesOldStreamUnaddressable() {
        var state = new FakeManagedLedgerProjectionMetadataStore.DurableState();
        try (var store = projectionStore(state)) {
            TopicProjectionRecord original = store.createFirstProjection(
                    CLUSTER, request(NAME, 3, 1), ALLOW).join();
            TopicProjectionRecord deleting = store.mirrorFacadeState(
                    CLUSTER,
                    NAME,
                    original.projectionIdentity(),
                    original.metadataVersion(),
                    ManagedLedgerFacadeState.DELETING).join();
            TopicProjectionRecord deleted = store.mirrorFacadeState(
                    CLUSTER,
                    NAME,
                    deleting.projectionIdentity(),
                    deleting.metadataVersion(),
                    ManagedLedgerFacadeState.DELETED).join();
            var domain = new ProjectionGenerationReferenceDomain(CLUSTER, store, CONFIG);
            var deletedView = domain.snapshot(query(new StreamId(original.streamId()))).join();
            assertThat(deletedView.complete()).isTrue();
            assertThat(deletedView.veto()).isFalse();
            TopicProjectionRecord recreated = store.recreateDeletedProjection(
                    CLUSTER,
                    deleted.projectionIdentity(),
                    deleted.metadataVersion(),
                    request(NAME, 4, 2),
                    ALLOW).join();

            var old = domain.snapshot(query(new StreamId(original.streamId()))).join();
            var current = domain.snapshot(query(new StreamId(recreated.streamId()))).join();
            assertThat(old.complete()).isTrue();
            assertThat(old.veto()).isFalse();
            assertThat(current.complete()).isTrue();
            assertThat(current.veto()).isTrue();
        }
    }

    @Test
    void missingBindingAndOwnerlessQueriesFailClosedWithoutInventingGlobalProof() {
        var state = new FakeManagedLedgerProjectionMetadataStore.DurableState();
        try (var store = projectionStore(state)) {
            StreamId streamId = ManagedLedgerProjectionNames.streamId(NAME, 1);
            var domain = new ProjectionGenerationReferenceDomain(CLUSTER, store, CONFIG);
            var missing = domain.snapshot(query(streamId)).join();
            assertThat(missing.complete()).isTrue();
            assertThat(missing.veto()).isTrue();
            assertThat(missing.authorities()).hasSize(1);

            long before = state.backendCalls();
            var ownerless = domain.snapshot(GcReferenceQuery.create(
                    GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                    object(),
                    List.of(),
                    sha256('2'))).join();
            assertThat(ownerless.complete()).isFalse();
            assertThat(ownerless.veto()).isTrue();
            assertThat(state.backendCalls()).isEqualTo(before);
        }
    }

    @Test
    void topicPublishedBeforeDerivedBindingRepairCannotAuthorizeDeletion() {
        var state = new FakeManagedLedgerProjectionMetadataStore.DurableState();
        try (var store = projectionStore(state)) {
            store.failNext(
                    FakeManagedLedgerProjectionMetadataStore.FailurePoint.AFTER_TOPIC_WRITE);
            assertThatThrownBy(() -> store.createFirstProjection(
                            CLUSTER, request(NAME, 3, 1), ALLOW).join())
                    .isInstanceOf(RuntimeException.class);

            var domain = new ProjectionGenerationReferenceDomain(CLUSTER, store, CONFIG);
            var blocked = domain.snapshot(query(
                    ManagedLedgerProjectionNames.streamId(NAME, 1))).join();
            assertThat(blocked.complete()).isTrue();
            assertThat(blocked.veto()).isTrue();
            assertThat(blocked.authorities()).hasSize(1);
        }
    }

    private static FakeManagedLedgerProjectionMetadataStore projectionStore(
            FakeManagedLedgerProjectionMetadataStore.DurableState state) {
        return new FakeManagedLedgerProjectionMetadataStore(
                state, ProjectionMetadataStoreConfig.defaults(), Clock.systemUTC());
    }

    private static GcReferenceQuery query(StreamId streamId) {
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object(),
                List.of(streamId),
                sha256('1'));
    }

    private static PhysicalObjectIdentity object() {
        return PhysicalObjectIdentity.create(
                new ObjectKey("objects/projection-domain-source"),
                Optional.empty(),
                PhysicalObjectKind.OBJECT_WAL,
                8,
                new Checksum(ChecksumType.CRC32C, "00000000"),
                Optional.empty(),
                Optional.empty());
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

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, String.valueOf(value).repeat(64));
    }
}
