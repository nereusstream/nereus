/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.ProjectionCreateRequest;
import com.nereusstream.metadata.oxia.ProjectionPublishGuard;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ManagedLedgerGenerationProjectionRefV1Test {
    private static final String CLUSTER = "cluster/a";
    private static final String NAME =
            "tenant/ns/persistent/generation-projection";
    private static final ProjectionPublishGuard ALLOW =
            () -> CompletableFuture.completedFuture(null);

    @Test
    void strictNpr1RoundTripAndIdentityDigestAreStable() {
        try (var store =
                new FakeManagedLedgerProjectionMetadataStore()) {
            TopicProjectionRecord projection = store.createFirstProjection(
                    CLUSTER, request(NAME, 7, 1), ALLOW).join();
            var value = new ManagedLedgerGenerationProjectionRefV1(
                    NAME, projection.projectionIdentity());

            ProjectionRef encoded = value.toProjectionRef();

            assertThat(encoded.type())
                    .isEqualTo(ProjectionType.VIRTUAL_LEDGER);
            assertThat(encoded.value())
                    .isEqualTo(
                            "nereus-ml-v1.TlBSMQAAACp0ZW5hbnQvbnMvcGVyc2lzdGVudC9nZW5lcmF0aW9uLXByb2plY3Rpb24AAAAAAAAABwAAAAAAAAABAAAANnMtbWx1emV5NHdib3VseTVpenl2b2F5eHJmNWQ0ZWZlM3dxZnVpeG1sZGozanVkbGFuaXBxYUAAAAAAAAAABsqMhw")
                    .doesNotContain("=");
            assertThat(
                            ManagedLedgerGenerationProjectionRefV1
                                    .from(encoded))
                    .isEqualTo(value);
            assertThat(value.projectionIdentitySha256())
                    .isEqualTo(new com.nereusstream.api.Checksum(
                            com.nereusstream.api.ChecksumType.SHA256,
                            "5359bd6fe2167a56b0dad7d304cab4f67fc86f6b3e480138c15138084554c91d"));
        }
    }

    @Test
    void backfillCandidateFreezesTheSameExactNpr1Identity() {
        try (var store =
                new FakeManagedLedgerProjectionMetadataStore()) {
            TopicProjectionRecord projection = store.createFirstProjection(
                    CLUSTER, request(NAME, 7, 1), ALLOW).join();

            ManagedLedgerMaterializationRegistrationCandidate candidate =
                    ManagedLedgerMaterializationRegistrationCandidate.from(
                            projection);

            assertThat(candidate.managedLedgerName()).isEqualTo(NAME);
            assertThat(candidate.storageClassBindingGeneration())
                    .isEqualTo(7);
            assertThat(candidate.projectionIdentity())
                    .isEqualTo(projection.projectionIdentity());
            assertThat(candidate.projectionIdentitySha256())
                    .isEqualTo(new ManagedLedgerGenerationProjectionRefV1(
                                    NAME, projection.projectionIdentity())
                            .projectionIdentitySha256());
            assertThatThrownBy(() ->
                            new ManagedLedgerMaterializationRegistrationCandidate(
                                    NAME,
                                    7,
                                    projection.projectionIdentity(),
                                    new com.nereusstream.api.Checksum(
                                            com.nereusstream.api.ChecksumType
                                                    .SHA256,
                                            "00".repeat(32))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "projectionIdentitySha256");
        }
    }

    @Test
    void rejectsWrongTypePaddingAndCorruptCrc() {
        try (var store =
                new FakeManagedLedgerProjectionMetadataStore()) {
            TopicProjectionRecord projection = store.createFirstProjection(
                    CLUSTER, request(NAME, 7, 1), ALLOW).join();
            ProjectionRef valid =
                    new ManagedLedgerGenerationProjectionRefV1(
                                    NAME,
                                    projection.projectionIdentity())
                            .toProjectionRef();

            assertThatThrownBy(() ->
                            ManagedLedgerGenerationProjectionRefV1
                                    .from(new ProjectionRef(
                                            ProjectionType.PROTOCOL_HINT,
                                            valid.value())))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() ->
                            ManagedLedgerGenerationProjectionRefV1
                                    .from(new ProjectionRef(
                                            valid.type(),
                                            valid.value() + "=")))
                    .isInstanceOf(IllegalArgumentException.class);

            String payload = valid.value().substring(
                    ManagedLedgerGenerationProjectionRefV1
                            .VALUE_PREFIX
                            .length());
            byte[] corrupt =
                    Base64.getUrlDecoder().decode(payload);
            corrupt[corrupt.length - 1] ^= 1;
            ProjectionRef corruptRef = new ProjectionRef(
                    ProjectionType.VIRTUAL_LEDGER,
                    ManagedLedgerGenerationProjectionRefV1
                                    .VALUE_PREFIX
                            + Base64.getUrlEncoder()
                                    .withoutPadding()
                                    .encodeToString(corrupt));
            assertThatThrownBy(() ->
                            ManagedLedgerGenerationProjectionRefV1
                                    .from(corruptRef))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("CRC32C");
        }
    }

    @Test
    void authorityReaderCapturesExactBindingAndTopicAndClassifiesDeletionAsNonLive() {
        try (var store =
                new FakeManagedLedgerProjectionMetadataStore()) {
            TopicProjectionRecord projection = store.createFirstProjection(
                    CLUSTER, request(NAME, 7, 1), ALLOW).join();
            var ref = new ManagedLedgerGenerationProjectionRefV1(
                    NAME, projection.projectionIdentity());
            LiveProjectionSubject subject = new LiveProjectionSubject(
                    new StreamId(projection.streamId()),
                    ref.toProjectionRef(),
                    ref.projectionIdentitySha256());
            var reader =
                    new ManagedLedgerGenerationProjectionAuthorityReader(
                            CLUSTER, store);

            var live = reader.capture(subject).join();

            assertThat(live.live()).isTrue();
            assertThat(live.managedLedgerIdentity())
                    .contains(projection.projectionIdentity());
            assertThat(live.authorities()).hasSize(2);

            store.mirrorFacadeState(
                            CLUSTER,
                            NAME,
                            projection.projectionIdentity(),
                            projection.metadataVersion(),
                            ManagedLedgerFacadeState.DELETING)
                    .join();
            var deleting = reader.capture(subject).join();
            assertThat(deleting.live()).isFalse();
            assertThat(deleting.managedLedgerIdentity()).isEmpty();
            assertThat(deleting.authorities()).hasSize(2);
            assertThat(deleting).isNotEqualTo(live);
        }
    }

    private static ProjectionCreateRequest request(
            String name, long binding, long incarnation) {
        return new ProjectionCreateRequest(
                name,
                binding,
                incarnation,
                new StreamMetadata(
                        ManagedLedgerProjectionNames.streamId(
                                name, incarnation),
                        ManagedLedgerProjectionNames.streamName(
                                name, incarnation),
                        StreamState.ACTIVE,
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        Map.of(
                                ManagedLedgerProjectionNames
                                        .PAYLOAD_MAPPING_ATTRIBUTE,
                                ManagedLedgerProjectionNames
                                        .PAYLOAD_MAPPING_V1),
                        100,
                        7,
                        0,
                        0,
                        0),
                Map.of());
    }
}
