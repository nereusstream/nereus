/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import static com.nereusstream.metadata.oxia.retirement.RetirementMetadataStoreTestSupport.CLUSTER;
import static com.nereusstream.metadata.oxia.retirement.RetirementMetadataStoreTestSupport.COMMIT_ID;
import static com.nereusstream.metadata.oxia.retirement.RetirementMetadataStoreTestSupport.OBJECT_ID;
import static com.nereusstream.metadata.oxia.retirement.RetirementMetadataStoreTestSupport.OFFSET_END;
import static com.nereusstream.metadata.oxia.retirement.RetirementMetadataStoreTestSupport.SLICE_ID;
import static com.nereusstream.metadata.oxia.retirement.RetirementMetadataStoreTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.PartitionKey;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.records.CommittedAppendRecord;
import com.nereusstream.metadata.oxia.records.CommittedSliceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class SourceRetirementMetadataStoreContractTest {
    private static final StreamId STREAM_ID = new StreamId(STREAM);
    private static final Checksum WRONG_DIGEST = new Checksum(ChecksumType.SHA256, "f".repeat(64));

    @Test
    void conditionallyDeletesBothGenerationZeroIndexEncodingsByExactBytesAndVersion() {
        RetirementMetadataStoreTestSupport.FakeClient client = new RetirementMetadataStoreTestSupport.FakeClient();
        SourceRetirementMetadataStore store = new OxiaJavaSourceRetirementMetadataStore(client);
        OxiaKeyspace keys = new OxiaKeyspace(CLUSTER);
        String key = keys.offsetIndexKey(STREAM_ID, OFFSET_END, 0);
        PartitionKey partition = keys.streamPartitionKey(STREAM_ID);

        OffsetIndexTargetRecord generic = RetirementMetadataStoreTestSupport.genericIndex();
        client.put(key, partition, generic, OffsetIndexTargetRecord.class, 41);
        store.deleteGenerationZeroIndex(
                CLUSTER,
                STREAM_ID,
                OFFSET_END,
                41,
                RetirementMetadataStoreTestSupport.digest(generic, OffsetIndexTargetRecord.class)).join();
        assertThat(client.contains(key, partition)).isFalse();

        OffsetIndexRecord legacy = RetirementMetadataStoreTestSupport.legacyIndex();
        client.put(key, partition, legacy, OffsetIndexRecord.class, 43);
        assertThatThrownBy(() -> store.deleteGenerationZeroIndex(
                        CLUSTER, STREAM_ID, OFFSET_END, 43, WRONG_DIGEST).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
        assertThat(client.contains(key, partition)).isTrue();
        assertThatThrownBy(() -> store.deleteGenerationZeroIndex(
                        CLUSTER,
                        STREAM_ID,
                        OFFSET_END,
                        44,
                        RetirementMetadataStoreTestSupport.digest(legacy, OffsetIndexRecord.class)).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
        assertThat(client.contains(key, partition)).isTrue();

        store.deleteGenerationZeroIndex(
                CLUSTER,
                STREAM_ID,
                OFFSET_END,
                43,
                RetirementMetadataStoreTestSupport.digest(legacy, OffsetIndexRecord.class)).join();
        assertThat(client.contains(key, partition)).isFalse();
        assertThatThrownBy(() -> store.deleteGenerationZeroIndex(
                        CLUSTER,
                        STREAM_ID,
                        OFFSET_END,
                        43,
                        RetirementMetadataStoreTestSupport.digest(legacy, OffsetIndexRecord.class)).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
    }

    @Test
    void capturesAndDeletesBothCommittedMarkerEncodingsWithoutKeyAliasing() {
        RetirementMetadataStoreTestSupport.FakeClient client = new RetirementMetadataStoreTestSupport.FakeClient();
        SourceRetirementMetadataStore store = new OxiaJavaSourceRetirementMetadataStore(client);
        OxiaKeyspace keys = new OxiaKeyspace(CLUSTER);
        PartitionKey partition = keys.streamPartitionKey(STREAM_ID);
        LegacyCommittedSliceIdentity legacyIdentity = new LegacyCommittedSliceIdentity(
                new ObjectId(OBJECT_ID), SLICE_ID);
        GenericCommittedAppendIdentity genericIdentity = new GenericCommittedAppendIdentity(COMMIT_ID);

        CommittedSliceRecord legacy = RetirementMetadataStoreTestSupport.legacyMarker();
        String legacyKey = keys.committedSliceKey(STREAM_ID, legacyIdentity.objectId(), SLICE_ID);
        client.put(legacyKey, partition, legacy, CommittedSliceRecord.class, 51);
        VersionedGenerationZeroMarker capturedLegacy = store.getCommittedMarker(
                CLUSTER, STREAM_ID, legacyIdentity).join().orElseThrow();
        assertThat(capturedLegacy.identity()).isEqualTo(legacyIdentity);
        assertThat(capturedLegacy.metadataVersion()).isEqualTo(51);
        assertThat(capturedLegacy.offsetEnd()).isEqualTo(OFFSET_END);
        store.deleteCommittedMarker(
                CLUSTER,
                STREAM_ID,
                legacyIdentity,
                capturedLegacy.metadataVersion(),
                capturedLegacy.durableValueSha256()).join();
        assertThat(client.contains(legacyKey, partition)).isFalse();

        CommittedAppendRecord generic = RetirementMetadataStoreTestSupport.genericMarker();
        String genericKey = keys.committedAppendKey(STREAM_ID, COMMIT_ID);
        client.put(genericKey, partition, generic, CommittedAppendRecord.class, 53);
        VersionedGenerationZeroMarker capturedGeneric = store.getCommittedMarker(
                CLUSTER, STREAM_ID, genericIdentity).join().orElseThrow();
        assertThat(capturedGeneric.identity()).isEqualTo(genericIdentity);
        assertThat(capturedGeneric.key()).isEqualTo(genericKey);
        store.deleteCommittedMarker(
                CLUSTER,
                STREAM_ID,
                genericIdentity,
                capturedGeneric.metadataVersion(),
                capturedGeneric.durableValueSha256()).join();
        assertThat(client.contains(genericKey, partition)).isFalse();
    }

    @Test
    void exactJournalKeysRecoverLegacyIdentityAndCanonicalCommitEvidence() {
        RetirementMetadataStoreTestSupport.FakeClient client = new RetirementMetadataStoreTestSupport.FakeClient();
        SourceRetirementMetadataStore store = new OxiaJavaSourceRetirementMetadataStore(client);
        OxiaKeyspace keys = new OxiaKeyspace(CLUSTER);
        PartitionKey partition = keys.streamPartitionKey(STREAM_ID);

        CommittedSliceRecord marker = RetirementMetadataStoreTestSupport.legacyMarker();
        String markerKey = keys.committedSliceKey(
                STREAM_ID, new ObjectId(OBJECT_ID), SLICE_ID);
        client.put(markerKey, partition, marker, CommittedSliceRecord.class, 57);
        VersionedGenerationZeroMarker capturedMarker = store.getCommittedMarkerByKey(
                CLUSTER, markerKey).join().orElseThrow();
        assertThat(capturedMarker.identity()).isEqualTo(new LegacyCommittedSliceIdentity(
                new ObjectId(OBJECT_ID), SLICE_ID));
        store.deleteCommittedMarkerByKey(
                CLUSTER,
                markerKey,
                capturedMarker.metadataVersion(),
                capturedMarker.durableValueSha256()).join();
        assertThat(client.contains(markerKey, partition)).isFalse();

        StreamCommitRecord commit = RetirementMetadataStoreTestSupport.legacyCommit();
        String commitKey = keys.streamCommitKey(STREAM_ID, COMMIT_ID);
        client.put(commitKey, partition, commit, StreamCommitRecord.class, 59);
        VersionedGenerationZeroCommit capturedCommit = store.getCommitNodeByKey(
                CLUSTER, commitKey).join().orElseThrow();
        assertThat(capturedCommit.sourceEncoding())
                .isEqualTo(AppendRecoveryCommitEncoding.LEGACY_STREAM_COMMIT_V1);
        assertThat(capturedCommit.markerIdentity()).isEqualTo(capturedMarker.identity());
        assertThat(capturedCommit.canonicalCommitRecordSha256().value()).hasSize(64);
        store.deleteCommitNodeByKey(
                CLUSTER,
                commitKey,
                capturedCommit.metadataVersion(),
                capturedCommit.durableValueSha256()).join();
        assertThat(client.contains(commitKey, partition)).isFalse();
    }

    @Test
    void exactJournalRoutingRejectsCrossFamilyAndKeyValueAliasAttempts() {
        RetirementMetadataStoreTestSupport.FakeClient client = new RetirementMetadataStoreTestSupport.FakeClient();
        SourceRetirementMetadataStore store = new OxiaJavaSourceRetirementMetadataStore(client);
        OxiaKeyspace keys = new OxiaKeyspace(CLUSTER);
        PartitionKey partition = keys.streamPartitionKey(STREAM_ID);
        String canonical = keys.committedSliceKey(
                STREAM_ID, new ObjectId(OBJECT_ID), SLICE_ID);
        String aliased = canonical.substring(0, canonical.length() - 1)
                + (canonical.endsWith("a") ? "b" : "a");
        CommittedSliceRecord marker = RetirementMetadataStoreTestSupport.legacyMarker();
        client.put(aliased, partition, marker, CommittedSliceRecord.class, 60);

        assertThatThrownBy(() -> store.getCommittedMarkerByKey(CLUSTER, aliased).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        ErrorCode.METADATA_INVARIANT_VIOLATION)));
        assertThat(client.contains(aliased, partition)).isTrue();

        String indexKey = keys.offsetIndexKey(STREAM_ID, OFFSET_END, 0);
        assertThatThrownBy(() -> store.getCommitNodeByKey(CLUSTER, indexKey))
                .isInstanceOfSatisfying(NereusException.class, nereus ->
                        assertThat(nereus.code()).isEqualTo(
                                ErrorCode.METADATA_INVARIANT_VIOLATION));
        String otherClusterKey = new OxiaKeyspace("other-cluster")
                .streamCommitKey(STREAM_ID, COMMIT_ID);
        assertThatThrownBy(() -> store.getCommitNodeByKey(CLUSTER, otherClusterKey))
                .isInstanceOfSatisfying(NereusException.class, nereus ->
                        assertThat(nereus.code()).isEqualTo(
                                ErrorCode.METADATA_INVARIANT_VIOLATION));
    }

    @Test
    void conditionallyDeletesBothCommitEncodingsAndLeavesResponseLossForCoordinatorReproof() {
        RetirementMetadataStoreTestSupport.FakeClient client = new RetirementMetadataStoreTestSupport.FakeClient();
        SourceRetirementMetadataStore store = new OxiaJavaSourceRetirementMetadataStore(client);
        OxiaKeyspace keys = new OxiaKeyspace(CLUSTER);
        String key = keys.streamCommitKey(STREAM_ID, COMMIT_ID);
        PartitionKey partition = keys.streamPartitionKey(STREAM_ID);

        StreamCommitTargetRecord generic = RetirementMetadataStoreTestSupport.genericCommit();
        client.put(key, partition, generic, StreamCommitTargetRecord.class, 61);
        store.deleteCommitNode(
                CLUSTER,
                STREAM_ID,
                COMMIT_ID,
                61,
                RetirementMetadataStoreTestSupport.digest(generic, StreamCommitTargetRecord.class)).join();
        assertThat(client.contains(key, partition)).isFalse();

        StreamCommitRecord legacy = RetirementMetadataStoreTestSupport.legacyCommit();
        client.put(key, partition, legacy, StreamCommitRecord.class, 63);
        client.loseNextDeleteResponse();
        assertThatThrownBy(() -> store.deleteCommitNode(
                        CLUSTER,
                        STREAM_ID,
                        COMMIT_ID,
                        63,
                        RetirementMetadataStoreTestSupport.digest(legacy, StreamCommitRecord.class)).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(client.contains(key, partition)).isFalse();
        assertThatThrownBy(() -> store.deleteCommitNode(
                        CLUSTER,
                        STREAM_ID,
                        COMMIT_ID,
                        63,
                        RetirementMetadataStoreTestSupport.digest(legacy, StreamCommitRecord.class)).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
    }

    @Test
    void rejectsIdentityContradictionsAndStopsAdmissionAfterClose() {
        RetirementMetadataStoreTestSupport.FakeClient client = new RetirementMetadataStoreTestSupport.FakeClient();
        SourceRetirementMetadataStore store = new OxiaJavaSourceRetirementMetadataStore(client);
        OxiaKeyspace keys = new OxiaKeyspace(CLUSTER);
        String key = keys.offsetIndexKey(STREAM_ID, OFFSET_END, 0);
        PartitionKey partition = keys.streamPartitionKey(STREAM_ID);
        OffsetIndexTargetRecord wrongStream = new OffsetIndexTargetRecord(
                "other-stream",
                0,
                OFFSET_END,
                0,
                100,
                RetirementMetadataStoreTestSupport.genericIndex().readTarget(),
                RetirementMetadataStoreTestSupport.genericIndex().payloadFormat(),
                2,
                2,
                100,
                java.util.List.of(),
                "projection-f4",
                0,
                100,
                1,
                false,
                0);
        client.put(key, partition, wrongStream, OffsetIndexTargetRecord.class, 71);

        assertThatThrownBy(() -> store.deleteGenerationZeroIndex(
                        CLUSTER,
                        STREAM_ID,
                        OFFSET_END,
                        71,
                        RetirementMetadataStoreTestSupport.digest(
                                wrongStream, OffsetIndexTargetRecord.class)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        ErrorCode.METADATA_INVARIANT_VIOLATION)));
        assertThat(client.contains(key, partition)).isTrue();

        store.close();
        assertThatThrownBy(() -> store.getCommittedMarker(
                        CLUSTER, STREAM_ID, new GenericCommittedAppendIdentity(COMMIT_ID)))
                .isInstanceOfSatisfying(NereusException.class, nereus ->
                        assertThat(nereus.code()).isEqualTo(ErrorCode.STORAGE_CLOSED));
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
