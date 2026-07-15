/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.metadata.oxia.testing;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.metadata.oxia.AppendReplayCursor;
import com.nereusstream.metadata.oxia.AppendReplaySearchResult;
import com.nereusstream.metadata.oxia.AppendReplayStatus;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.StableAppendResult;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.StreamStateTransitionRequest;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class Phase15MetadataContractTest {
    @Test
    void stableCommitMaterializationPagedReplayAndLifecycleAreIndependent() {
        FakeOxiaMetadataStore store = new FakeOxiaMetadataStore(() -> 1_000L, 10);
        String cluster = "phase15";
        StreamId streamId = new StreamId(store.createOrGetStream(cluster, new StreamName("events"),
                new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId());
        AppendSessionRecord session = store.acquireAppendSession(cluster, streamId,
                new AppendSessionOptions("writer", Duration.ofSeconds(30), false)).join();
        List<CommitAppendRequest> requests = new ArrayList<>();
        for (int offset = 0; offset < 4; offset++) {
            CommitAppendRequest request = request(streamId, session, offset);
            requests.add(request);
            StableAppendResult stable = commitProtected(store, cluster, request);
            assertThat(store.scanOffsetIndex(cluster, streamId, 0, 10).join()).hasSize(offset);
            store.materializeGenerationZero(cluster, stable.reachableAppend()).join();
        }

        Optional<AppendReplayCursor> cursor = Optional.empty();
        AppendReplaySearchResult search;
        int pages = 0;
        do {
            search = store.searchAppendReplay(cluster, requests.getFirst(), cursor, 1).join();
            cursor = search.continuation();
            pages++;
        } while (search.status() == AppendReplayStatus.CONTINUE);
        assertThat(search.status()).isEqualTo(AppendReplayStatus.FOUND);
        assertThat(pages).isGreaterThan(1);

        StreamMetadataSnapshot active = store.getStreamSnapshot(cluster, streamId).join();
        StreamMetadataSnapshot sealed = store.transitionStreamState(cluster, new StreamStateTransitionRequest(
                streamId, StreamState.ACTIVE, StreamState.SEALED, active.metadataVersion())).join();
        StreamMetadataSnapshot deleting = store.transitionStreamState(cluster, new StreamStateTransitionRequest(
                streamId, StreamState.SEALED, StreamState.DELETING, sealed.metadataVersion())).join();
        StreamMetadataSnapshot deleted = store.transitionStreamState(cluster, new StreamStateTransitionRequest(
                streamId, StreamState.DELETING, StreamState.DELETED, deleting.metadataVersion())).join();
        assertThat(deleted.metadata().state()).isEqualTo(StreamState.DELETED.name());

        assertThat(store.storedMetadataValuesForTesting())
                .extracting(FakeOxiaMetadataStore.StoredMetadataValue::recordType)
                .contains("StreamCommitTargetRecord", "OffsetIndexTargetRecord", "CommittedAppendRecord");
    }

    private static StableAppendResult commitProtected(
            FakeOxiaMetadataStore store,
            String cluster,
            CommitAppendRequest request) {
        ObjectSliceReadTarget target = (ObjectSliceReadTarget) request.readTarget();
        store.putObjectManifest(cluster, manifest(request, target)).join();
        PreparedStableAppend prepared = store.prepareStableAppend(cluster, request).join();
        var root = store.createRoot(cluster, root(target)).join();
        String referenceId = "ra1-" + DeterministicIds.stableHashComponent(
                request.streamId().value()
                        + prepared.commitId()
                        + prepared.objectKeyHash().value());
        ObjectProtectionIdentity identity = new ObjectProtectionIdentity(
                prepared.objectKeyHash(),
                ObjectProtectionType.REACHABLE_APPEND,
                referenceId);
        var protection = store.createProtection(cluster, new ObjectProtectionRecord(
                1,
                prepared.objectKeyHash().value(),
                ObjectProtectionType.REACHABLE_APPEND.wireId(),
                referenceId,
                prepared.commitKey(),
                prepared.commitMetadataVersion(),
                prepared.commitRecordSha256().value(),
                root.value().lifecycleEpoch(),
                1_000,
                0,
                0)).join();
        return store.commitPreparedStableAppend(
                cluster,
                prepared,
                identity,
                root.metadataVersion(),
                root.value().lifecycleEpoch(),
                protection.metadataVersion(),
                protection.durableValueSha256()).join();
    }

    private static ObjectManifestRecord manifest(
            CommitAppendRequest request,
            ObjectSliceReadTarget target) {
        return new ObjectManifestRecord(
                target.objectId().value(),
                target.objectKey().value(),
                ObjectType.MULTI_STREAM_WAL_OBJECT.name(),
                "UPLOADED",
                1,
                0,
                "test-writer",
                request.writerId(),
                request.writerRunIdHash(),
                request.epoch(),
                1_000,
                1_000,
                128,
                ChecksumType.CRC32C.name(),
                "44444444",
                ChecksumType.CRC32C.name(),
                "33333333",
                List.of(new StreamSliceManifestRecord(
                        0,
                        request.streamId().value(),
                        target.sliceId(),
                        request.epoch(),
                        target.objectOffset(),
                        target.objectLength(),
                        request.recordCount(),
                        request.entryCount(),
                        request.logicalBytes(),
                        request.schemaRefs(),
                        EntryIndexReferenceRecord.fromApi(target.entryIndexRef()),
                        target.sliceChecksum().type().name(),
                        target.sliceChecksum().value(),
                        request.payloadFormat().name(),
                        "UPLOADED")),
                10_000,
                0);
    }

    private static PhysicalObjectRootRecord root(ObjectSliceReadTarget target) {
        return new PhysicalObjectRootRecord(
                1,
                ObjectKeyHash.from(target.objectKey()).value(),
                target.objectKey().value(),
                target.objectId().value(),
                1,
                128,
                ChecksumType.CRC32C.name(),
                "33333333",
                "",
                "",
                PhysicalObjectLifecycle.ACTIVE,
                1,
                1_000,
                2_000,
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                0);
    }

    private static CommitAppendRequest request(
            StreamId streamId, AppendSessionRecord session, long offset) {
        String suffix = Long.toString(offset);
        EntryIndexRef index = new EntryIndexRef(EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(), Optional.empty(), Optional.empty(), 100, 10,
                new Checksum(ChecksumType.CRC32C, "11111111"));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1, new ObjectId("object-" + suffix), new ObjectKey("key-" + suffix),
                ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1", "OPAQUE_SLICE", "slice-" + suffix,
                10, 20, new Checksum(ChecksumType.CRC32C, "22222222"), index);
        return new CommitAppendRequest(streamId, "writer", "run", session.epoch(), session.fencingToken(), offset,
                target, PayloadFormat.OPAQUE_RECORD_BATCH, 1, 1, 7, List.of(), 1, 1, Optional.empty());
    }
}
