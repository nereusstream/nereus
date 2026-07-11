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
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.AppendReplayCursor;
import com.nereusstream.metadata.oxia.AppendReplaySearchResult;
import com.nereusstream.metadata.oxia.AppendReplayStatus;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.StableAppendResult;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.StreamStateTransitionRequest;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
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
            StableAppendResult stable = store.commitStableAppend(cluster, request).join();
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
