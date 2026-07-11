/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia;

import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface OxiaMetadataStore extends AutoCloseable {
    CompletableFuture<StreamMetadataRecord> createOrGetStream(
            String cluster,
            StreamName streamName,
            StreamCreateOptions options);

    CompletableFuture<StreamMetadataRecord> getStream(
            String cluster,
            StreamId streamId);

    CompletableFuture<StreamMetadataSnapshot> getStreamSnapshot(
            String cluster,
            StreamId streamId);

    CompletableFuture<AppendSessionRecord> acquireAppendSession(
            String cluster,
            StreamId streamId,
            AppendSessionOptions options);

    CompletableFuture<AppendSessionRecord> renewAppendSession(
            String cluster,
            StreamId streamId,
            String writerId,
            long epoch,
            String fencingToken,
            Duration ttl);

    CompletableFuture<Void> putObjectManifest(
            String cluster,
            ObjectManifestRecord manifest);

    CompletableFuture<Optional<ObjectManifestRecord>> getObjectManifest(
            String cluster,
            ObjectId objectId);

    CompletableFuture<Optional<ObjectReferenceRecord>> getObjectReferences(
            String cluster,
            ObjectId objectId);

    CompletableFuture<ObjectReferenceRecord> repairObjectReferences(
            String cluster,
            ObjectId objectId);

    CompletableFuture<CommitSliceResult> commitStreamSlice(
            String cluster,
            CommitSliceRequest request);

    CompletableFuture<StableAppendResult> commitStableAppend(
            String cluster,
            CommitAppendRequest request);

    CompletableFuture<CommittedAppend> materializeGenerationZero(
            String cluster,
            ReachableCommittedAppend reachableAppend);

    CompletableFuture<AppendReplaySearchResult> searchAppendReplay(
            String cluster,
            CommitAppendRequest request,
            Optional<AppendReplayCursor> continuation,
            int maxCommitsToScan);

    CompletableFuture<StreamMetadataSnapshot> transitionStreamState(
            String cluster,
            StreamStateTransitionRequest request);

    CompletableFuture<DerivedIndexRepairResult> repairDerivedStreamIndexes(
            String cluster,
            StreamId streamId,
            long targetOffset,
            Optional<DerivedIndexRepairCursor> continuation,
            int maxCommitsToScan);

    default CompletableFuture<DerivedIndexRepairResult> repairDerivedStreamIndexes(
            String cluster,
            StreamId streamId,
            long targetOffset,
            int maxCommitsToScan) {
        return repairDerivedStreamIndexes(
                cluster,
                streamId,
                targetOffset,
                Optional.empty(),
                maxCommitsToScan);
    }

    CompletableFuture<List<OffsetIndexEntry>> scanOffsetIndex(
            String cluster,
            StreamId streamId,
            long startOffset,
            int limit);

    CompletableFuture<CommittedEndOffsetRecord> getCommittedEndOffset(
            String cluster,
            StreamId streamId);

    CompletableFuture<TrimRecord> updateTrim(
            String cluster,
            StreamId streamId,
            long beforeOffset,
            String reason);

    CompletableFuture<TrimRecord> getTrim(
            String cluster,
            StreamId streamId);

    WatchRegistration watchStream(
            String cluster,
            StreamId streamId,
            MetadataWatcher watcher);

    @Override
    void close();
}
