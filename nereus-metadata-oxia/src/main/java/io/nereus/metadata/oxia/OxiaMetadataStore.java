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

package io.nereus.metadata.oxia;

import io.nereus.api.AppendSessionOptions;
import io.nereus.api.ObjectId;
import io.nereus.api.StreamCreateOptions;
import io.nereus.api.StreamId;
import io.nereus.api.StreamName;
import io.nereus.metadata.oxia.records.AppendSessionRecord;
import io.nereus.metadata.oxia.records.CommittedEndOffsetRecord;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.metadata.oxia.records.ObjectReferenceRecord;
import io.nereus.metadata.oxia.records.OffsetIndexRecord;
import io.nereus.metadata.oxia.records.StreamMetadataRecord;
import io.nereus.metadata.oxia.records.TrimRecord;
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

    CompletableFuture<List<OffsetIndexRecord>> scanOffsetIndex(
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
