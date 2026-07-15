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

package com.nereusstream.metadata.oxia.testing;

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
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.CommitSliceResult;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared fake/real operation-level contract used by M7 gates. */
public final class OxiaMetadataStoreContractScenario {
    private OxiaMetadataStoreContractScenario() {
    }

    public static ContractResult run(OxiaMetadataStore store, String cluster) {
        String suffix = Integer.toUnsignedString(cluster.hashCode(), 36);
        StreamName name = new StreamName("contract-" + suffix);
        StreamId streamId = new StreamId(store.createOrGetStream(
                cluster,
                name,
                new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of("contract", "m7")))
                .join().streamId());
        StreamId replayedId = new StreamId(store.createOrGetStream(
                cluster,
                name,
                new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                .join().streamId());
        require(streamId.equals(replayedId), "create-or-get must be deterministic");

        AppendSessionRecord session = store.acquireAppendSession(
                cluster,
                streamId,
                new AppendSessionOptions("contract-writer", Duration.ofSeconds(30), false)).join();
        CommitSliceRequest request = request(streamId, session, suffix);
        store.putObjectManifest(cluster, manifest(request)).join();
        CommitSliceResult committed = store.commitStreamSlice(cluster, request).join();
        CommitSliceResult replay = store.commitStreamSlice(cluster, request).join();
        require(committed.equals(replay), "same physical slice replay must return the original commit");
        require(store.scanOffsetIndex(cluster, streamId, 0, 10).join().size() == 1,
                "successful commit must materialize one offset index");
        require(store.repairObjectReferences(cluster, request.objectId()).join().visibleSlices().size() == 1,
                "reference repair must find the reachable commit");
        AppendRecoveryTailPage recoveryTail = store.readAppendRecoveryTail(
                cluster,
                streamId,
                AppendRecoveryAnchor.genesis(streamId),
                Optional.empty(),
                1).join();
        require(recoveryTail.anchorReached()
                        && recoveryTail.continuation().isEmpty()
                        && recoveryTail.commitsNewestFirst().size() == 1,
                "anchor-aware recovery walk must bridge the legacy live tail to genesis");
        require(recoveryTail.commitsNewestFirst().get(0).sourceEncoding()
                        == AppendRecoveryCommitEncoding.LEGACY_STREAM_COMMIT_V1
                        && recoveryTail.commitsNewestFirst().get(0).canonicalCommit().metadataVersion() == 0,
                "legacy recovery evidence must be canonicalized as a generic NRC1 envelope");
        store.updateTrim(cluster, streamId, 1, "contract").join();
        require(store.getTrim(cluster, streamId).join().trimOffset() == 1,
                "trim must persist the requested low-watermark");
        require(store.getCommittedEndOffset(cluster, streamId).join().committedEndOffset() == 1,
                "trim must not change committed end");
        StreamMetadataSnapshot snapshot = store.getStreamSnapshot(cluster, streamId).join();
        require(snapshot.trim().trimOffset() == 1 && snapshot.committedEnd().committedEndOffset() == 1,
                "stream snapshot must contain trim and committed end from one head");
        require(snapshot.metadata().metadataVersion() == snapshot.committedEnd().metadataVersion()
                        && snapshot.metadataVersion() == snapshot.trim().metadataVersion(),
                "stream snapshot views must share one head version");
        return new ContractResult(streamId, request.objectId(), committed.commitVersion());
    }

    private static CommitSliceRequest request(
            StreamId streamId,
            AppendSessionRecord session,
            String suffix) {
        ObjectId objectId = new ObjectId("contract-object-" + suffix);
        ObjectKey objectKey = new ObjectKey("contract-object-" + suffix + "-key");
        return new CommitSliceRequest(
                streamId,
                "contract-writer",
                "contract-run",
                session.epoch(),
                session.fencingToken(),
                0,
                "contract-slice",
                1,
                1,
                3,
                List.of(),
                objectId,
                objectKey,
                checksum("11111111"),
                8,
                16,
                new EntryIndexRef(
                        EntryIndexLocation.OBJECT_FOOTER,
                        Optional.of(objectId),
                        Optional.of(objectKey),
                        Optional.empty(),
                        32,
                        8,
                        checksum("22222222")),
                checksum("33333333"),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                Optional.empty());
    }

    private static ObjectManifestRecord manifest(CommitSliceRequest request) {
        long now = System.currentTimeMillis();
        return new ObjectManifestRecord(
                request.objectId().value(),
                request.objectKey().value(),
                ObjectType.MULTI_STREAM_WAL_OBJECT.name(),
                "UPLOADED",
                1,
                0,
                "contract-writer-version",
                request.writerId(),
                request.writerRunIdHash(),
                request.epoch(),
                now,
                now,
                64,
                request.objectChecksum().type().name(),
                request.objectChecksum().value(),
                ChecksumType.CRC32C.name(),
                "44444444",
                List.of(new StreamSliceManifestRecord(
                        0,
                        request.streamId().value(),
                        request.sliceId(),
                        request.epoch(),
                        request.objectOffset(),
                        request.objectLength(),
                        request.recordCount(),
                        request.entryCount(),
                        request.logicalBytes(),
                        request.schemaRefs(),
                        EntryIndexReferenceRecord.fromApi(request.entryIndexRef()),
                        request.sliceChecksum().type().name(),
                        request.sliceChecksum().value(),
                        request.payloadFormat().name(),
                        "UPLOADED")),
                now + 60_000,
                0);
    }

    private static Checksum checksum(String value) {
        return new Checksum(ChecksumType.CRC32C, value);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public record ContractResult(StreamId streamId, ObjectId objectId, long commitVersion) {
    }
}
