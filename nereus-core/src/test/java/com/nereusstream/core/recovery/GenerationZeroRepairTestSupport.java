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

package com.nereusstream.core.recovery;

import com.nereusstream.api.AppendSession;
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
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.GenerationZeroProtectionIdentities;
import com.nereusstream.core.append.ProtectedGenerationZero;
import com.nereusstream.core.append.ProtectedStableAppend;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class GenerationZeroRepairTestSupport {
    static final String CLUSTER = "generation-zero-repair-cluster";
    static final StreamId STREAM =
            new StreamId("generation-zero-repair-stream");
    static final Duration TIMEOUT = Duration.ofSeconds(5);

    private GenerationZeroRepairTestSupport() {}

    static Fixture fixture(long trimOffset) {
        AtomicLong trim = new AtomicLong(trimOffset);
        AtomicInteger tailReads = new AtomicInteger();
        AtomicInteger materializations = new AtomicInteger();
        AtomicInteger protections = new AtomicInteger();
        AppendRecoveryCommit commit = commit();
        AppendRecoveryAnchor anchor = AppendRecoveryAnchor.genesis(STREAM);
        AppendRecoveryHead head = new AppendRecoveryHead(
                STREAM, "commit-1", 1, 10, 1, 9);
        AppendRecoveryTailPage page = new AppendRecoveryTailPage(
                anchor,
                head,
                List.of(commit),
                true,
                Optional.empty());
        OxiaMetadataStore metadata = metadata(
                trim,
                tailReads,
                materializations,
                page);
        GenerationMetadataStore generations = generations();
        RecordingPublisher publisher = new RecordingPublisher(protections);
        AnchorAwareCommitWalker walker = new AnchorAwareCommitWalker(
                CLUSTER, metadata, generations);
        GenerationZeroRepairScanner scanner = new GenerationZeroRepairScanner(
                CLUSTER,
                metadata,
                walker,
                publisher,
                16,
                4);
        return new Fixture(
                scanner,
                trim,
                tailReads,
                materializations,
                protections);
    }

    private static OxiaMetadataStore metadata(
            AtomicLong trim,
            AtomicInteger tailReads,
            AtomicInteger materializations,
            AppendRecoveryTailPage page) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getStreamSnapshot" -> CompletableFuture.completedFuture(
                            snapshot(trim.get()));
                    case "readAppendRecoveryTail" -> {
                        tailReads.incrementAndGet();
                        yield CompletableFuture.completedFuture(page);
                    }
                    case "materializeGenerationZero" -> {
                        materializations.incrementAndGet();
                        ReachableCommittedAppend reachable =
                                (ReachableCommittedAppend) arguments[1];
                        yield CompletableFuture.completedFuture(
                                materialized(
                                        reachable.committedAppend()));
                    }
                    case "close" -> null;
                    case "toString" -> "generation-zero-repair-l0";
                    default -> throw new UnsupportedOperationException(
                            method.getName());
                });
    }

    private static GenerationMetadataStore generations() {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRecoveryRoot" -> CompletableFuture.completedFuture(
                            Optional.empty());
                    case "close" -> null;
                    case "toString" -> "generation-zero-repair-generations";
                    default -> throw new UnsupportedOperationException(
                            method.getName());
                });
    }

    private static StreamMetadataSnapshot snapshot(long trimOffset) {
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/topic",
                        "stream-name-hash",
                        StreamState.ACTIVE.name(),
                        StorageProfile.OBJECT_WAL_ASYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        9),
                new CommittedEndOffsetRecord(
                        STREAM.value(), 1, 10, 1, 9),
                new TrimRecord(
                        STREAM.value(), trimOffset, "test", 1, 9));
    }

    private static AppendRecoveryCommit commit() {
        StreamCommitTargetRecord value = new StreamCommitTargetRecord(
                STREAM.value(),
                "commit-1",
                "",
                0,
                1,
                0,
                10,
                1,
                "writer",
                "writer-run",
                1,
                "fencing-hash",
                ReadTargetCodecRegistry.phase15().encode(target()),
                PayloadFormat.OPAQUE_RECORD_BATCH.name(),
                1,
                1,
                10,
                List.of(),
                ProjectionIdentity.encode(Optional.empty()),
                1,
                1,
                1,
                0);
        byte[] canonical = MetadataRecordCodecFactory.encodeEnvelope(
                value, StreamCommitTargetRecord.class);
        Checksum digest = sha256(canonical);
        return new AppendRecoveryCommit(
                "/commit/commit-1",
                AppendRecoveryCommitEncoding
                        .GENERIC_STREAM_COMMIT_TARGET_V1,
                value,
                7,
                digest,
                ByteBuffer.wrap(canonical),
                digest);
    }

    private static ObjectSliceReadTarget target() {
        Checksum sha = sha();
        return new ObjectSliceReadTarget(
                1,
                new ObjectId("repair-object"),
                new ObjectKey("wal/repair-object"),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "slice-0",
                0,
                128,
                sha,
                new EntryIndexRef(
                        EntryIndexLocation.INLINE,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new byte[] {1}),
                        0,
                        0,
                        sha));
    }

    private static MaterializedGenerationZero materialized(
            com.nereusstream.metadata.oxia.CommittedAppend committed) {
        return new MaterializedGenerationZero(
                committed,
                "/index/generation-zero-repair-stream/1/0",
                11,
                sha());
    }

    private static ProtectedGenerationZero protectedGeneration(
            MaterializedGenerationZero materialized) {
        ObjectKey key = ((ObjectSliceReadTarget)
                        materialized.committedAppend().readTarget())
                .objectKey();
        return new ProtectedGenerationZero(
                materialized,
                new ObjectProtectionIdentity(
                        ObjectKeyHash.from(key),
                        ObjectProtectionType.VISIBLE_GENERATION,
                        GenerationZeroProtectionIdentities
                                .visibleGenerationReferenceId(
                                        materialized.committedAppend()
                                                .streamId(),
                                        materialized.indexKey(),
                                        materialized.indexRecordSha256())),
                3,
                1,
                4,
                sha());
    }

    private static Checksum sha() {
        return new Checksum(ChecksumType.SHA256, "a".repeat(64));
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(bytes)));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    record Fixture(
            GenerationZeroRepairScanner scanner,
            AtomicLong trimOffset,
            AtomicInteger tailReads,
            AtomicInteger materializations,
            AtomicInteger protections) {}

    private static final class RecordingPublisher
            implements GenerationZeroPhysicalReferencePublisher {
        private final AtomicInteger protections;

        private RecordingPublisher(AtomicInteger protections) {
            this.protections = protections;
        }

        @Override
        public CompletableFuture<Void> authorizeUpload(
                AppendSession session,
                PhysicalObjectIdentity object,
                Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ProtectedStableAppend> protectBeforeHead(
                PreparedStableAppend append, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
                MaterializedGenerationZero append, Duration timeout) {
            protections.incrementAndGet();
            return CompletableFuture.completedFuture(
                    protectedGeneration(append));
        }
    }
}
