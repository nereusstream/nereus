/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ResolvedObjectRange;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.wal.object.ObjectWalReaderAdapter;
import com.nereusstream.objectstore.wal.WalObjectReader;
import com.nereusstream.objectstore.wal.WalReadResult;
import com.nereusstream.objectstore.wal.WalSliceReadStats;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ProviderNeutralObjectReadRegressionTest {
    @Test
    void preservesLogicalAndPhysicalAccounting() {
        ObjectSliceReadTarget target = target();
        ResolvedRange range = new ResolvedRange(
                new OffsetRange(5, 6),
                0,
                target,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                3,
                List.of(),
                Optional.empty(),
                7);
        ReadSourceRef source = new ReadSourceRef(
                range.offsetRange(),
                range.generation(),
                range.commitVersion(),
                target,
                ReadTargetIdentities.sha256(target));
        ReadBatch batch = new ReadBatch(
                range.offsetRange(),
                range.payloadFormat(),
                new byte[] {1, 2, 3},
                List.of(),
                Optional.empty(),
                source,
                target.objectOffset(),
                3);
        WalReadResult legacy = new WalReadResult(
                List.of(batch),
                List.of(new WalSliceReadStats(
                        target.objectId(),
                        target.objectOffset(),
                        target.objectLength(),
                        target.entryIndexRef().length(),
                        12,
                        4,
                        3)));
        WalObjectReader reader = new WalObjectReader() {
            @Override
            public CompletableFuture<WalReadResult> readWithStats(
                    long startOffset,
                    List<ResolvedObjectRange> ranges,
                    ReadOptions options) {
                assertThat(ranges).hasSize(1);
                assertThat(ranges.getFirst().readTarget()).isEqualTo(target);
                return CompletableFuture.completedFuture(legacy);
            }
        };

        PhysicalReadResult result = new ObjectWalReaderAdapter(reader).readPhysicalWithStats(
                new StreamId("stream"),
                5,
                List.of(range),
                new ReadOptions(10, 10, ReadIsolation.COMMITTED, Duration.ofSeconds(1)))
                .join();

        assertThat(result.batches()).containsExactly(batch);
        assertThat(result.rangeStats()).singleElement().satisfies(stats -> {
            assertThat(stats.targetIdentity()).isEqualTo(ReadTargetIdentities.sha256(target));
            assertThat(stats.resolvedPayloadBytes()).isEqualTo(target.objectLength());
            assertThat(stats.resolvedAuxiliaryBytes()).isEqualTo(target.entryIndexRef().length());
            assertThat(stats.physicalPayloadBytesRead()).isEqualTo(12);
            assertThat(stats.physicalAuxiliaryBytesRead()).isEqualTo(4);
            assertThat(stats.returnedPayloadBytes()).isEqualTo(3);
        });
    }

    private static ObjectSliceReadTarget target() {
        ObjectId objectId = new ObjectId("object");
        ObjectKey objectKey = new ObjectKey("key");
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.of(objectId),
                Optional.of(objectKey),
                Optional.empty(),
                20,
                4,
                new Checksum(ChecksumType.CRC32C, "22222222"));
        return new ObjectSliceReadTarget(
                1,
                objectId,
                objectKey,
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "slice",
                8,
                16,
                new Checksum(ChecksumType.CRC32C, "11111111"),
                index);
    }
}
