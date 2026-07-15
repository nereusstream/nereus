/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ReadTargetReaderRegistryTest {
    @Test
    void keepsWalAndCompactedReadersDistinctUnderTheSameTargetType() {
        TestReader wal = new TestReader(key(ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1"));
        TestReader compacted = new TestReader(key(
                ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1"));
        ReadTargetReaderRegistry registry = new ReadTargetReaderRegistry(List.of(wal, compacted));

        assertThat(registry.require(target(ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1")))
                .isSameAs(wal);
        assertThat(registry.require(target(
                ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1")))
                .isSameAs(compacted);
    }

    @Test
    void duplicateExactKeyAndUnknownFormatFailClosed() {
        ReadTargetReaderKey key = key(ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1");
        assertThatThrownBy(() -> new ReadTargetReaderRegistry(
                List.of(new TestReader(key), new TestReader(key))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");

        ReadTargetReaderRegistry registry = new ReadTargetReaderRegistry(
                List.of(new TestReader(key)));
        assertThatThrownBy(() -> registry.require(target(
                ObjectType.STREAM_COMPACTED_OBJECT, "UNKNOWN_FORMAT")))
                .isInstanceOfSatisfying(NereusException.class,
                        error -> assertThat(error.code()).isEqualTo(ErrorCode.UNSUPPORTED_READ_TARGET));
    }

    static ReadTargetReaderKey key(ObjectType type, String format) {
        return new ReadTargetReaderKey(
                com.nereusstream.api.target.ReadTargetType.OBJECT_SLICE,
                1,
                Optional.of(type),
                Optional.of(format));
    }

    static ObjectSliceReadTarget target(ObjectType type, String format) {
        ObjectId objectId = new ObjectId("object-" + type.name().toLowerCase());
        ObjectKey objectKey = new ObjectKey("key-" + type.name().toLowerCase());
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.of(objectId),
                Optional.of(objectKey),
                Optional.empty(),
                0,
                8,
                new Checksum(ChecksumType.CRC32C, "00000001"));
        return new ObjectSliceReadTarget(
                1,
                objectId,
                objectKey,
                type,
                format,
                "OPAQUE_SLICE",
                "slice",
                0,
                8,
                new Checksum(ChecksumType.CRC32C, "00000002"),
                index);
    }

    static final class TestReader implements ReadTargetReader {
        private final ReadTargetReaderKey key;

        TestReader(ReadTargetReaderKey key) {
            this.key = key;
        }

        @Override
        public ReadTargetReaderKey key() {
            return key;
        }

        @Override
        public long reservationBytes(ResolvedRange range) {
            return 1;
        }

        @Override
        public CompletableFuture<WalReadResult> readWithStats(
                long startOffset, List<ResolvedRange> ranges, ReadOptions options) {
            return CompletableFuture.completedFuture(new WalReadResult(List.of(), List.of()));
        }
    }
}
