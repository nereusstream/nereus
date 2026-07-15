/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PinnedResolvedRangeTest {
    @Test
    void releaseIsIdempotentAndOwnsTheExactObjectLease() {
        ObjectSliceReadTarget target = ReadTargetReaderRegistryTest.target(
                ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1");
        ResolvedRange resolved = new ResolvedRange(
                new OffsetRange(0, 1), 2, target, PayloadFormat.OPAQUE_RECORD_BATCH,
                1, 1, 1, List.of(), Optional.empty(), 1);
        GenerationReadCandidate candidate = new GenerationReadCandidate(
                ReadView.COMMITTED,
                resolved,
                "/index/1/2",
                3,
                new Checksum(ChecksumType.SHA256, "11".repeat(32)),
                false,
                Optional.of(new PublicationId("aaaaaaaaaaaaaaaaaaaaaaaaaa")));
        TestLease lease = new TestLease(PhysicalObjectIdentity.create(
                target.objectKey(),
                Optional.of(target.objectId()),
                PhysicalObjectKind.COMMITTED_COMPACTED,
                target.objectLength(),
                new Checksum(ChecksumType.CRC32C, "00000002"),
                Optional.empty(),
                Optional.empty()));
        PinnedResolvedRange pinned = new PinnedResolvedRange(candidate, lease);

        assertThat(pinned.release()).isSameAs(pinned.release());
        pinned.close();
        assertThat(lease.releases).hasValue(1);
        assertThat(pinned.isReleased()).isTrue();
        assertThat(ObjectKeyHash.from(target.objectKey())).isEqualTo(lease.object().objectKeyHash());
    }

    private static final class TestLease implements ObjectReadLease {
        private final PhysicalObjectIdentity object;
        private final AtomicInteger releases = new AtomicInteger();

        private TestLease(PhysicalObjectIdentity object) {
            this.object = object;
        }

        @Override
        public PhysicalObjectIdentity object() {
            return object;
        }

        @Override
        public String leaseId() {
            return "lease";
        }

        @Override
        public long maximumReadDeadlineMillis() {
            return Long.MAX_VALUE;
        }

        @Override
        public CompletableFuture<Void> release() {
            releases.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean isReleased() {
            return releases.get() > 0;
        }
    }
}
