/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BookKeeperReadResultContractTest {
    @Test
    void usesOnlyCanonicalBookKeeperSourceIdentity() {
        byte[] payload = {9, 8, 7};
        Checksum rangeChecksum = BookKeeperRangeChecksums.computeBytes(3, List.of(payload));
        var target = new BookKeeperEntryRangeReadTarget(1, "primary", 17, 3, 1,
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY, rangeChecksum);
        Checksum identity = ReadTargetIdentities.sha256(target);
        var source = new ReadSourceRef(new OffsetRange(51, 52), 0, 19, target, identity);
        var batch = new ReadBatch(new OffsetRange(51, 52), PayloadFormat.OPAQUE_RECORD_BATCH,
                payload, List.of(), Optional.empty(), source);
        var result = new PhysicalReadResult(List.of(batch),
                List.of(new PhysicalReadStats(identity, payload.length, 0, payload.length, 0, payload.length)));

        assertThat(result.batches().getFirst().source().target()).isSameAs(target);
        assertThat(result.rangeStats().getFirst().targetIdentity()).isEqualTo(identity);
        assertThatThrownBy(batch::sourceObjectId).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(batch::entryIndexRef).isInstanceOf(IllegalStateException.class);
    }
}
