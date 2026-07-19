/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ProviderNeutralReadAccountingTest {
    @Test
    void rejectsIdentityAndByteDrift() {
        ResolvedRange range = range(0, "11");
        Checksum identity = ReadTargetIdentities.sha256(range.readTarget());
        PhysicalReadStats exact = stats(identity, 0);

        assertInvariant(() -> ProviderNeutralReadAccounting.validate(
                List.of(range), new PhysicalReadResult(List.of(batch(range)), List.of())));
        assertInvariant(() -> ProviderNeutralReadAccounting.validate(
                List.of(range), new PhysicalReadResult(List.of(), List.of(exact, exact))));
        assertInvariant(() -> ProviderNeutralReadAccounting.validate(
                List.of(range),
                new PhysicalReadResult(List.of(), List.of(stats(
                        ReadTargetIdentities.sha256(range(0, "22").readTarget()), 0)))));
        assertInvariant(() -> ProviderNeutralReadAccounting.validate(
                List.of(range), new PhysicalReadResult(List.of(), List.of(stats(identity, 1)))));
    }

    @Test
    void acceptsOneExactStatForEveryResolvedTarget() {
        ResolvedRange first = range(0, "11");
        ResolvedRange second = range(1, "22");
        PhysicalReadResult result = new PhysicalReadResult(
                List.of(),
                List.of(
                        stats(ReadTargetIdentities.sha256(first.readTarget()), 0),
                        stats(ReadTargetIdentities.sha256(second.readTarget()), 0)));

        ProviderNeutralReadAccounting.validate(List.of(first, second), result);
    }

    private static ResolvedRange range(long offset, String checksumByte) {
        return new ResolvedRange(
                new OffsetRange(offset, offset + 1),
                0,
                new BookKeeperEntryRangeReadTarget(
                        1,
                        "primary",
                        7,
                        offset,
                        1,
                        BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                        new Checksum(ChecksumType.SHA256, checksumByte.repeat(32))),
                PayloadFormat.PULSAR_ENTRY_BATCH,
                1,
                1,
                0,
                List.of(),
                Optional.empty(),
                1);
    }

    private static PhysicalReadStats stats(Checksum identity, long returnedBytes) {
        return new PhysicalReadStats(identity, 0, 0, 0, 0, returnedBytes);
    }

    private static ReadBatch batch(ResolvedRange range) {
        return new ReadBatch(
                range.offsetRange(),
                range.payloadFormat(),
                new byte[0],
                range.schemaRefs(),
                range.projectionRef(),
                new ReadSourceRef(
                        range.offsetRange(),
                        range.generation(),
                        range.commitVersion(),
                        range.readTarget(),
                        ReadTargetIdentities.sha256(range.readTarget())));
    }

    private static void assertInvariant(Runnable operation) {
        assertThatThrownBy(operation::run)
                .isInstanceOfSatisfying(
                        NereusException.class,
                        failure -> assertThat(failure.code())
                                .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
    }
}
