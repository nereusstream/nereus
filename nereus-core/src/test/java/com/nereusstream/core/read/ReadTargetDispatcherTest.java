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
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.wal.PrimaryWalReader;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReadTargetDispatcherTest {
    @Test
    void missingTargetAdapterFailsBeforeAnyRegisteredReaderIsInvoked() {
        AtomicInteger calls = new AtomicInteger();
        PrimaryWalReader object = new PrimaryWalReader() {
            @Override public ReadTargetType targetType() { return ReadTargetType.OBJECT_SLICE; }
            @Override public long reservationBytes(ResolvedRange range) { calls.incrementAndGet(); return 1; }
            @Override public CompletableFuture<WalReadResult> readWithStats(
                    long startOffset, List<ResolvedRange> ranges, com.nereusstream.api.ReadOptions options) {
                calls.incrementAndGet(); return CompletableFuture.completedFuture(new WalReadResult(List.of(), List.of()));
            }
        };
        ReadTargetDispatcher dispatcher = new ReadTargetDispatcher(
                new PrimaryWalRegistry(List.of(), List.of(object)));
        ResolvedRange bookKeeper = new ResolvedRange(new OffsetRange(0, 1), 0,
                new BookKeeperEntryRangeReadTarget(1, "primary", 1, 0, 1,
                        BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                        new Checksum(ChecksumType.SHA256, "11".repeat(32))),
                PayloadFormat.OPAQUE_RECORD_BATCH, 1, 1, 1, List.of(), Optional.empty(), 1);

        assertThatThrownBy(() -> dispatcher.reservationBytes(List.of(bookKeeper)))
                .isInstanceOfSatisfying(NereusException.class,
                        error -> assertThat(error.code()).isEqualTo(ErrorCode.UNSUPPORTED_READ_TARGET));
        assertThat(calls).hasValue(0);
    }
}
