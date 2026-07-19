/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BookKeeperPreparedPrimaryAppendTest {
    @Test
    void ownsAndReleasesExactEntryBuffersOnce() {
        var request = new PrimaryAppendRequest(new StreamId("stream"), batch(), session(), 10,
                new AppendAttemptId("attempt"), Duration.ofSeconds(5));
        var prepared = new BookKeeperPreparedPrimaryAppend(request);
        List<ByteBuf> borrowed = prepared.retainedEntries();
        try {
            assertThat(borrowed).extracting(ByteBuf::readableBytes).containsExactly(2, 1);
            assertThat(prepared.rangeChecksum(7)).isEqualTo(
                    BookKeeperRangeChecksums.computeBytes(7, List.of(new byte[] {1, 2}, new byte[] {3})));
        } finally {
            borrowed.forEach(ByteBuf::release);
        }
        prepared.close();
        prepared.close();
        assertThatThrownBy(prepared::retainedEntries).isInstanceOf(IllegalStateException.class);
    }

    private static AppendBatch batch() {
        return new AppendBatch(PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(new AppendEntry(new byte[] {1, 2}, 1, 1, Map.of()),
                        new AppendEntry(new byte[] {3}, 1, 2, Map.of())),
                2, 2, 1, 2, List.of(), Map.of(), Optional.empty());
    }
    private static AppendSession session() {
        return new AppendSession(new StreamId("stream"), "writer", 1, "token", 1,
                Long.MAX_VALUE);
    }
}
