/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import org.junit.jupiter.api.Test;

class BookKeeperRangeChecksumsTest {
    @Test
    void framesExactEntrySequence() {
        List<byte[]> entries = List.of(new byte[] {1, 2}, new byte[] {3}, new byte[0]);
        var checksum = BookKeeperRangeChecksums.computeBytes(41, entries);

        assertThat(checksum.value()).isEqualTo(
                "2a06a39e1f3c9c904e6a9489c1286d4d59d4fc127be71d56afb1a713d174ec6c");
        assertThat(BookKeeperRangeChecksums.computeBytes(42, entries)).isNotEqualTo(checksum);
        assertThat(BookKeeperRangeChecksums.computeBytes(41, List.of(entries.get(1), entries.get(0), entries.get(2))))
                .isNotEqualTo(checksum);
        assertThat(BookKeeperRangeChecksums.computeBytes(41, entries.subList(0, 2))).isNotEqualTo(checksum);

        List<ByteBuf> buffers = entries.stream().map(Unpooled::wrappedBuffer).toList();
        try {
            assertThat(BookKeeperRangeChecksums.compute(41, buffers)).isEqualTo(checksum);
        } finally {
            buffers.forEach(ByteBuf::release);
        }
    }
}
