/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class BookKeeperEntryRangeReadTargetCodecV1Test {
    private static final String GOLDEN_PAYLOAD_HEX =
            "4e524231000000077072696d617279000000000000000300000000000000050000000200000025"
                    + "4f4e455f4e45524555535f454e5452595f5045525f424f4f4b4b45455045525f454e545259"
                    + "0000000653484132353600000040"
                    + "3333333333333333333333333333333333333333333333333333333333333333"
                    + "3333333333333333333333333333333333333333333333333333333333333333";

    @Test
    void goldenAndRejectsMalformedValues() {
        ReadTargetCodecRegistry codecs = ReadTargetCodecRegistry.phase15();
        BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                1,
                "primary",
                3,
                5,
                2,
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                new Checksum(ChecksumType.SHA256, "33".repeat(32)));

        ReadTargetRecord encoded = codecs.encode(target);
        assertThat(HexFormat.of().formatHex(encoded.payload())).isEqualTo(GOLDEN_PAYLOAD_HEX);
        assertThat(codecs.decode(encoded)).isEqualTo(target);

        byte[] truncated = Arrays.copyOf(encoded.payload(), encoded.payload().length - 1);
        assertThatThrownBy(() -> codecs.decode(new ReadTargetRecord(
                encoded.targetType(),
                encoded.targetVersion(),
                encoded.payloadEncoding(),
                truncated,
                encoded.identityChecksumType(),
                ReadTargetCodecRegistry.identity(
                        encoded.targetType(), encoded.targetVersion(), truncated))))
                .isInstanceOf(MetadataCodecException.class);
        assertThatThrownBy(() -> new BookKeeperEntryRangeReadTarget(
                1,
                "primary",
                3,
                Long.MAX_VALUE,
                2,
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                target.rangeChecksum()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
