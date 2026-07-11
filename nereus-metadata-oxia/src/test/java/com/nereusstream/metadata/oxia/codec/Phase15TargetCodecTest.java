/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class Phase15TargetCodecTest {
    private final ReadTargetCodecRegistry codecs = ReadTargetCodecRegistry.phase15();

    @Test
    void bothReservedTargetTypesHaveStableRoundTrips() {
        ObjectSliceReadTarget object = new ObjectSliceReadTarget(
                1, new ObjectId("object"), new ObjectKey("key"), ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1", "OPAQUE_SLICE", "slice", 4, 8,
                new Checksum(ChecksumType.CRC32C, "11111111"),
                new EntryIndexRef(EntryIndexLocation.OBJECT_FOOTER, Optional.empty(), Optional.empty(),
                        Optional.empty(), 20, 4, new Checksum(ChecksumType.CRC32C, "22222222")));
        BookKeeperEntryRangeReadTarget bookKeeper = new BookKeeperEntryRangeReadTarget(
                1, "primary", 3, 5, 2,
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                new Checksum(ChecksumType.SHA256, "33".repeat(32)));

        assertThat(codecs.decode(codecs.encode(object))).isEqualTo(object);
        assertThat(codecs.decode(codecs.encode(bookKeeper))).isEqualTo(bookKeeper);
        assertThat(codecs.encode(object).payload()).startsWith('N', 'R', 'O', '1');
        assertThat(codecs.encode(bookKeeper).payload()).startsWith('N', 'R', 'B', '1');
    }

    @Test
    void identityMismatchAndTrailingBytesFailClosed() {
        BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                1, "primary", 3, 5, 2,
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                new Checksum(ChecksumType.SHA256, "33".repeat(32)));
        ReadTargetRecord record = codecs.encode(target);
        assertThatThrownBy(() -> codecs.decode(new ReadTargetRecord(
                record.targetType(), record.targetVersion(), record.payloadEncoding(), record.payload(),
                record.identityChecksumType(), "00".repeat(32))))
                .isInstanceOf(MetadataCodecException.class);
        byte[] trailing = java.util.Arrays.copyOf(record.payload(), record.payload().length + 1);
        String identity = ReadTargetCodecRegistry.identity(record.targetType(), record.targetVersion(), trailing);
        assertThatThrownBy(() -> codecs.decode(new ReadTargetRecord(
                record.targetType(), record.targetVersion(), record.payloadEncoding(), trailing,
                record.identityChecksumType(), identity)))
                .isInstanceOf(MetadataCodecException.class);
    }
}
