/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ReadTargetType;

/** NRB1 BookKeeper entry-range target payload. */
public final class BookKeeperEntryRangeReadTargetCodecV1
        implements ReadTargetCodec<BookKeeperEntryRangeReadTarget> {
    @Override public ReadTargetType targetType() { return ReadTargetType.BOOKKEEPER_ENTRY_RANGE; }
    @Override public int targetVersion() { return 1; }
    @Override public Class<BookKeeperEntryRangeReadTarget> targetClass() { return BookKeeperEntryRangeReadTarget.class; }

    @Override
    public byte[] encode(BookKeeperEntryRangeReadTarget target) {
        CanonicalTargetBinary.Writer writer = new CanonicalTargetBinary.Writer();
        writer.magic("NRB1");
        writer.string(target.clusterAlias());
        writer.longValue(target.ledgerId());
        writer.longValue(target.firstEntryId());
        writer.intValue(target.entryCount());
        writer.string(target.entryMapping().name());
        writer.string(target.rangeChecksum().type().name());
        writer.string(target.rangeChecksum().value());
        return writer.finish();
    }

    @Override
    public BookKeeperEntryRangeReadTarget decode(byte[] payload) {
        try {
            CanonicalTargetBinary.Reader reader = new CanonicalTargetBinary.Reader(payload);
            reader.magic("NRB1");
            String alias = reader.string("clusterAlias");
            long ledgerId = reader.longValue();
            long firstEntryId = reader.longValue();
            int entryCount = reader.intValue();
            BookKeeperEntryMapping mapping = BookKeeperEntryMapping.valueOf(reader.string("entryMapping"));
            Checksum checksum = new Checksum(
                    ChecksumType.valueOf(reader.string("rangeChecksumType")),
                    reader.string("rangeChecksumValue"));
            reader.finish();
            return new BookKeeperEntryRangeReadTarget(1, alias, ledgerId, firstEntryId, entryCount, mapping, checksum);
        } catch (MetadataCodecException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new MetadataCodecException("malformed BookKeeper target payload", e);
        }
    }
}
