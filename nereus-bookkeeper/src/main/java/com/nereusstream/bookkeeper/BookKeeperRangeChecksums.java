/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** SHA-256 over the exact NBKR1 framed contiguous BookKeeper entry sequence. */
public final class BookKeeperRangeChecksums {
    private BookKeeperRangeChecksums() { }

    public static Checksum compute(long firstEntryId, List<? extends ByteBuf> entries) {
        Objects.requireNonNull(entries, "entries");
        if (firstEntryId < 0 || entries.isEmpty()) throw new IllegalArgumentException("invalid BookKeeper entry range");
        MessageDigest digest = digest();
        digest.update(new byte[] {'N', 'B', 'K', 'R', '1'});
        digest.update(u32(entries.size()));
        for (int index = 0; index < entries.size(); index++) {
            ByteBuf entry = Objects.requireNonNull(entries.get(index), "entry");
            long entryId = Math.addExact(firstEntryId, index);
            int length = entry.readableBytes();
            digest.update(u64(entryId));
            digest.update(u32(length));
            for (ByteBuffer nio : entry.nioBuffers(entry.readerIndex(), length)) {
                digest.update(nio.asReadOnlyBuffer());
            }
        }
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    public static Checksum computeBytes(long firstEntryId, List<byte[]> entries) {
        Objects.requireNonNull(entries, "entries");
        if (firstEntryId < 0 || entries.isEmpty()) throw new IllegalArgumentException("invalid BookKeeper entry range");
        MessageDigest digest = digest();
        digest.update(new byte[] {'N', 'B', 'K', 'R', '1'});
        digest.update(u32(entries.size()));
        for (int index = 0; index < entries.size(); index++) {
            byte[] entry = Objects.requireNonNull(entries.get(index), "entry");
            digest.update(u64(Math.addExact(firstEntryId, index)));
            digest.update(u32(entry.length));
            digest.update(entry);
        }
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    private static byte[] u32(long value) {
        if (value < 0 || value > 0xffff_ffffL) throw new IllegalArgumentException("u32 value is out of range");
        return ByteBuffer.allocate(Integer.BYTES).putInt((int) value).array();
    }
    private static byte[] u64(long value) {
        if (value < 0) throw new IllegalArgumentException("u64 value exceeds the supported positive domain");
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }
    private static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 is required", e); }
    }
}
