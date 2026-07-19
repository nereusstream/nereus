/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Canonical versioned read-target identity shared by API, metadata codecs, readers, and reference adapters. */
public final class ReadTargetIdentities {
    private ReadTargetIdentities() { }

    public static Checksum sha256(ReadTarget target) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(target.type().name().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(Integer.toString(target.version()).getBytes(StandardCharsets.US_ASCII));
            digest.update((byte) 0);
            digest.update(canonicalPayload(target));
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required", e);
        }
    }

    /** Returns the exact V1 payload used inside the tagged Oxia read-target envelope. */
    public static byte[] canonicalPayload(ReadTarget target) {
        BinaryWriter writer = new BinaryWriter();
        if (target instanceof ObjectSliceReadTarget object) {
            writer.magic("NRO1");
            writer.string(object.objectId().value());
            writer.string(object.objectKey().value());
            writer.string(object.objectType().name());
            writer.string(object.physicalFormat());
            writer.string(object.logicalFormat());
            writer.string(object.sliceId());
            writer.longValue(object.objectOffset());
            writer.longValue(object.objectLength());
            writer.string(object.sliceChecksum().type().name());
            writer.string(object.sliceChecksum().value());
            EntryIndexRef index = object.entryIndexRef();
            writer.string(index.location().name());
            writer.string(index.objectId().map(ObjectId::value).orElse(""));
            writer.string(index.objectKey().map(ObjectKey::value).orElse(""));
            writer.byteArray(index.inlineData().orElseGet(() -> new byte[0]));
            writer.longValue(index.offset());
            writer.longValue(index.length());
            writer.string(index.checksum().type().name());
            writer.string(index.checksum().value());
            return writer.finish();
        }
        if (target instanceof BookKeeperEntryRangeReadTarget bookKeeper) {
            writer.magic("NRB1");
            writer.string(bookKeeper.clusterAlias());
            writer.longValue(bookKeeper.ledgerId());
            writer.longValue(bookKeeper.firstEntryId());
            writer.intValue(bookKeeper.entryCount());
            writer.string(bookKeeper.entryMapping().name());
            writer.string(bookKeeper.rangeChecksum().type().name());
            writer.string(bookKeeper.rangeChecksum().value());
            return writer.finish();
        }
        throw new IllegalArgumentException("unsupported read target " + target.getClass().getName());
    }

    private static final class BinaryWriter {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        void magic(String value) { bytes.writeBytes(value.getBytes(StandardCharsets.US_ASCII)); }
        void string(String value) { byteArray(value.getBytes(StandardCharsets.UTF_8)); }
        void byteArray(byte[] value) { intValue(value.length); bytes.writeBytes(value); }
        void intValue(int value) { bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()); }
        void longValue(long value) { bytes.writeBytes(ByteBuffer.allocate(Long.BYTES).putLong(value).array()); }
        byte[] finish() { return bytes.toByteArray(); }
    }
}
