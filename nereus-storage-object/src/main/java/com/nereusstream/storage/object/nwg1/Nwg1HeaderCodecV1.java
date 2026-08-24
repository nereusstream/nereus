/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32C;

/** Strict, allocation-bounded encoder/decoder for the ADR-0089 Header table. */
public final class Nwg1HeaderCodecV1 {
    private static final int CRC_OFFSET = 252;

    private Nwg1HeaderCodecV1() {}

    public static byte[] encode(Nwg1HeaderV1 header) {
        ByteBuffer out = ByteBuffer.allocate(Nwg1ConstantsV1.HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        out.put(Nwg1ConstantsV1.HEADER_MAGIC);
        putU16(out, Nwg1ConstantsV1.WIRE_VERSION);
        putU16(out, Nwg1ConstantsV1.HEADER_BYTES);
        out.putInt(0);
        putU16(out, header.protocolKind());
        putU16(out, 0);
        putU32(out, header.shardId());
        out.putLong(header.shardRunEpoch());
        out.putLong(header.laneSequence());
        out.putInt(header.packingPolicyVersion());
        out.putLong(header.resolvedTargetPayloadBytes());
        out.putLong(header.resolvedLingerNanos());
        out.putInt(0);
        out.putLong(header.actualPayloadBytesAtPlanSeal());
        out.putLong(header.actualCloseLingerNanos());
        putU32(out, header.directoryPlaintextLength());
        putU32(out, header.directoryStoredLength());
        putU32(out, header.bindingContextCount());
        putU32(out, header.appendUnitCount());
        putU32(out, header.frameCount());
        out.putLong(header.directoryPrefixEnd());
        out.putLong(header.canonicalBodyLength());
        putU8(out, header.laneId());
        putU8(out, Nwg1ConstantsV1.CLOSED_KIND);
        putU8(out, Nwg1ConstantsV1.CLOSED_VERSION);
        putU8(out, Nwg1ConstantsV1.CLOSED_KIND);
        putU8(out, Nwg1ConstantsV1.CLOSED_VERSION);
        putU8(out, Nwg1ConstantsV1.CLOSED_KIND);
        putU8(out, Nwg1ConstantsV1.CLOSED_VERSION);
        putU8(out, Nwg1ConstantsV1.CLOSED_KIND);
        putU8(out, Nwg1ConstantsV1.CLOSED_VERSION);
        putU8(out, Nwg1ConstantsV1.CLOSED_VERSION);
        putU8(out, Nwg1ConstantsV1.GCM_TAG_BYTES);
        putU8(out, header.closeReason());
        out.put(header.protocolCellCommitment());
        out.put(header.cellProviderScopeId());
        out.put(header.walRunRootSha256());
        out.put(header.wrappedEnvelopeCommitment());
        out.putInt(0);
        byte[] encoded = out.array();
        ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN).putInt(CRC_OFFSET, crc32c(encoded));
        return encoded;
    }

    public static Nwg1HeaderV1 decode(byte[] encoded) {
        if (encoded == null || encoded.length < Nwg1ConstantsV1.HEADER_BYTES) {
            fail(Nwg1RejectionV1.TRUNCATED_INPUT, Nwg1ValidationStageV1.HEADER_GRAMMAR, "short Header");
        }
        if (encoded.length > Nwg1ConstantsV1.HEADER_BYTES) {
            fail(Nwg1RejectionV1.TRAILING_BYTES, Nwg1ValidationStageV1.HEADER_GRAMMAR, "Header trailing bytes");
        }
        ByteBuffer in = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        byte[] magic = new byte[4];
        in.get(magic);
        if (!Arrays.equals(magic, Nwg1ConstantsV1.HEADER_MAGIC)) {
            fail(Nwg1RejectionV1.UNSUPPORTED_VERSION, Nwg1ValidationStageV1.HEADER_GRAMMAR, "wrong magic");
        }
        int version = u16(in);
        int length = u16(in);
        if (version != 1 || length != Nwg1ConstantsV1.HEADER_BYTES) {
            fail(Nwg1RejectionV1.UNSUPPORTED_VERSION, Nwg1ValidationStageV1.HEADER_GRAMMAR, "wrong version/length");
        }
        requireZero(in.getInt(), "headerFlags");
        int protocol = u16(in);
        requireZero(u16(in), "requiredZeroA");
        long shard = u32(in);
        long epoch = nonNegative(in.getLong(), "shardRunEpoch");
        long sequence = nonNegative(in.getLong(), "laneSequence");
        int policy = in.getInt();
        long target = nonNegative(in.getLong(), "resolvedTargetPayloadBytes");
        long linger = nonNegative(in.getLong(), "resolvedLingerNanos");
        requireZero(in.getInt(), "requiredZeroB");
        long actualPayload = nonNegative(in.getLong(), "actualPayloadBytesAtPlanSeal");
        long actualLinger = nonNegative(in.getLong(), "actualCloseLingerNanos");
        long directoryPlain = u32(in);
        long directoryStored = u32(in);
        long bindings = u32(in);
        long units = u32(in);
        long frames = u32(in);
        long prefixEnd = nonNegative(in.getLong(), "directoryPrefixEnd");
        long bodyLength = nonNegative(in.getLong(), "canonicalBodyLength");
        int lane = u8(in);
        requireClosedByte(in, "codecKind");
        requireClosedByte(in, "codecVersion");
        requireClosedByte(in, "objectDigestKind");
        requireClosedByte(in, "objectDigestVersion");
        requireClosedByte(in, "aeadKind");
        requireClosedByte(in, "aeadVersion");
        requireClosedByte(in, "kdfKind");
        requireClosedByte(in, "kdfVersion");
        requireClosedByte(in, "nonceLayoutVersion");
        if (u8(in) != Nwg1ConstantsV1.GCM_TAG_BYTES) {
            fail(Nwg1RejectionV1.UNKNOWN_CODE, Nwg1ValidationStageV1.HEADER_GRAMMAR, "wrong AEAD tag bytes");
        }
        int closeReason = u8(in);
        byte[] cell = bytes(in, 32);
        byte[] scope = bytes(in, 32);
        byte[] root = bytes(in, 32);
        byte[] envelope = bytes(in, 32);
        int storedCrc = in.getInt();
        int expectedCrc = crc32c(encoded);
        if (storedCrc != expectedCrc) {
            fail(Nwg1RejectionV1.CHECKSUM_MISMATCH, Nwg1ValidationStageV1.HEADER_CRC, "Header CRC32C mismatch");
        }
        Nwg1HeaderV1 result = new Nwg1HeaderV1(
                protocol,
                shard,
                epoch,
                sequence,
                policy,
                target,
                linger,
                actualPayload,
                actualLinger,
                directoryPlain,
                directoryStored,
                bindings,
                units,
                frames,
                prefixEnd,
                bodyLength,
                lane,
                closeReason,
                cell,
                scope,
                root,
                envelope);
        if (!Arrays.equals(encoded, encode(result))) {
            fail(
                    Nwg1RejectionV1.NON_CANONICAL_ENCODING,
                    Nwg1ValidationStageV1.HEADER_GRAMMAR,
                    "Header does not canonically re-encode");
        }
        return result;
    }

    public static int crc32c(byte[] exactHeader) {
        if (exactHeader.length != Nwg1ConstantsV1.HEADER_BYTES) {
            throw new IllegalArgumentException("Header length");
        }
        CRC32C crc = new CRC32C();
        crc.update(exactHeader, 0, CRC_OFFSET);
        crc.update(new byte[4], 0, 4);
        return (int) crc.getValue();
    }

    private static void putU8(ByteBuffer out, int value) {
        if (value < 0 || value > 255) {
            throw new IllegalArgumentException("u8 overflow");
        }
        out.put((byte) value);
    }

    private static void putU16(ByteBuffer out, int value) {
        if (value < 0 || value > 65_535) {
            throw new IllegalArgumentException("u16 overflow");
        }
        out.putShort((short) value);
    }

    private static void putU32(ByteBuffer out, long value) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("u32 overflow");
        }
        out.putInt((int) value);
    }

    private static int u8(ByteBuffer in) {
        return Byte.toUnsignedInt(in.get());
    }

    private static int u16(ByteBuffer in) {
        return Short.toUnsignedInt(in.getShort());
    }

    private static long u32(ByteBuffer in) {
        return Integer.toUnsignedLong(in.getInt());
    }

    private static byte[] bytes(ByteBuffer in, int length) {
        byte[] result = new byte[length];
        in.get(result);
        return result;
    }

    private static long nonNegative(long value, String field) {
        if (value < 0) {
            fail(Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION, Nwg1ValidationStageV1.HEADER_GRAMMAR, field);
        }
        return value;
    }

    private static void requireZero(int value, String field) {
        if (value != 0) {
            fail(Nwg1RejectionV1.REQUIRED_ZERO_NONZERO, Nwg1ValidationStageV1.HEADER_GRAMMAR, field);
        }
    }

    private static void requireClosedByte(ByteBuffer in, String field) {
        if (u8(in) != 1) {
            fail(Nwg1RejectionV1.UNKNOWN_CODE, Nwg1ValidationStageV1.HEADER_GRAMMAR, field);
        }
    }

    private static void fail(Nwg1RejectionV1 rejection, Nwg1ValidationStageV1 stage, String message) {
        throw new Nwg1ValidationException(rejection, stage, Nwg1IsolationScopeV1.SHARED_OBJECT, message);
    }
}
