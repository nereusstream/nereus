/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Exact closed commitment preimages from ADR 0088. */
public final class Nwg1CommitmentsV1 {
    private Nwg1CommitmentsV1() {}

    public static byte[] protocolCell(byte[] npc1) {
        return digest(
                Nwg1ConstantsV1.CELL_COMMITMENT_DOMAIN,
                ByteBuffer.allocate(4)
                        .order(ByteOrder.BIG_ENDIAN)
                        .putInt(npc1.length)
                        .array(),
                npc1);
    }

    public static byte[] ownerFence(int kind, int version, byte[] witness) {
        ByteBuffer prefix = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        prefix.putShort((short) kind).putShort((short) version).putInt(witness.length);
        return digest(Nwg1ConstantsV1.OWNER_COMMITMENT_DOMAIN, prefix.array(), witness);
    }

    public static byte[] wrappedEnvelope(Nwg1EnvelopeV1 envelope) {
        byte[] canonical = envelope.canonicalBytes();
        ByteBuffer prefix = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        prefix.putShort((short) Nwg1EnvelopeV1.KIND)
                .putShort((short) Nwg1EnvelopeV1.VERSION)
                .putInt(canonical.length);
        return digest(Nwg1ConstantsV1.ENVELOPE_COMMITMENT_DOMAIN, prefix.array(), canonical);
    }

    public static byte[] sha256(byte[] value) {
        return digest(value);
    }

    private static byte[] digest(byte[]... parts) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (byte[] part : parts) {
                digest.update(part);
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK has no SHA-256", e);
        }
    }
}
