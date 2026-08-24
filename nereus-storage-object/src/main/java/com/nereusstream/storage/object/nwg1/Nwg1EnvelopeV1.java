/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;

/** Closed lengths-first KMS envelope preimage. */
public final class Nwg1EnvelopeV1 {
    private static final Pattern TOKEN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._/-]{0,63}");
    public static final int KIND = 1;
    public static final int VERSION = 1;

    private final byte[] providerId;
    private final byte[] wrappingAlgorithmId;
    private final byte[] wrappingKeyId;
    private final byte[] wrappingKeyVersion;
    private final byte[] wrappedKey;

    public Nwg1EnvelopeV1(
            byte[] providerId,
            byte[] wrappingAlgorithmId,
            byte[] wrappingKeyId,
            byte[] wrappingKeyVersion,
            byte[] wrappedKey) {
        this.providerId = bounded(providerId, 64, "providerId");
        this.wrappingAlgorithmId = bounded(wrappingAlgorithmId, 64, "wrappingAlgorithmId");
        this.wrappingKeyId = bounded(wrappingKeyId, 4_096, "wrappingKeyId");
        this.wrappingKeyVersion = bounded(wrappingKeyVersion, 1_024, "wrappingKeyVersion");
        this.wrappedKey = bounded(wrappedKey, 16_384, "wrappedKey");
        requireToken(this.providerId, "providerId");
        requireToken(this.wrappingAlgorithmId, "wrappingAlgorithmId");
        String version = strictAscii(this.wrappingKeyVersion, "wrappingKeyVersion");
        if (version.equalsIgnoreCase("current")) {
            throw new IllegalArgumentException("mutable key-version alias");
        }
    }

    public byte[] canonicalBytes() {
        int total = Math.addExact(
                20,
                providerId.length
                        + wrappingAlgorithmId.length
                        + wrappingKeyId.length
                        + wrappingKeyVersion.length
                        + wrappedKey.length);
        ByteBuffer out = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
        out.putInt(providerId.length)
                .putInt(wrappingAlgorithmId.length)
                .putInt(wrappingKeyId.length)
                .putInt(wrappingKeyVersion.length)
                .putInt(wrappedKey.length);
        out.put(providerId)
                .put(wrappingAlgorithmId)
                .put(wrappingKeyId)
                .put(wrappingKeyVersion)
                .put(wrappedKey);
        return out.array();
    }

    public byte[] framedBytes() {
        byte[] canonical = canonicalBytes();
        ByteBuffer out = ByteBuffer.allocate(8 + canonical.length).order(ByteOrder.BIG_ENDIAN);
        out.putShort((short) KIND)
                .putShort((short) VERSION)
                .putInt(canonical.length)
                .put(canonical);
        return out.array();
    }

    public static Nwg1EnvelopeV1 decode(byte[] framed) {
        if (framed == null || framed.length < 28) {
            fail(Nwg1RejectionV1.TRUNCATED_INPUT, "short envelope");
        }
        ByteBuffer in = ByteBuffer.wrap(framed).order(ByteOrder.BIG_ENDIAN);
        if (Short.toUnsignedInt(in.getShort()) != KIND || Short.toUnsignedInt(in.getShort()) != VERSION) {
            fail(Nwg1RejectionV1.UNKNOWN_CODE, "envelope kind/version");
        }
        long declared = Integer.toUnsignedLong(in.getInt());
        if (declared != in.remaining()) {
            fail(Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH, "envelope length");
        }
        int[] lengths = new int[5];
        int[] caps = {64, 64, 4_096, 1_024, 16_384};
        long sum = 20;
        for (int i = 0; i < lengths.length; i++) {
            long length = Integer.toUnsignedLong(in.getInt());
            if (length == 0 || length > caps[i]) {
                fail(Nwg1RejectionV1.LIMIT_EXCEEDED, "envelope field length");
            }
            lengths[i] = (int) length;
            sum = Math.addExact(sum, length);
        }
        if (sum > declared) {
            fail(Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH, "envelope fields");
        }
        Nwg1EnvelopeV1 result;
        try {
            result = new Nwg1EnvelopeV1(
                    read(in, lengths[0]),
                    read(in, lengths[1]),
                    read(in, lengths[2]),
                    read(in, lengths[3]),
                    read(in, lengths[4]));
        } catch (IllegalArgumentException e) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                    Nwg1ValidationStageV1.KMS_ENVELOPE,
                    Nwg1IsolationScopeV1.WALRUN,
                    "invalid envelope field domain",
                    e);
        }
        if (in.hasRemaining()) {
            fail(Nwg1RejectionV1.TRAILING_BYTES, "envelope trailing bytes");
        }
        if (!Arrays.equals(framed, result.framedBytes())) {
            fail(Nwg1RejectionV1.NON_CANONICAL_ENCODING, "envelope re-encode");
        }
        return result;
    }

    public byte[] providerId() {
        return providerId.clone();
    }

    public byte[] wrappingAlgorithmId() {
        return wrappingAlgorithmId.clone();
    }

    public byte[] wrappingKeyId() {
        return wrappingKeyId.clone();
    }

    public byte[] wrappingKeyVersion() {
        return wrappingKeyVersion.clone();
    }

    public byte[] wrappedKey() {
        return wrappedKey.clone();
    }

    private static byte[] bounded(byte[] value, int cap, String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0 || value.length > cap) {
            throw new IllegalArgumentException(name + " length");
        }
        return value.clone();
    }

    private static void requireToken(byte[] value, String name) {
        String token = strictAscii(value, name);
        if (!TOKEN.matcher(token).matches()) {
            throw new IllegalArgumentException(name + " is not a closed ASCII token");
        }
    }

    private static String strictAscii(byte[] value, String name) {
        for (byte item : value) {
            if (item < 0x20 || item > 0x7e) {
                throw new IllegalArgumentException(name + " must be printable ASCII");
            }
        }
        return new String(value, StandardCharsets.US_ASCII);
    }

    private static byte[] read(ByteBuffer in, int length) {
        if (in.remaining() < length) {
            fail(Nwg1RejectionV1.TRUNCATED_INPUT, "envelope field");
        }
        byte[] result = new byte[length];
        in.get(result);
        return result;
    }

    private static void fail(Nwg1RejectionV1 rejection, String message) {
        throw new Nwg1ValidationException(
                rejection, Nwg1ValidationStageV1.KMS_ENVELOPE, Nwg1IsolationScopeV1.WALRUN, message);
    }
}
