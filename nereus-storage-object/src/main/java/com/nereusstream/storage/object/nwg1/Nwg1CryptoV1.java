/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/** RFC-5869 HKDF-SHA-256 and AES-256-GCM primitives for NWG1 v1. */
public final class Nwg1CryptoV1 {
    private Nwg1CryptoV1() {}

    public static byte[] objectKeyInfo(long shardId, long shardRunEpoch, int laneId, long laneSequence) {
        if (shardId < 0
                || shardId > 0xffff_ffffL
                || shardRunEpoch < 0
                || laneId < 0
                || laneId > 2
                || laneSequence < 0) {
            throw new IllegalArgumentException("invalid Object key tuple");
        }
        ByteBuffer info =
                ByteBuffer.allocate(Nwg1ConstantsV1.OBJECT_KEY_INFO_BYTES).order(ByteOrder.BIG_ENDIAN);
        info.put(Nwg1ConstantsV1.OBJECT_KEY_DOMAIN)
                .putInt((int) shardId)
                .putLong(shardRunEpoch)
                .put((byte) laneId)
                .putLong(laneSequence);
        return info.array();
    }

    public static byte[] deriveObjectAeadKey(byte[] walRunKey, byte[] walRunRootSha256, byte[] info) {
        requireLength(walRunKey, 32, "WalRun key");
        requireLength(walRunRootSha256, 32, "Root SHA");
        requireLength(info, Nwg1ConstantsV1.OBJECT_KEY_INFO_BYTES, "ObjectKeyInfoV1");
        byte[] prk = hmac(walRunRootSha256, walRunKey);
        byte[] expandInput = Arrays.copyOf(info, info.length + 1);
        expandInput[info.length] = 1;
        byte[] result = hmac(prk, expandInput);
        Arrays.fill(prk, (byte) 0);
        Arrays.fill(expandInput, (byte) 0);
        return result;
    }

    public static byte[] directoryNonce() {
        return ByteBuffer.allocate(12)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(0x4e444952)
                .putLong(0)
                .array();
    }

    public static byte[] frameNonce(long ordinal) {
        if (ordinal < 0) {
            throw new IllegalArgumentException("negative frame ordinal");
        }
        return ByteBuffer.allocate(12)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(0x4e46524d)
                .putLong(ordinal)
                .array();
    }

    public static byte[] directoryAad(byte[] header) {
        requireLength(header, 256, "Header");
        return ByteBuffer.allocate(272)
                .put(Nwg1ConstantsV1.DIRECTORY_AAD_DOMAIN)
                .put(header)
                .array();
    }

    public static byte[] frameAad(byte[] header, long ordinal, byte[] exactFrameRow) {
        requireLength(header, 256, "Header");
        requireLength(exactFrameRow, 48, "Frame row");
        return ByteBuffer.allocate(328)
                .order(ByteOrder.BIG_ENDIAN)
                .put(Nwg1ConstantsV1.FRAME_AAD_DOMAIN)
                .put(header)
                .putLong(ordinal)
                .put(exactFrameRow)
                .array();
    }

    public static byte[] encrypt(byte[] key, byte[] nonce, byte[] aad, byte[] plaintext) {
        return crypt(Cipher.ENCRYPT_MODE, key, nonce, aad, plaintext, Nwg1ValidationStageV1.FRAME_AEAD);
    }

    public static byte[] decrypt(byte[] key, byte[] nonce, byte[] aad, byte[] ciphertext, Nwg1ValidationStageV1 stage) {
        return crypt(Cipher.DECRYPT_MODE, key, nonce, aad, ciphertext, stage);
    }

    private static byte[] crypt(
            int mode, byte[] key, byte[] nonce, byte[] aad, byte[] input, Nwg1ValidationStageV1 stage) {
        requireLength(key, 32, "AES key");
        requireLength(nonce, 12, "nonce");
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(mode, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, nonce));
            cipher.updateAAD(aad);
            return cipher.doFinal(input);
        } catch (AEADBadTagException e) {
            Nwg1IsolationScopeV1 scope = stage == Nwg1ValidationStageV1.DIRECTORY_AEAD
                    ? Nwg1IsolationScopeV1.SHARED_OBJECT
                    : Nwg1IsolationScopeV1.APPEND_UNIT;
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.AEAD_AUTHENTICATION_FAILED, stage, scope, "AES-GCM authentication failed", e);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("JDK AES-256-GCM unavailable", e);
        }
    }

    private static byte[] hmac(byte[] key, byte[] value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(value);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("JDK HmacSHA256 unavailable", e);
        }
    }

    private static void requireLength(byte[] value, int length, String name) {
        if (value == null || value.length != length) {
            throw new IllegalArgumentException(name + " must be " + length + " bytes");
        }
    }
}
