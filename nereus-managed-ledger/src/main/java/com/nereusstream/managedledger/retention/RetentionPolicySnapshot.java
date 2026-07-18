/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Exact versioned Pulsar retention values used by one planning attempt. */
public record RetentionPolicySnapshot(
        long policyVersion,
        long retentionTimeMillis,
        long retentionSizeBytes) {
    private static final long MEBIBYTE = 1L << 20;
    private static final byte[] POLICY_VERSION_DOMAIN =
            "nereus-retention-policy-v1".getBytes(StandardCharsets.US_ASCII);

    public RetentionPolicySnapshot {
        if (policyVersion < 0) {
            throw new IllegalArgumentException("policyVersion must be non-negative");
        }
        requireRetentionValue(retentionTimeMillis, "retentionTimeMillis");
        requireRetentionValue(retentionSizeBytes, "retentionSizeBytes");
        if ((retentionTimeMillis == 0) != (retentionSizeBytes == 0)) {
            throw new IllegalArgumentException(
                    "retention time and size must either both be zero or use -1 to ignore one dimension");
        }
    }

    public static RetentionPolicySnapshot fromMinutesAndMebibytes(
            long policyVersion,
            long retentionTimeMinutes,
            long retentionSizeMebibytes) {
        requireRetentionValue(retentionTimeMinutes, "retentionTimeMinutes");
        requireRetentionValue(retentionSizeMebibytes, "retentionSizeMebibytes");
        final long timeMillis;
        final long sizeBytes;
        try {
            timeMillis = retentionTimeMinutes == -1
                    ? -1
                    : Math.multiplyExact(retentionTimeMinutes, 60_000L);
            sizeBytes = retentionSizeMebibytes == -1
                    ? -1
                    : Math.multiplyExact(retentionSizeMebibytes, MEBIBYTE);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(
                    "retention policy conversion overflows milliseconds or bytes",
                    failure);
        }
        return new RetentionPolicySnapshot(policyVersion, timeMillis, sizeBytes);
    }

    /**
     * Converts one exact effective Pulsar policy and derives a stable version from both raw policy values.
     *
     * <p>The complete converted values remain part of every authority comparison; the 63-bit digest is a stable
     * version label rather than the sole equality proof.
     */
    public static RetentionPolicySnapshot fromCanonicalMinutesAndMebibytes(
            long retentionTimeMinutes,
            long retentionSizeMebibytes) {
        return fromMinutesAndMebibytes(
                canonicalPolicyVersion(
                        retentionTimeMinutes,
                        retentionSizeMebibytes),
                retentionTimeMinutes,
                retentionSizeMebibytes);
    }

    public boolean disablesPostConsumeRetention() {
        return retentionTimeMillis == 0 && retentionSizeBytes == 0;
    }

    public boolean retainsIndefinitely() {
        return retentionTimeMillis == -1 && retentionSizeBytes == -1;
    }

    private static void requireRetentionValue(long value, String field) {
        if (value < -1) {
            throw new IllegalArgumentException(field + " must be -1 or non-negative");
        }
    }

    private static long canonicalPolicyVersion(
            long retentionTimeMinutes,
            long retentionSizeMebibytes) {
        requireRetentionValue(retentionTimeMinutes, "retentionTimeMinutes");
        requireRetentionValue(retentionSizeMebibytes, "retentionSizeMebibytes");
        ByteBuffer canonical = ByteBuffer.allocate(
                        POLICY_VERSION_DOMAIN.length + Long.BYTES * 2)
                .order(ByteOrder.BIG_ENDIAN);
        canonical.put(POLICY_VERSION_DOMAIN);
        canonical.putLong(retentionTimeMinutes);
        canonical.putLong(retentionSizeMebibytes);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.array());
            return ByteBuffer.wrap(digest)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getLong() & Long.MAX_VALUE;
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
