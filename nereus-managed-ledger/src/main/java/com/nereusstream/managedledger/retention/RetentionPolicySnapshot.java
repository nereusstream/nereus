/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

/** Exact versioned Pulsar retention values used by one planning attempt. */
public record RetentionPolicySnapshot(
        long policyVersion,
        long retentionTimeMillis,
        long retentionSizeBytes) {
    private static final long MEBIBYTE = 1L << 20;

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
}
