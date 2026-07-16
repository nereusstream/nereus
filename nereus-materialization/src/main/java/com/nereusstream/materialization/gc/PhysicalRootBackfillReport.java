/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.List;
import java.util.Objects;

/** Complete counters, coverage digests and bounded diagnostics for one run. */
public record PhysicalRootBackfillReport(
        String runId,
        long brokerReadinessEpoch,
        long streamsScanned,
        long dataObjectsScanned,
        long cursorObjectsScanned,
        long rootsCreatedOrVerified,
        long protectionsCreatedOrVerified,
        long failureCount,
        Checksum dataCoverageSha256,
        Checksum cursorCoverageSha256,
        List<PhysicalRootBackfillFailure> boundedFailures) {
    public static final int MAX_FAILURE_DETAILS = 100;

    public PhysicalRootBackfillReport {
        new PhysicalRootBackfillRequest(
                runId,
                brokerReadinessEpoch,
                1,
                java.time.Duration.ofMillis(1));
        requireNonNegative(streamsScanned, "streamsScanned");
        requireNonNegative(dataObjectsScanned, "dataObjectsScanned");
        requireNonNegative(cursorObjectsScanned, "cursorObjectsScanned");
        requireNonNegative(
                rootsCreatedOrVerified, "rootsCreatedOrVerified");
        requireNonNegative(
                protectionsCreatedOrVerified,
                "protectionsCreatedOrVerified");
        requireNonNegative(failureCount, "failureCount");
        dataCoverageSha256 =
                requireSha256(dataCoverageSha256, "dataCoverageSha256");
        cursorCoverageSha256 =
                requireSha256(cursorCoverageSha256, "cursorCoverageSha256");
        boundedFailures =
                List.copyOf(Objects.requireNonNull(
                        boundedFailures, "boundedFailures"));
        if (boundedFailures.stream().anyMatch(Objects::isNull)
                || boundedFailures.size()
                        != Math.min(MAX_FAILURE_DETAILS, failureCount)) {
            throw new IllegalArgumentException(
                    "boundedFailures must contain exactly the first min(100, failureCount) failures");
        }
    }

    private static Checksum requireSha256(
            Checksum value, String field) {
        Objects.requireNonNull(value, field);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must use SHA256");
        }
        return value;
    }

    private static void requireNonNegative(long value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
    }
}
