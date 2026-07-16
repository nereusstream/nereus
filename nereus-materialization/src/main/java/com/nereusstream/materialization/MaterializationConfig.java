/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ReadView;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** Complete process-level limits for lossless committed materialization. */
public record MaterializationConfig(
        MaterializationPolicy committedPolicy,
        int registryScanPageSize,
        Duration registryScanInterval,
        int plannerPageSize,
        int taskScanPageSize,
        int maxTasksPerPlan,
        int maxConcurrentWorkers,
        int maxConcurrentWorkersPerStream,
        int sourceReadPageRecords,
        long sourceReadPageBytes,
        Path stagingDirectory,
        long maxStagingBytes,
        int uploadChunkBytes,
        Duration workerClaimDuration,
        Duration workerClaimRenewInterval,
        Duration maximumClockSkew,
        Duration operationTimeout,
        Duration closeTimeout,
        Duration retryMinBackoff,
        Duration retryMaxBackoff,
        int maxTaskAttempts,
        long lagThrottleRecords,
        long lagRejectRecords,
        long lagThrottleBytes,
        long lagRejectBytes,
        Duration lagRejectAge,
        Duration lagThrottleDelay,
        Duration sourceRetirementGrace,
        Duration appendReplayGrace,
        Duration metadataAuditGrace,
        int recoveryCheckpointMaxEntries,
        long recoveryCheckpointMaxBytes) {
    public static final int MAX_PAGE_SIZE = 1_000;
    public static final int MAX_TASKS_PER_PLAN = 1_000;
    public static final int MAX_SOURCE_READ_PAGE_RECORDS = 65_536;
    public static final long MIN_SOURCE_READ_PAGE_BYTES = 64L << 10;
    public static final long MAX_SOURCE_READ_PAGE_BYTES = 64L << 20;
    public static final int MAX_RECOVERY_CHECKPOINT_ENTRIES = 1_000_000;
    public static final long MAX_RECOVERY_CHECKPOINT_BYTES = 1L << 30;

    private static final Set<PosixFilePermission> OWNER_ONLY_DIRECTORY = EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE);

    public MaterializationConfig {
        committedPolicy = requireCommittedPolicy(committedPolicy);
        requireInRange(registryScanPageSize, 1, MAX_PAGE_SIZE, "registryScanPageSize");
        registryScanInterval = requirePositiveMillis(registryScanInterval, "registryScanInterval");
        requireInRange(plannerPageSize, 1, MAX_PAGE_SIZE, "plannerPageSize");
        requireInRange(taskScanPageSize, 1, MAX_PAGE_SIZE, "taskScanPageSize");
        requireInRange(maxTasksPerPlan, 1, MAX_TASKS_PER_PLAN, "maxTasksPerPlan");
        if (maxConcurrentWorkers <= 0
                || maxConcurrentWorkersPerStream <= 0
                || maxConcurrentWorkersPerStream > maxConcurrentWorkers) {
            throw new IllegalArgumentException(
                    "worker concurrency must be positive and per-stream concurrency must not exceed global concurrency");
        }
        requireInRange(
                sourceReadPageRecords,
                1,
                MAX_SOURCE_READ_PAGE_RECORDS,
                "sourceReadPageRecords");
        if (sourceReadPageBytes < MIN_SOURCE_READ_PAGE_BYTES
                || sourceReadPageBytes > MAX_SOURCE_READ_PAGE_BYTES) {
            throw new IllegalArgumentException("sourceReadPageBytes must be in [64 KiB, 64 MiB]");
        }
        stagingDirectory = requirePrivateStagingPath(stagingDirectory);
        if (maxStagingBytes < Math.max(
                committedPolicy.targetObjectBytes(), recoveryCheckpointMaxBytes)) {
            throw new IllegalArgumentException(
                    "maxStagingBytes must cover the largest compacted or recovery-checkpoint object");
        }
        if (uploadChunkBytes < StagingFileManager.MIN_UPLOAD_CHUNK_BYTES
                || uploadChunkBytes > StagingFileManager.MAX_UPLOAD_CHUNK_BYTES) {
            throw new IllegalArgumentException("uploadChunkBytes must be in [64 KiB, 8 MiB]");
        }
        workerClaimDuration = requirePositiveMillis(workerClaimDuration, "workerClaimDuration");
        workerClaimRenewInterval = requirePositiveMillis(
                workerClaimRenewInterval, "workerClaimRenewInterval");
        maximumClockSkew = requireNonNegativeMillis(maximumClockSkew, "maximumClockSkew");
        operationTimeout = requirePositiveMillis(operationTimeout, "operationTimeout");
        closeTimeout = requirePositiveMillis(closeTimeout, "closeTimeout");
        if (workerClaimRenewInterval.compareTo(workerClaimDuration.dividedBy(3)) > 0) {
            throw new IllegalArgumentException(
                    "workerClaimRenewInterval must be at most one third of workerClaimDuration");
        }
        Duration claimSafetyWindow;
        try {
            claimSafetyWindow = operationTimeout.plus(maximumClockSkew);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("operationTimeout plus maximumClockSkew overflows", failure);
        }
        if (claimSafetyWindow.compareTo(workerClaimDuration) >= 0) {
            throw new IllegalArgumentException(
                    "operationTimeout plus maximumClockSkew must be shorter than workerClaimDuration");
        }
        retryMinBackoff = requirePositiveMillis(retryMinBackoff, "retryMinBackoff");
        retryMaxBackoff = requirePositiveMillis(retryMaxBackoff, "retryMaxBackoff");
        if (retryMinBackoff.compareTo(retryMaxBackoff) > 0) {
            throw new IllegalArgumentException("retryMinBackoff must not exceed retryMaxBackoff");
        }
        if (maxTaskAttempts <= 0) {
            throw new IllegalArgumentException("maxTaskAttempts must be positive");
        }
        requireThresholds(lagThrottleRecords, lagRejectRecords, "record lag");
        requireThresholds(lagThrottleBytes, lagRejectBytes, "byte lag");
        lagRejectAge = requireNonNegativeMillis(
                lagRejectAge, "lagRejectAge");
        lagThrottleDelay = requirePositiveMillis(
                lagThrottleDelay, "lagThrottleDelay");
        sourceRetirementGrace = requirePositiveMillis(
                sourceRetirementGrace, "sourceRetirementGrace");
        appendReplayGrace = requirePositiveMillis(appendReplayGrace, "appendReplayGrace");
        metadataAuditGrace = requirePositiveMillis(metadataAuditGrace, "metadataAuditGrace");
        if (metadataAuditGrace.compareTo(sourceRetirementGrace) < 0) {
            throw new IllegalArgumentException(
                    "metadataAuditGrace must not be shorter than sourceRetirementGrace");
        }
        requireInRange(
                recoveryCheckpointMaxEntries,
                1,
                MAX_RECOVERY_CHECKPOINT_ENTRIES,
                "recoveryCheckpointMaxEntries");
        if (recoveryCheckpointMaxBytes <= 0
                || recoveryCheckpointMaxBytes > MAX_RECOVERY_CHECKPOINT_BYTES) {
            throw new IllegalArgumentException("recoveryCheckpointMaxBytes must be in (0, 1 GiB]");
        }
    }

    public static MaterializationConfig defaults(Path stagingDirectory) {
        MaterializationPolicy policy = MaterializationPolicyFactory.losslessCommitted(
                4,
                64,
                MaterializationPolicy.MAX_RANGE_RECORDS,
                256L << 20,
                MaterializationPolicy.MAX_ROW_GROUP_RECORDS,
                "ZSTD");
        return new MaterializationConfig(
                policy,
                256,
                Duration.ofSeconds(10),
                256,
                256,
                64,
                4,
                1,
                4_096,
                16L << 20,
                stagingDirectory,
                1L << 30,
                8 << 20,
                Duration.ofMinutes(2),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(1),
                Duration.ofMinutes(1),
                16,
                1_000_000,
                10_000_000,
                1L << 30,
                8L << 30,
                Duration.ofMinutes(10),
                Duration.ofMillis(25),
                Duration.ofHours(1),
                Duration.ofHours(6),
                Duration.ofHours(24),
                1_000_000,
                512L << 20);
    }

    private static MaterializationPolicy requireCommittedPolicy(MaterializationPolicy policy) {
        Objects.requireNonNull(policy, "committedPolicy");
        if (policy.view() != ReadView.COMMITTED
                || policy.taskKind() != TaskKind.LOSSLESS_REWRITE
                || !policy.targetPhysicalFormat().equals(MaterializationPolicy.COMMITTED_FORMAT)
                || policy.topicCompaction().isPresent()) {
            throw new IllegalArgumentException(
                    "committedPolicy must be the lossless COMMITTED NCP1 policy");
        }
        return policy;
    }

    private static Path requirePrivateStagingPath(Path value) {
        Objects.requireNonNull(value, "stagingDirectory");
        if (!value.isAbsolute()) {
            throw new IllegalArgumentException("stagingDirectory must be absolute");
        }
        Path normalized = value.normalize();
        try {
            if (Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
                if (Files.isSymbolicLink(normalized)
                        || !Files.isDirectory(normalized, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalArgumentException(
                            "stagingDirectory must be a non-symlink directory");
                }
                if (!Files.getPosixFilePermissions(normalized, LinkOption.NOFOLLOW_LINKS)
                        .equals(OWNER_ONLY_DIRECTORY)) {
                    throw new IllegalArgumentException(
                            "stagingDirectory permissions must be owner-only 0700");
                }
            }
        } catch (UnsupportedOperationException failure) {
            throw new IllegalArgumentException(
                    "stagingDirectory requires POSIX owner-only permissions", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("cannot validate stagingDirectory", failure);
        }
        return normalized;
    }

    private static void requireThresholds(long throttle, long reject, String field) {
        if (throttle < 0
                || reject < 0
                || throttle > 0
                        && reject > 0
                        && reject <= throttle) {
            throw new IllegalArgumentException(
                    field
                            + " thresholds must be non-negative and throttle must be below reject when both are enabled");
        }
    }

    private static void requireInRange(int value, int minimum, int maximum, String field) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    field + " must be in [" + minimum + ", " + maximum + "]");
        }
    }

    private static Duration requirePositiveMillis(Duration value, String field) {
        Duration exact = requireNonNegativeMillis(value, field);
        if (exact.isZero()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return exact;
    }

    private static Duration requireNonNegativeMillis(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isNegative()) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
        try {
            value.toMillis();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(field + " is not millisecond-representable", failure);
        }
        return value;
    }
}
