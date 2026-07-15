/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.materialization.MaterializationConfig;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Bounded process-level configuration for Phase 4 physical-object garbage collection. */
public record PhysicalGcConfig(
        boolean enabled,
        boolean dryRun,
        int metadataScanPageSize,
        int objectListPageSize,
        int maxConcurrentDeletes,
        int maxStreamsPerCandidate,
        int maxAuthoritiesPerDomainSnapshot,
        int maxReferencesPerDomainSnapshot,
        Duration scanInterval,
        Duration readerLeaseDuration,
        Duration readerLeaseRenewInterval,
        Duration maximumClockSkew,
        Duration drainGrace,
        Duration pendingProtectionDuration,
        Duration orphanGrace,
        Duration tombstoneAuditGrace,
        Duration operationTimeout,
        Duration closeTimeout) {
    public static final int MAX_PAGE_SIZE = 1_000;
    public static final int MAX_STREAMS_PER_CANDIDATE = 4_096;
    public static final int MAX_DOMAIN_VALUES = 100_000;

    public PhysicalGcConfig {
        requireInRange(metadataScanPageSize, 1, MAX_PAGE_SIZE, "metadataScanPageSize");
        requireInRange(objectListPageSize, 1, MAX_PAGE_SIZE, "objectListPageSize");
        if (maxConcurrentDeletes <= 0) {
            throw new IllegalArgumentException("maxConcurrentDeletes must be positive");
        }
        requireInRange(
                maxStreamsPerCandidate,
                1,
                MAX_STREAMS_PER_CANDIDATE,
                "maxStreamsPerCandidate");
        requireInRange(
                maxAuthoritiesPerDomainSnapshot,
                1,
                MAX_DOMAIN_VALUES,
                "maxAuthoritiesPerDomainSnapshot");
        requireInRange(
                maxReferencesPerDomainSnapshot,
                1,
                MAX_DOMAIN_VALUES,
                "maxReferencesPerDomainSnapshot");
        scanInterval = requirePositiveMillis(scanInterval, "scanInterval");
        readerLeaseDuration = requirePositiveMillis(readerLeaseDuration, "readerLeaseDuration");
        readerLeaseRenewInterval = requirePositiveMillis(
                readerLeaseRenewInterval, "readerLeaseRenewInterval");
        maximumClockSkew = requireNonNegativeMillis(maximumClockSkew, "maximumClockSkew");
        drainGrace = requirePositiveMillis(drainGrace, "drainGrace");
        pendingProtectionDuration = requirePositiveMillis(
                pendingProtectionDuration, "pendingProtectionDuration");
        orphanGrace = requirePositiveMillis(orphanGrace, "orphanGrace");
        tombstoneAuditGrace = requirePositiveMillis(tombstoneAuditGrace, "tombstoneAuditGrace");
        operationTimeout = requirePositiveMillis(operationTimeout, "operationTimeout");
        closeTimeout = requirePositiveMillis(closeTimeout, "closeTimeout");

        if (readerLeaseRenewInterval.compareTo(readerLeaseDuration.dividedBy(3)) > 0) {
            throw new IllegalArgumentException(
                    "readerLeaseRenewInterval must be at most one third of readerLeaseDuration");
        }
        Duration leaseSafety = checkedPlus(
                readerLeaseDuration, maximumClockSkew, "readerLeaseDuration plus maximumClockSkew");
        if (drainGrace.compareTo(leaseSafety) < 0) {
            throw new IllegalArgumentException(
                    "drainGrace must cover readerLeaseDuration plus maximumClockSkew");
        }
        Duration operationSafety = checkedPlus(
                operationTimeout, maximumClockSkew, "operationTimeout plus maximumClockSkew");
        if (operationSafety.compareTo(readerLeaseDuration) >= 0) {
            throw new IllegalArgumentException(
                    "operationTimeout plus maximumClockSkew must be shorter than readerLeaseDuration");
        }
    }

    /** Safe defaults keep all destructive mutation disabled. */
    public static PhysicalGcConfig defaults() {
        return new PhysicalGcConfig(
                false,
                true,
                256,
                256,
                4,
                MAX_STREAMS_PER_CANDIDATE,
                10_000,
                10_000,
                Duration.ofMinutes(1),
                Duration.ofMinutes(2),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                Duration.ofMinutes(3),
                Duration.ofHours(1),
                Duration.ofHours(25),
                Duration.ofDays(7),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30));
    }

    /** Local config may plan and observe while both disabled and dry-run modes reject mutation. */
    public boolean mutationsAllowed() {
        return enabled && !dryRun;
    }

    public GcReferenceDomainConfig referenceDomainConfig() {
        return new GcReferenceDomainConfig(
                metadataScanPageSize,
                maxAuthoritiesPerDomainSnapshot,
                maxReferencesPerDomainSnapshot);
    }

    /** Cross-validates publication/recovery grace periods that can retain a physical reference. */
    public PhysicalGcConfig validateAgainst(MaterializationConfig materialization) {
        Objects.requireNonNull(materialization, "materialization");
        Duration longestPublicationGrace = maximum(List.of(
                materialization.sourceRetirementGrace(),
                materialization.appendReplayGrace(),
                pendingProtectionDuration));
        if (orphanGrace.compareTo(longestPublicationGrace) <= 0) {
            throw new IllegalArgumentException(
                    "orphanGrace must exceed source-retirement and pending-publication grace periods");
        }
        Duration auditFloor = checkedPlus(
                materialization.metadataAuditGrace(),
                orphanGrace,
                "metadataAuditGrace plus orphanGrace");
        if (tombstoneAuditGrace.compareTo(auditFloor) < 0) {
            throw new IllegalArgumentException(
                    "tombstoneAuditGrace must cover metadataAuditGrace plus orphanGrace");
        }
        requireTombstoneStrictlyExceeds(
                "materialization worker/reference lifetime",
                materialization.workerClaimDuration(),
                materialization.operationTimeout(),
                pendingProtectionDuration,
                readerLeaseDuration);
        return this;
    }

    /** Cross-validates append/session/recovery terminal lifetimes in addition to materialization. */
    public PhysicalGcConfig validateAgainst(
            StreamStorageConfig storage,
            MaterializationConfig materialization) {
        Objects.requireNonNull(storage, "storage");
        validateAgainst(materialization);
        requireTombstoneStrictlyExceeds(
                "append/session/recovery lifetime",
                storage.appendSessionTtl(),
                storage.appendTimeout(),
                storage.appendRecoveryAttemptTimeout(),
                storage.appendRecoveryBackoffMax(),
                storage.appendRecoveryTerminalTtl());
        return this;
    }

    /** Returns an overflow-checked deadline or empty when the candidate must remain ineligible. */
    public java.util.OptionalLong deadline(long baseMillis, Duration grace) {
        if (baseMillis < 0) {
            throw new IllegalArgumentException("baseMillis must be non-negative");
        }
        grace = requireNonNegativeMillis(grace, "grace");
        try {
            return java.util.OptionalLong.of(Math.addExact(baseMillis, grace.toMillis()));
        } catch (ArithmeticException failure) {
            return java.util.OptionalLong.empty();
        }
    }

    private void requireTombstoneStrictlyExceeds(String label, Duration... lifetimes) {
        for (Duration lifetime : lifetimes) {
            Duration withSkew = checkedPlus(lifetime, maximumClockSkew, label + " plus maximumClockSkew");
            if (tombstoneAuditGrace.compareTo(withSkew) <= 0) {
                throw new IllegalArgumentException("tombstoneAuditGrace must strictly exceed " + label);
            }
        }
    }

    private static Duration maximum(List<Duration> values) {
        return values.stream().max(Duration::compareTo).orElseThrow();
    }

    private static Duration checkedPlus(Duration left, Duration right, String label) {
        try {
            return left.plus(right);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(label + " overflows", failure);
        }
    }

    private static Duration requirePositiveMillis(Duration value, String name) {
        Objects.requireNonNull(value, name);
        try {
            long millis = value.toMillis();
            if (value.isNegative() || value.isZero() || millis <= 0) {
                throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
            }
            requireExactMillis(value, millis, name);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(name + " must fit the millisecond representation", failure);
        }
        return value;
    }

    private static Duration requireNonNegativeMillis(Duration value, String name) {
        Objects.requireNonNull(value, name);
        try {
            if (value.isNegative()) {
                throw new IllegalArgumentException(name + " must be non-negative");
            }
            long millis = value.toMillis();
            requireExactMillis(value, millis, name);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(name + " must fit the millisecond representation", failure);
        }
        return value;
    }

    private static void requireExactMillis(Duration value, long millis, String name) {
        if (!value.equals(Duration.ofMillis(millis))) {
            throw new IllegalArgumentException(name + " must be exactly millisecond-representable");
        }
    }

    private static void requireInRange(int value, int minimum, int maximum, String name) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(name + " must be in [" + minimum + ", " + maximum + "]");
        }
    }
}
