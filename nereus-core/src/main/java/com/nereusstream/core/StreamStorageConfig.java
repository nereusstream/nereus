/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.core;

import com.nereusstream.api.keys.DeterministicIds;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Objects;

/** Resource, timeout, and append-session limits for one stream-storage instance. */
public record StreamStorageConfig(
        String cluster,
        String writerId,
        Duration appendSessionTtl,
        Duration appendSessionRenewBefore,
        Duration appendSessionMinCommitRemaining,
        Duration appendTimeout,
        Duration readTimeout,
        Duration shutdownGrace,
        int maxResolveRanges,
        int maxCommitChainScan,
        int maxDerivedIndexRepairCommitsPerCall,
        int maxCachedStreams,
        int maxInFlightAppends,
        long maxBufferedBytes,
        int maxConcurrentObjectReads,
        long maxReadBufferBytes,
        int maxObjectBytes,
        int maxAppendBatchRecords,
        Duration offsetIndexCacheTtl,
        boolean autoAcquireAppendSession,
        boolean enableMetadataWatch,
        boolean enableOffsetIndexCache,
        String processRunId,
        Duration appendRecoveryAttemptTimeout,
        Duration appendRecoveryBackoffMin,
        Duration appendRecoveryBackoffMax,
        Duration appendRecoveryTerminalTtl,
        int maxRetainedAppendAttempts,
        int maxAppendRecoveryTerminals) {
    public StreamStorageConfig {
        cluster = requireNonBlank(cluster, "cluster");
        writerId = requireNonBlank(writerId, "writerId");
        appendSessionTtl = requirePositive(appendSessionTtl, "appendSessionTtl");
        appendSessionRenewBefore = requirePositive(appendSessionRenewBefore, "appendSessionRenewBefore");
        appendSessionMinCommitRemaining = requirePositive(
                appendSessionMinCommitRemaining, "appendSessionMinCommitRemaining");
        requirePositiveMillis(appendSessionTtl, "appendSessionTtl");
        requirePositiveMillis(appendSessionRenewBefore, "appendSessionRenewBefore");
        requirePositiveMillis(appendSessionMinCommitRemaining, "appendSessionMinCommitRemaining");
        appendTimeout = requirePositive(appendTimeout, "appendTimeout");
        readTimeout = requirePositive(readTimeout, "readTimeout");
        shutdownGrace = requirePositive(shutdownGrace, "shutdownGrace");
        offsetIndexCacheTtl = requirePositive(offsetIndexCacheTtl, "offsetIndexCacheTtl");
        processRunId = requireNonBlank(processRunId, "processRunId");
        appendRecoveryAttemptTimeout = requirePositive(appendRecoveryAttemptTimeout, "appendRecoveryAttemptTimeout");
        appendRecoveryBackoffMin = requirePositive(appendRecoveryBackoffMin, "appendRecoveryBackoffMin");
        appendRecoveryBackoffMax = requirePositive(appendRecoveryBackoffMax, "appendRecoveryBackoffMax");
        appendRecoveryTerminalTtl = requirePositive(appendRecoveryTerminalTtl, "appendRecoveryTerminalTtl");
        if (appendRecoveryBackoffMin.compareTo(appendRecoveryBackoffMax) > 0) {
            throw new IllegalArgumentException("appendRecoveryBackoffMin must be <= appendRecoveryBackoffMax");
        }
        if (appendSessionRenewBefore.compareTo(appendSessionTtl) >= 0) {
            throw new IllegalArgumentException("appendSessionRenewBefore must be less than appendSessionTtl");
        }
        if (appendSessionMinCommitRemaining.compareTo(appendSessionTtl) >= 0) {
            throw new IllegalArgumentException("appendSessionMinCommitRemaining must be less than appendSessionTtl");
        }
        if (maxResolveRanges <= 0 || maxCommitChainScan <= 0 || maxDerivedIndexRepairCommitsPerCall <= 0
                || maxCachedStreams <= 0
                || maxInFlightAppends <= 0 || maxBufferedBytes <= 0 || maxConcurrentObjectReads <= 0
                || maxReadBufferBytes <= 0 || maxObjectBytes <= 0 || maxAppendBatchRecords <= 0) {
            throw new IllegalArgumentException("numeric resource limits must be positive");
        }
        if (maxObjectBytes > maxBufferedBytes) {
            throw new IllegalArgumentException("maxObjectBytes must be <= maxBufferedBytes");
        }
        if (maxObjectBytes > maxReadBufferBytes) {
            throw new IllegalArgumentException("maxObjectBytes must be <= maxReadBufferBytes");
        }
        if (maxRetainedAppendAttempts <= 0 || maxAppendRecoveryTerminals < maxRetainedAppendAttempts) {
            throw new IllegalArgumentException("append recovery capacities are invalid");
        }
    }

    /** Phase 1 source-compatible constructor with generated process identity and bounded recovery defaults. */
    public StreamStorageConfig(
            String cluster, String writerId, Duration appendSessionTtl, Duration appendSessionRenewBefore,
            Duration appendSessionMinCommitRemaining, Duration appendTimeout, Duration readTimeout,
            Duration shutdownGrace, int maxResolveRanges, int maxCommitChainScan,
            int maxDerivedIndexRepairCommitsPerCall, int maxCachedStreams, int maxInFlightAppends,
            long maxBufferedBytes, int maxConcurrentObjectReads, long maxReadBufferBytes, int maxObjectBytes,
            int maxAppendBatchRecords, Duration offsetIndexCacheTtl, boolean autoAcquireAppendSession,
            boolean enableMetadataWatch, boolean enableOffsetIndexCache) {
        this(cluster, writerId, appendSessionTtl, appendSessionRenewBefore, appendSessionMinCommitRemaining,
                appendTimeout, readTimeout, shutdownGrace, maxResolveRanges, maxCommitChainScan,
                maxDerivedIndexRepairCommitsPerCall, maxCachedStreams, maxInFlightAppends, maxBufferedBytes,
                maxConcurrentObjectReads, maxReadBufferBytes, maxObjectBytes, maxAppendBatchRecords,
                offsetIndexCacheTtl, autoAcquireAppendSession, enableMetadataWatch, enableOffsetIndexCache,
                randomProcessRunId(),
                Duration.ofSeconds(5), Duration.ofMillis(100), Duration.ofSeconds(5), Duration.ofMinutes(10),
                Math.max(1, maxInFlightAppends),
                maxInFlightAppends > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE : Math.max(2, maxInFlightAppends * 2));
    }

    public static StreamStorageConfig defaults(String cluster, String writerId) {
        return new StreamStorageConfig(
                cluster,
                writerId,
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                64,
                10_000,
                256,
                10_000,
                1_024,
                64L * 1024 * 1024,
                64,
                128L * 1024 * 1024,
                16 * 1024 * 1024,
                100_000,
                Duration.ofSeconds(5),
                true,
                false,
                true,
                randomProcessRunId(),
                Duration.ofSeconds(5),
                Duration.ofMillis(100),
                Duration.ofSeconds(5),
                Duration.ofMinutes(10),
                1_024,
                2_048);
    }

    public int maxConcurrentPrimaryReads() { return maxConcurrentObjectReads; }
    public long maxPrimaryReadBufferBytes() { return maxReadBufferBytes; }
    public long maxPrimaryAppendBytes() { return maxBufferedBytes; }

    private static String randomProcessRunId() {
        byte[] entropy = new byte[32];
        new SecureRandom().nextBytes(entropy);
        return DeterministicIds.randomRunIdHash(entropy);
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static void requirePositiveMillis(Duration value, String name) {
        try {
            if (value.toMillis() <= 0) {
                throw new IllegalArgumentException(name + " must be at least one millisecond");
            }
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(name + " must fit the millisecond lease representation", e);
        }
    }
}
