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

package com.nereusstream.domain.registry.allocator;

/** Aggregate row recomputed by the raw ADR-0094 validator; callers cannot make it selection authority. */
public record AllocatorNativeRelativeMetricsV1(
        AllocatorEvidenceWorkloadV1 workload,
        double sustainableRolloverRequestsPerSecond,
        double nativeRolloverRequestsPerSecond,
        long rolloverEndToEndP99Micros,
        long oxiaOperationP99Micros,
        long queueDepthMaximum,
        long queueAgeP99Micros,
        long topicStarvationMaximumMicros,
        long cellAppendStallP99Micros,
        long nativeCellAppendStallP99Micros,
        long takeoverRecoveryP99Micros,
        long successfulOperations,
        long fencedOperations,
        long errorOperations,
        long timedOutOperations,
        long permanentOrphans,
        long duplicateLedgerIds,
        long reusedLedgerIds,
        long failedAssertions,
        long skippedAssertions,
        long unexpectedErrors,
        long metadataOperationCalls,
        long metadataRequestBytes,
        long metadataResponseBytes,
        long grantUseOperations,
        long grantWasteIds,
        long staleCandidateBurns) {
    public AllocatorNativeRelativeMetricsV1 {
        if (workload == null
                || !Double.isFinite(sustainableRolloverRequestsPerSecond)
                || sustainableRolloverRequestsPerSecond < 0
                || !Double.isFinite(nativeRolloverRequestsPerSecond)
                || nativeRolloverRequestsPerSecond <= 0
                || rolloverEndToEndP99Micros < 0
                || oxiaOperationP99Micros < 0
                || queueDepthMaximum < 0
                || queueAgeP99Micros < 0
                || topicStarvationMaximumMicros < 0
                || cellAppendStallP99Micros < 0
                || nativeCellAppendStallP99Micros < 0
                || takeoverRecoveryP99Micros < 0
                || successfulOperations < 0
                || fencedOperations < 0
                || errorOperations < 0
                || timedOutOperations < 0
                || permanentOrphans < 0
                || duplicateLedgerIds < 0
                || reusedLedgerIds < 0
                || failedAssertions < 0
                || skippedAssertions < 0
                || unexpectedErrors < 0
                || metadataOperationCalls < 0
                || metadataRequestBytes < 0
                || metadataResponseBytes < 0
                || grantUseOperations < 0
                || grantWasteIds < 0
                || staleCandidateBurns < 0) {
            throw new IllegalArgumentException("allocator metrics contain an invalid or missing recomputed value");
        }
    }

    /** Compatibility constructor for pre-ADR-0094 local tests; production selection never accepts this aggregate. */
    AllocatorNativeRelativeMetricsV1(
            AllocatorEvidenceWorkloadV1 workload,
            double sustainableRolloverRequestsPerSecond,
            double nativeRolloverRequestsPerSecond,
            long operationP99Micros,
            long queueDepthMaximum,
            long queueAgeP99Micros,
            long topicStarvationMaximumMicros,
            long cellAppendStallP99Micros,
            long takeoverRecoveryP99Micros,
            long successfulOperations,
            long fencedOperations,
            long errorOperations,
            long failedAssertions,
            long skippedAssertions) {
        this(
                workload,
                sustainableRolloverRequestsPerSecond,
                nativeRolloverRequestsPerSecond,
                operationP99Micros,
                operationP99Micros,
                queueDepthMaximum,
                queueAgeP99Micros,
                topicStarvationMaximumMicros,
                cellAppendStallP99Micros,
                cellAppendStallP99Micros,
                takeoverRecoveryP99Micros,
                successfulOperations,
                fencedOperations,
                errorOperations,
                0,
                0,
                0,
                0,
                failedAssertions,
                skippedAssertions,
                0,
                0,
                0,
                0,
                successfulOperations,
                0,
                0);
    }

    public long operationP99Micros() {
        return rolloverEndToEndP99Micros;
    }

    public double nativeThroughputRatio() {
        return sustainableRolloverRequestsPerSecond / nativeRolloverRequestsPerSecond;
    }
}
