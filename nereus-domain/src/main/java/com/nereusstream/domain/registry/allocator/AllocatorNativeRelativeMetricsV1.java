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

/** Source-neutral output structure for real candidate/native comparison; it does not constitute evidence itself. */
public record AllocatorNativeRelativeMetricsV1(
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
    public AllocatorNativeRelativeMetricsV1 {
        if (workload == null
                || !Double.isFinite(sustainableRolloverRequestsPerSecond)
                || sustainableRolloverRequestsPerSecond < 0
                || !Double.isFinite(nativeRolloverRequestsPerSecond)
                || nativeRolloverRequestsPerSecond <= 0
                || operationP99Micros < 0
                || queueDepthMaximum < 0
                || queueAgeP99Micros < 0
                || topicStarvationMaximumMicros < 0
                || cellAppendStallP99Micros < 0
                || takeoverRecoveryP99Micros < 0
                || successfulOperations < 0
                || fencedOperations < 0
                || errorOperations < 0
                || failedAssertions < 0
                || skippedAssertions < 0) {
            throw new IllegalArgumentException("allocator metrics contain an invalid or missing measurement");
        }
    }

    public double nativeThroughputRatio() {
        return sustainableRolloverRequestsPerSecond / nativeRolloverRequestsPerSecond;
    }
}
