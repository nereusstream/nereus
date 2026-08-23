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

import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import java.util.ArrayList;
import java.util.List;

/** One frozen 10k/100k, multi-broker, metadata-latency allocator evidence workload. */
public record AllocatorEvidenceWorkloadV1(
        AllocatorModeV1 mode,
        long selectedRangeSize,
        int activeManagedLedgers,
        int brokerCount,
        int metadataLatencyP99Millis) {
    private static final List<Integer> SCALES = List.of(10_000, 100_000);
    private static final List<Integer> LATENCIES = List.of(1, 5, 10, 25);

    public AllocatorEvidenceWorkloadV1 {
        if (mode == null
                || !SCALES.contains(activeManagedLedgers)
                || brokerCount < 2
                || !LATENCIES.contains(metadataLatencyP99Millis)) {
            throw new IllegalArgumentException("allocator workload must use frozen scale/broker/latency dimensions");
        }
        if ((mode == AllocatorModeV1.STRICT_SERIALIZED && selectedRangeSize != 1)
                || (mode == AllocatorModeV1.RANGE_LEASED
                        && (selectedRangeSize <= 1 || selectedRangeSize > VirtualLedgerSliceAssignmentV1.SLICE_SIZE))) {
            throw new IllegalArgumentException("allocator workload range size does not match its mode");
        }
    }

    /** Deterministic complete scale matrix; execution and measurements remain external and source-qualified. */
    public static List<AllocatorEvidenceWorkloadV1> completeMatrix(
            AllocatorModeV1 mode, long selectedRangeSize, int brokerCount) {
        List<AllocatorEvidenceWorkloadV1> workloads = new ArrayList<>(SCALES.size() * LATENCIES.size());
        for (int scale : SCALES) {
            for (int latency : LATENCIES) {
                workloads.add(new AllocatorEvidenceWorkloadV1(mode, selectedRangeSize, scale, brokerCount, latency));
            }
        }
        return List.copyOf(workloads);
    }
}
