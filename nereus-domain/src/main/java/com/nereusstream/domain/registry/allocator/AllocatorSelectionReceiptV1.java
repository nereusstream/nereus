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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Validated selection receipt input. Constructing it does not run or fabricate provider/native evidence. */
public record AllocatorSelectionReceiptV1(
        AllocatorModeV1 selectedMode,
        int allocatorProtocolVersion,
        long selectedRangeSize,
        String nereusSourceCommit,
        Sha256Digest evidenceReceiptSha256,
        List<AllocatorNativeRelativeMetricsV1> completeScaleMetrics,
        int deterministicFaultCuts,
        int completedFaultCuts,
        boolean multiBrokerCrashAndMassTakeoverCompleted,
        boolean thresholdsFrozenBeforeRun,
        boolean selectionEligible) {
    public static final int REQUIRED_FAULT_CUTS = 9;
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");

    public AllocatorSelectionReceiptV1 {
        Objects.requireNonNull(selectedMode, "selectedMode");
        Objects.requireNonNull(nereusSourceCommit, "nereusSourceCommit");
        Objects.requireNonNull(evidenceReceiptSha256, "evidenceReceiptSha256");
        completeScaleMetrics = List.copyOf(Objects.requireNonNull(completeScaleMetrics, "completeScaleMetrics"));
        if (allocatorProtocolVersion != VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION
                || !COMMIT.matcher(nereusSourceCommit).matches()
                || evidenceReceiptSha256.isZero()
                || (selectedMode == AllocatorModeV1.STRICT_SERIALIZED && selectedRangeSize != 1)
                || (selectedMode == AllocatorModeV1.RANGE_LEASED
                        && (selectedRangeSize <= 1 || selectedRangeSize > VirtualLedgerSliceAssignmentV1.SLICE_SIZE))) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator selection identity/version/source/range is invalid");
        }
        Set<AllocatorEvidenceWorkloadV1> expected = Set.copyOf(AllocatorEvidenceWorkloadV1.completeMatrix(
                selectedMode, selectedRangeSize, minimumBrokerCount(completeScaleMetrics)));
        Set<AllocatorEvidenceWorkloadV1> actual = completeScaleMetrics.stream()
                .map(AllocatorNativeRelativeMetricsV1::workload)
                .collect(Collectors.toUnmodifiableSet());
        boolean allZeroFailure = completeScaleMetrics.stream()
                .allMatch(metric -> metric.successfulOperations() > 0
                        && metric.errorOperations() == 0
                        && metric.failedAssertions() == 0
                        && metric.skippedAssertions() == 0);
        if (!selectionEligible
                || !thresholdsFrozenBeforeRun
                || !multiBrokerCrashAndMassTakeoverCompleted
                || deterministicFaultCuts != REQUIRED_FAULT_CUTS
                || completedFaultCuts != REQUIRED_FAULT_CUTS
                || completeScaleMetrics.size() != expected.size()
                || !actual.equals(expected)
                || !allZeroFailure) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator receipt lacks complete zero-failure 10k/100k native-relative evidence");
        }
    }

    public AllocatorActivationV1 activate(String exactRunningSourceCommit) {
        return AllocatorActivationV1.fromReceipt(this, exactRunningSourceCommit);
    }

    private static int minimumBrokerCount(List<AllocatorNativeRelativeMetricsV1> metrics) {
        if (metrics.isEmpty()) {
            return 2;
        }
        int brokerCount = metrics.get(0).workload().brokerCount();
        if (metrics.stream().anyMatch(metric -> metric.workload().brokerCount() != brokerCount)) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator evidence broker count differs across the scale matrix");
        }
        return brokerCount;
    }
}
