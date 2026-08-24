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

import java.util.List;
import java.util.Objects;

/** Closed ADR-0094 candidate/native population-latency-rate context compressed into one unsigned 16-bit ID. */
public record AllocatorEvidenceContextV1(
        int contextId,
        boolean nativePath,
        AllocatorEvidenceCandidateV1 candidate,
        int activeManagedLedgers,
        int metadataLatencyP99Millis,
        int offeredRolloverRequestsPerSecond) {
    public static final List<Integer> POPULATIONS = List.of(10_000, 100_000);
    public static final List<Integer> LATENCIES_MILLIS = List.of(1, 5, 10, 25);
    public static final List<Integer> OFFERED_RATES = List.of(200, 250, 333, 500, 750, 1000);
    private static final int NATIVE_CONTEXTS = 48;
    private static final int CONTEXTS_PER_CANDIDATE = 48;

    public AllocatorEvidenceContextV1 {
        if (contextId < 0
                || contextId >= NATIVE_CONTEXTS + 5 * CONTEXTS_PER_CANDIDATE
                || nativePath != (candidate == null)
                || !POPULATIONS.contains(activeManagedLedgers)
                || !LATENCIES_MILLIS.contains(metadataLatencyP99Millis)
                || !OFFERED_RATES.contains(offeredRolloverRequestsPerSecond)) {
            throw new IllegalArgumentException("allocator evidence context is outside the frozen ADR-0094 matrix");
        }
        int expectedContextId;
        int exactRow = row(activeManagedLedgers, metadataLatencyP99Millis, offeredRolloverRequestsPerSecond);
        if (nativePath) {
            expectedContextId = exactRow;
        } else {
            int candidateIndex = candidate.mode() == AllocatorModeV1.STRICT_SERIALIZED
                    ? 0
                    : 1 + AllocatorEvidenceCandidateV1.RANGE_SIZES.indexOf(candidate.rangeSize());
            expectedContextId = NATIVE_CONTEXTS + candidateIndex * CONTEXTS_PER_CANDIDATE + exactRow;
        }
        if (contextId != expectedContextId) {
            throw new IllegalArgumentException("allocator evidence context ID aliases different frozen dimensions");
        }
    }

    public static AllocatorEvidenceContextV1 nativeContext(
            int activeManagedLedgers, int latencyMillis, int offeredRate) {
        int row = row(activeManagedLedgers, latencyMillis, offeredRate);
        return new AllocatorEvidenceContextV1(row, true, null, activeManagedLedgers, latencyMillis, offeredRate);
    }

    public static AllocatorEvidenceContextV1 candidateContext(
            AllocatorEvidenceCandidateV1 candidate, int activeManagedLedgers, int latencyMillis, int offeredRate) {
        Objects.requireNonNull(candidate, "candidate");
        int candidateIndex = candidate.mode() == AllocatorModeV1.STRICT_SERIALIZED
                ? 0
                : 1 + AllocatorEvidenceCandidateV1.RANGE_SIZES.indexOf(candidate.rangeSize());
        int contextId = NATIVE_CONTEXTS
                + candidateIndex * CONTEXTS_PER_CANDIDATE
                + row(activeManagedLedgers, latencyMillis, offeredRate);
        return new AllocatorEvidenceContextV1(
                contextId, false, candidate, activeManagedLedgers, latencyMillis, offeredRate);
    }

    public static AllocatorEvidenceContextV1 fromId(int contextId) {
        if (contextId < 0 || contextId >= NATIVE_CONTEXTS + 5 * CONTEXTS_PER_CANDIDATE) {
            throw AllocatorSelectionReceiptV1.invalid("allocator raw event context ID is unknown");
        }
        boolean nativePath = contextId < NATIVE_CONTEXTS;
        int local = nativePath ? contextId : (contextId - NATIVE_CONTEXTS) % CONTEXTS_PER_CANDIDATE;
        int population = POPULATIONS.get(local / 24);
        int latency = LATENCIES_MILLIS.get((local % 24) / 6);
        int rate = OFFERED_RATES.get(local % 6);
        if (nativePath) {
            return nativeContext(population, latency, rate);
        }
        int candidateIndex = (contextId - NATIVE_CONTEXTS) / CONTEXTS_PER_CANDIDATE;
        AllocatorEvidenceCandidateV1 candidate = candidateIndex == 0
                ? AllocatorEvidenceCandidateV1.strict()
                : AllocatorEvidenceCandidateV1.range(AllocatorEvidenceCandidateV1.RANGE_SIZES.get(candidateIndex - 1));
        return candidateContext(candidate, population, latency, rate);
    }

    private static int row(int population, int latency, int rate) {
        int populationIndex = POPULATIONS.indexOf(population);
        int latencyIndex = LATENCIES_MILLIS.indexOf(latency);
        int rateIndex = OFFERED_RATES.indexOf(rate);
        if (populationIndex < 0 || latencyIndex < 0 || rateIndex < 0) {
            throw new IllegalArgumentException("allocator evidence context dimensions are not frozen ADR-0094 values");
        }
        return populationIndex * 24 + latencyIndex * 6 + rateIndex;
    }
}
