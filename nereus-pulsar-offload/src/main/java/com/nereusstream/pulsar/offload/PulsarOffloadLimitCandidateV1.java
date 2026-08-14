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

package com.nereusstream.pulsar.offload;

import java.util.List;

/** Predeclared ADR-0056/0057 evidence candidates; this type is not a selected production default. */
public record PulsarOffloadLimitCandidateV1(
        long maxDataObjectBytes,
        int maxMultipartParts,
        long maxEntryBytes,
        long maxDecodedBlockBytes,
        int maxEntriesPerBlock,
        List<Integer> blockTargetBytes) {
    public static final long FOUR_GIB = 4L * 1_024 * 1_024 * 1_024;
    public static final int MIB = 1_024 * 1_024;

    public PulsarOffloadLimitCandidateV1 {
        blockTargetBytes = List.copyOf(blockTargetBytes);
        if (maxDataObjectBytes <= 32
                || maxMultipartParts <= 0
                || maxEntryBytes <= 0
                || maxDecodedBlockBytes < maxEntryBytes
                || maxEntriesPerBlock <= 0) {
            throw new IllegalArgumentException("candidate numeric limit is non-positive or inconsistent");
        }
        if (!blockTargetBytes.equals(List.of(MIB, 4 * MIB, 8 * MIB, 16 * MIB))) {
            throw new IllegalArgumentException("block evidence candidates must remain exactly 1/4/8/16 MiB");
        }
        if (blockTargetBytes.get(blockTargetBytes.size() - 1) > maxDecodedBlockBytes) {
            throw new IllegalArgumentException("block target exceeds decoded block candidate cap");
        }
    }

    public static PulsarOffloadLimitCandidateV1 adr0056EvidenceCandidate() {
        return new PulsarOffloadLimitCandidateV1(
                FOUR_GIB, 1_024, 64L * MIB, 64L * MIB, 65_536, List.of(MIB, 4 * MIB, 8 * MIB, 16 * MIB));
    }
}
