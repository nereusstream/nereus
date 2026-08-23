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

/**
 * Closed formal-evidence candidate token. It can parameterize the production transition coordinator, but it is not
 * and cannot be converted into runtime activation authority.
 */
public final class AllocatorEvidenceCandidateV1 {
    public static final List<Long> RANGE_SIZES = List.of(16L, 64L, 256L, 1024L);
    private final AllocatorModeV1 mode;
    private final long rangeSize;

    private AllocatorEvidenceCandidateV1(AllocatorModeV1 mode, long rangeSize) {
        this.mode = mode;
        this.rangeSize = rangeSize;
    }

    public static AllocatorEvidenceCandidateV1 strict() {
        return new AllocatorEvidenceCandidateV1(AllocatorModeV1.STRICT_SERIALIZED, 1);
    }

    public static AllocatorEvidenceCandidateV1 range(long rangeSize) {
        if (!RANGE_SIZES.contains(rangeSize)) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "formal RANGE evidence candidate must be one of 16/64/256/1024");
        }
        return new AllocatorEvidenceCandidateV1(AllocatorModeV1.RANGE_LEASED, rangeSize);
    }

    public AllocatorModeV1 mode() {
        return mode;
    }

    public int allocatorProtocolVersion() {
        return VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION;
    }

    public long rangeSize() {
        return rangeSize;
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof AllocatorEvidenceCandidateV1 that
                        && mode == that.mode
                        && rangeSize == that.rangeSize;
    }

    @Override
    public int hashCode() {
        return 31 * mode.hashCode() + Long.hashCode(rangeSize);
    }

    @Override
    public String toString() {
        return mode + "(" + rangeSize + ")";
    }
}
