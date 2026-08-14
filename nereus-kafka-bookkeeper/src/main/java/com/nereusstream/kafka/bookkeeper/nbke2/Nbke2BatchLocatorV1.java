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

package com.nereusstream.kafka.bookkeeper.nbke2;

/** Fixed 32-byte locator row relative to one RANGE_INDEX_BLOCK anchor. */
public record Nbke2BatchLocatorV1(
        long baseOffsetDelta,
        long logicalOffsetCount,
        long entryIdDelta,
        long appendGroupDelta,
        long payloadOffset,
        long payloadLength,
        long physicalChecksumGeneration) {
    private static final long UINT32_MAX = 0xffff_ffffL;

    public Nbke2BatchLocatorV1 {
        requireUnsigned(baseOffsetDelta, Long.MAX_VALUE, "baseOffsetDelta");
        requireUnsigned(logicalOffsetCount, UINT32_MAX, "logicalOffsetCount");
        requireUnsigned(entryIdDelta, UINT32_MAX, "entryIdDelta");
        requireUnsigned(appendGroupDelta, UINT32_MAX, "appendGroupDelta");
        requireUnsigned(payloadOffset, UINT32_MAX, "payloadOffset");
        requireUnsigned(payloadLength, UINT32_MAX, "payloadLength");
        requireUnsigned(physicalChecksumGeneration, UINT32_MAX, "physicalChecksumGeneration");
        if (logicalOffsetCount == 0 || payloadLength == 0 || payloadOffset != 0) {
            throw new IllegalArgumentException("v1 locator requires positive coverage/payload and zero payload offset");
        }
    }

    private static void requireUnsigned(long value, long max, String name) {
        if (value < 0 || value > max) {
            throw new IllegalArgumentException(name + " is outside its unsigned NBKE2 v1 domain");
        }
    }
}
