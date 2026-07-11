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

package com.nereusstream.metadata.oxia.records;

final class MetadataRecordValidation {
    private MetadataRecordValidation() {
    }

    static void requireNonNegativeNonOverflowingRange(long offset, long length, String fieldName) {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException(fieldName + " offset and length must be non-negative");
        }
        try {
            Math.addExact(offset, length);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(fieldName + " offset + length must not overflow", e);
        }
    }

    static void requirePositiveNonOverflowingRange(long offset, long length, String fieldName) {
        requireNonNegativeNonOverflowingRange(offset, length, fieldName);
        if (length == 0) {
            throw new IllegalArgumentException(fieldName + " length must be positive");
        }
    }

    static void requireDenseLogicalRange(long offsetStart, long offsetEnd, int recordCount, String fieldName) {
        long expectedEnd;
        try {
            expectedEnd = Math.addExact(offsetStart, recordCount);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(fieldName + " start + recordCount must not overflow", e);
        }
        if (offsetEnd != expectedEnd) {
            throw new IllegalArgumentException(fieldName + " end must equal start + recordCount");
        }
    }
}
