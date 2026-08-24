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

package com.nereusstream.storage.object.control;

/** Root-frozen physical checkpoint policy. Proactive cadence may be zero, but hard uncovered bounds may not. */
public record WalCheckpointPolicy(
        long proactiveCadenceMillis,
        int maxUncheckpointedExtents,
        long maxUncheckpointedBytes,
        long maxUncheckpointedAgeMillis,
        int maxRowsPerPage,
        int maxCanonicalPageBytes) {
    public static final int FORMAT_MAX_ROWS_PER_PAGE = 256;
    public static final int FORMAT_MAX_CANONICAL_PAGE_BYTES = 64 * 1024;
    public static final int SUCCESSOR_PAGE_FIXED_BYTES = 107;
    public static final int PROOF_NONE_ROW_BYTES = 56;

    public WalCheckpointPolicy {
        if (proactiveCadenceMillis < 0) {
            throw new IllegalArgumentException("proactiveCadenceMillis must be non-negative");
        }
        if (maxUncheckpointedExtents <= 0 || maxUncheckpointedBytes <= 0 || maxUncheckpointedAgeMillis <= 0) {
            throw new IllegalArgumentException("uncheckpointed-tail bounds must be positive");
        }
        if (maxRowsPerPage <= 0 || maxRowsPerPage > FORMAT_MAX_ROWS_PER_PAGE) {
            throw new IllegalArgumentException("maxRowsPerPage exceeds the format bound");
        }
        if (maxCanonicalPageBytes <= 0 || maxCanonicalPageBytes > FORMAT_MAX_CANONICAL_PAGE_BYTES) {
            throw new IllegalArgumentException("maxCanonicalPageBytes exceeds the format bound");
        }
        int requiredProofNonePageBytes =
                Math.addExact(SUCCESSOR_PAGE_FIXED_BYTES, Math.multiplyExact(maxRowsPerPage, PROOF_NONE_ROW_BYTES));
        if (maxCanonicalPageBytes < requiredProofNonePageBytes) {
            throw new IllegalArgumentException(
                    "maxCanonicalPageBytes cannot encode maxRowsPerPage proof-NONE successor rows");
        }
    }
}
