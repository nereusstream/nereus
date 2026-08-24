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

package com.nereusstream.storage.object.provider;

/** Exact Provider capability snapshot required by the C1 strategy. */
public record ObjectProviderCapabilities(
        String providerIdentity,
        boolean conditionalCreate,
        boolean singleRangeGet,
        boolean streamingFullGet,
        boolean stronglyConsistentPaginatedList,
        boolean definitiveAbsence,
        long maximumObjectBytes,
        int maximumRangeBytes,
        int maximumListPageKeys) {
    public ObjectProviderCapabilities {
        if (providerIdentity == null || providerIdentity.isBlank()) {
            throw new IllegalArgumentException("providerIdentity must be non-blank");
        }
        if (maximumObjectBytes <= 0 || maximumRangeBytes <= 0 || maximumListPageKeys <= 0) {
            throw new IllegalArgumentException("Provider caps must be positive");
        }
    }

    public void requireC1() {
        if (!conditionalCreate
                || !singleRangeGet
                || !streamingFullGet
                || !stronglyConsistentPaginatedList
                || !definitiveAbsence) {
            throw new IllegalArgumentException("Provider does not satisfy the C1 capability contract");
        }
    }
}
