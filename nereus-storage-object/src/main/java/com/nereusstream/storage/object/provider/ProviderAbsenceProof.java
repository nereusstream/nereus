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

/** Strong complete-LIST plus exact full-GET typed-NOT_FOUND receipt for one immutable key. */
public record ProviderAbsenceProof(
        String key,
        String listedPrefix,
        int listPages,
        long listedKeys,
        boolean exactFullGetNotFound,
        int headRequests) {
    public ProviderAbsenceProof {
        if (key == null
                || key.isEmpty()
                || listedPrefix == null
                || listedPrefix.isEmpty()
                || !key.startsWith(listedPrefix)
                || listPages <= 0
                || listedKeys < 0
                || !exactFullGetNotFound
                || headRequests != 0) {
            throw new IllegalArgumentException("Provider absence proof is invalid");
        }
    }
}
