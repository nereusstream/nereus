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

package com.nereusstream.storage.api.bookkeeper;

/** Immutable timeout class captured by an admitted Cell session. */
public record BookKeeperTimeoutClassV1(long connectMillis, long addMillis, long readMillis, long recoveryMillis) {
    public BookKeeperTimeoutClassV1 {
        requirePositive(connectMillis, "connectMillis");
        requirePositive(addMillis, "addMillis");
        requirePositive(readMillis, "readMillis");
        requirePositive(recoveryMillis, "recoveryMillis");
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
