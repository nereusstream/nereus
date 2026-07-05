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

package io.nereus.api;

import java.time.Duration;
import java.util.Objects;

/** Limits and timeout for a read call. */
public record ReadOptions(
        int maxRecords,
        int maxBytes,
        ReadIsolation isolation,
        Duration timeout) {
    public ReadOptions {
        Objects.requireNonNull(isolation, "isolation");
        Objects.requireNonNull(timeout, "timeout");
        if (maxRecords <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("maxRecords and maxBytes must be positive");
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
