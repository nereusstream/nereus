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

package com.nereusstream.api;

import java.time.Duration;
import java.util.Objects;

/** Options for advancing a stream trim low-watermark. */
public record TrimOptions(
        Duration timeout,
        String reason) {
    public TrimOptions {
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(reason, "reason");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
