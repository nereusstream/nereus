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

/** Options for acquiring or stealing an expired append session. */
public record AppendSessionOptions(
        String writerId,
        Duration ttl,
        boolean allowStealExpiredSession) {
    public AppendSessionOptions {
        Objects.requireNonNull(writerId, "writerId");
        Objects.requireNonNull(ttl, "ttl");
        if (writerId.isBlank()) {
            throw new IllegalArgumentException("writerId cannot be blank");
        }
        long ttlMillis;
        try {
            ttlMillis = ttl.toMillis();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("ttl must fit millisecond lease representation", e);
        }
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttl must be at least one millisecond");
        }
    }
}
