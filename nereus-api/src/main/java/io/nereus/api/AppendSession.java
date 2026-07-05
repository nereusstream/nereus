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

import java.util.Objects;

/** Fencing information for appends to one stream. */
public record AppendSession(
        StreamId streamId,
        String writerId,
        long epoch,
        String fencingToken,
        long leaseVersion,
        long expiresAtMillis) {
    public AppendSession {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(writerId, "writerId");
        Objects.requireNonNull(fencingToken, "fencingToken");
        if (writerId.isBlank() || fencingToken.isBlank()) {
            throw new IllegalArgumentException("writerId and fencingToken cannot be blank");
        }
        if (epoch < 0 || leaseVersion < 0 || expiresAtMillis < 0) {
            throw new IllegalArgumentException("session numeric fields must be non-negative");
        }
    }
}
