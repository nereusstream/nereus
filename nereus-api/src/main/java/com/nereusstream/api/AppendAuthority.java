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

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** External monotonic leadership identity used to preempt an append session without waiting for TTL. */
public record AppendAuthority(
        String authorityType,
        String authorityId,
        long authorityEpoch,
        String ownerId,
        long ownerEpoch) {
    private static final int MAX_COMPONENT_BYTES = 16 * 1024;

    public AppendAuthority {
        authorityType = requireText(authorityType, "authorityType");
        authorityId = requireText(authorityId, "authorityId");
        ownerId = requireText(ownerId, "ownerId");
        if (authorityEpoch < 0 || ownerEpoch < 0) {
            throw new IllegalArgumentException("append authority epochs must be non-negative");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > MAX_COMPONENT_BYTES) {
            throw new IllegalArgumentException(field + " must be nonblank and bounded");
        }
        return value;
    }
}
