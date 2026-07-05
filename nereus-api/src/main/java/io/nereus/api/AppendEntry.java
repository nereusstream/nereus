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

import java.util.Map;
import java.util.Objects;

/** One caller-visible append entry. */
public record AppendEntry(
        byte[] payload,
        int recordCount,
        long eventTimeMillis,
        Map<String, String> attributes) {
    public AppendEntry {
        payload = Objects.requireNonNull(payload, "payload").clone();
        attributes = Map.copyOf(attributes);
        if (recordCount <= 0) {
            throw new IllegalArgumentException("recordCount must be positive");
        }
        if (eventTimeMillis < 0) {
            throw new IllegalArgumentException("eventTimeMillis must be non-negative");
        }
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
