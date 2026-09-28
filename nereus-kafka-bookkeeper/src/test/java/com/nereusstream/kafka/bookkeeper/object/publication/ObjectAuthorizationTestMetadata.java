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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Exact byte CAS for local publication contract tests only. Real evidence uses Oxia. */
final class ObjectAuthorizationTestMetadata implements CanonicalControlMetadataStore {
    private final Map<String, CanonicalBytes> values = new HashMap<>();

    @Override
    public synchronized Optional<CanonicalBytes> get(String key) {
        return Optional.ofNullable(values.get(key));
    }

    @Override
    public synchronized ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
        var previous = values.putIfAbsent(key, value);
        return previous == null || previous.equals(value)
                ? ControlMutationOutcome.APPLIED
                : ControlMutationOutcome.DEFINITIVE_CONFLICT;
    }

    @Override
    public synchronized ControlMutationOutcome compareAndSet(
            String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
        if (!get(key).equals(expected)) {
            return ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }
        values.put(key, value);
        return ControlMutationOutcome.APPLIED;
    }
}
