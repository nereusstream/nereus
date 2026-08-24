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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class TestControlMetadataStore implements CanonicalControlMetadataStore {
    enum NextMode {
        NORMAL,
        APPLY_BUT_UNKNOWN,
        UNKNOWN_WITHOUT_APPLY
    }

    private final Map<String, CanonicalBytes> values = new LinkedHashMap<>();
    private final List<String> operations = new ArrayList<>();
    private NextMode nextMode = NextMode.NORMAL;

    @Override
    public Optional<CanonicalBytes> get(String key) {
        operations.add("get:" + key);
        return Optional.ofNullable(values.get(key));
    }

    @Override
    public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
        operations.add("create:" + key);
        if (nextMode == NextMode.UNKNOWN_WITHOUT_APPLY) {
            nextMode = NextMode.NORMAL;
            return ControlMutationOutcome.RESPONSE_UNKNOWN;
        }
        CanonicalBytes previous = values.putIfAbsent(key, exactValue);
        if (nextMode == NextMode.APPLY_BUT_UNKNOWN) {
            nextMode = NextMode.NORMAL;
            return ControlMutationOutcome.RESPONSE_UNKNOWN;
        }
        return previous == null ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
    }

    @Override
    public ControlMutationOutcome compareAndSet(
            String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
        operations.add("cas:" + key);
        if (nextMode == NextMode.UNKNOWN_WITHOUT_APPLY) {
            nextMode = NextMode.NORMAL;
            return ControlMutationOutcome.RESPONSE_UNKNOWN;
        }
        Optional<CanonicalBytes> actual = Optional.ofNullable(values.get(key));
        boolean matches = actual.equals(exactExpected);
        if (matches) {
            values.put(key, exactCandidate);
        }
        if (nextMode == NextMode.APPLY_BUT_UNKNOWN) {
            nextMode = NextMode.NORMAL;
            return ControlMutationOutcome.RESPONSE_UNKNOWN;
        }
        return matches ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
    }

    void nextMode(NextMode value) {
        nextMode = value;
    }

    void putExact(String key, CanonicalBytes value) {
        values.put(key, value);
    }

    List<String> operations() {
        return List.copyOf(operations);
    }
}
