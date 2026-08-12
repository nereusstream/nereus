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

package com.nereusstream.metadata.oxia.v2.capability;

/** Fail-closed control-plane failure from the P1 selector/aggregate coordinator. */
public final class PulsarTopicAuthorityException extends IllegalStateException {
    public enum Kind {
        DEFINITIVE_CONFLICT,
        INDETERMINATE,
        INVALID_STATE,
        MISSING_AGGREGATE
    }

    private final Kind kind;

    public PulsarTopicAuthorityException(Kind kind, String message) {
        super(message);
        this.kind = java.util.Objects.requireNonNull(kind, "kind");
    }

    public Kind kind() {
        return kind;
    }
}
