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

import java.util.Objects;
import java.util.OptionalLong;

/** Protocol-neutral logical-offset condition applied by the append linearization point. */
public record AppendPrecondition(OptionalLong expectedStartOffset) {
    private static final AppendPrecondition NONE = new AppendPrecondition(OptionalLong.empty());

    public AppendPrecondition {
        Objects.requireNonNull(expectedStartOffset, "expectedStartOffset");
        if (expectedStartOffset.isPresent() && expectedStartOffset.getAsLong() < 0) {
            throw new IllegalArgumentException("expectedStartOffset must be non-negative");
        }
    }

    public static AppendPrecondition none() {
        return NONE;
    }

    public static AppendPrecondition expectedStartOffset(long value) {
        return new AppendPrecondition(OptionalLong.of(value));
    }
}
