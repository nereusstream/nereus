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

/** Closed Provider access strategy. Only C1 is M3-promotable; C2 remains implemented but evidence-gated. */
public enum ObjectProviderAccessProfile {
    C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST(1, true),
    C2_SINGLE_PUT_SEGMENTED_PREFIX_STRONG_LIST(2, false);

    private final int code;
    private final boolean m3Promotable;

    ObjectProviderAccessProfile(int code, boolean m3Promotable) {
        this.code = code;
        this.m3Promotable = m3Promotable;
    }

    public int code() {
        return code;
    }

    public boolean m3Promotable() {
        return m3Promotable;
    }

    public static ObjectProviderAccessProfile fromCode(int code) {
        return switch (code) {
            case 1 -> C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST;
            case 2 -> C2_SINGLE_PUT_SEGMENTED_PREFIX_STRONG_LIST;
            default -> throw new IllegalArgumentException("unknown Object Provider access profile: " + code);
        };
    }
}
