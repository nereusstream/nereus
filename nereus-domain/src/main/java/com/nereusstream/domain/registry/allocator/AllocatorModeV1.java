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

package com.nereusstream.domain.registry.allocator;

/** The two evidence-owned production allocator candidates. */
public enum AllocatorModeV1 {
    STRICT_SERIALIZED(1),
    RANGE_LEASED(2);

    private final int code;

    AllocatorModeV1(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    static AllocatorModeV1 fromCode(int code) {
        return switch (code) {
            case 1 -> STRICT_SERIALIZED;
            case 2 -> RANGE_LEASED;
            default -> throw AllocatorWireV1.invalid("unknown allocator mode: " + code);
        };
    }
}
