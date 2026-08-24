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

/** Closed checkpoint provider-proof variants. */
public enum ProviderProofMode {
    NONE(0),
    VERSION_BOUND_FULL_OBJECT_SHA256_V1(1);

    private final int code;

    ProviderProofMode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static ProviderProofMode fromCode(int code) {
        return switch (code) {
            case 0 -> NONE;
            case 1 -> VERSION_BOUND_FULL_OBJECT_SHA256_V1;
            default -> throw new IllegalArgumentException("unknown provider-proof mode: " + code);
        };
    }
}
