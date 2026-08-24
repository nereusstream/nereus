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

/** Permanent Object WAL lane identities. The numeric code is also the complete one-digit leaf token. */
public enum WalLaneId {
    OBJECT_LATENCY(0),
    OBJECT_BALANCED(1),
    OBJECT_COST(2);

    private final int code;

    WalLaneId(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public String leafToken() {
        return Integer.toString(code);
    }

    public static WalLaneId fromCode(int code) {
        return switch (code) {
            case 0 -> OBJECT_LATENCY;
            case 1 -> OBJECT_BALANCED;
            case 2 -> OBJECT_COST;
            default -> throw new IllegalArgumentException("unknown Object WAL lane code: " + code);
        };
    }
}
