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

package com.nereusstream.domain.aggregate;

/** The complete V1 Storage Profile discriminator table. */
public enum StorageProfileV1 {
    OBJECT_WAL(1),
    BOOKKEEPER_WAL_ONLY(2),
    BOOKKEEPER_WAL_ASYNC_OBJECT(3);

    private final int code;

    StorageProfileV1(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static StorageProfileV1 fromCode(int code) {
        return switch (code) {
            case 1 -> OBJECT_WAL;
            case 2 -> BOOKKEEPER_WAL_ONLY;
            case 3 -> BOOKKEEPER_WAL_ASYNC_OBJECT;
            default -> throw new IllegalArgumentException("unknown StorageProfileV1 code: " + code);
        };
    }
}
