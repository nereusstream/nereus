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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

/** Stable fail-closed NWKCP1 decode rejection identities. */
public enum Nwkcp1RejectionV1 {
    OBJECT_TOO_LARGE,
    OBJECT_LENGTH,
    OBJECT_DIGEST,
    OBJECT_KEY,
    TRUNCATED,
    MAGIC_VERSION,
    HEADER_LENGTH,
    HEADER_FLAGS,
    HEADER_CRC,
    ROOT_IDENTITY,
    ROW_COUNT,
    ROW_LENGTH,
    ROW_DIGEST,
    ROW_STATE,
    ROW_ORDER,
    TRAILING_BYTES
}
