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

package com.nereusstream.kafka.bookkeeper.nbke2;

/** Stable corruption/admission rejection families for NBKE2 v1. */
public enum Nbke2RejectionV1 {
    TRUNCATED,
    BAD_MAGIC,
    UNKNOWN_MAJOR,
    UNKNOWN_MINOR,
    UNKNOWN_FRAME_TYPE,
    UNKNOWN_FLAGS,
    RESERVED_NON_ZERO,
    HEADER_LENGTH_MISMATCH,
    TOTAL_LENGTH_INVALID,
    LEDGER_ID_MISMATCH,
    ENTRY_ID_MISMATCH,
    CRC32C_MISMATCH,
    SHA256_MISMATCH,
    FIELD_OUT_OF_DOMAIN,
    COUNT_LIMIT_EXCEEDED,
    LENGTH_LIMIT_EXCEEDED,
    ARITHMETIC_OVERFLOW,
    ORDERING_VIOLATION,
    TRAILING_BYTES
}
