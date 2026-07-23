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

/** Stable API error code for exceptional public futures. */
public enum ErrorCode {
    INVALID_ARGUMENT,
    CANCELLED,
    STREAM_NOT_FOUND,
    STREAM_NOT_ACTIVE,
    APPEND_SESSION_EXPIRED,
    FENCED_APPEND,
    OFFSET_CONFLICT,
    BACKPRESSURE_REJECTED,
    TIMEOUT,
    STORAGE_CLOSED,
    OBJECT_UPLOAD_FAILED,
    OBJECT_READ_FAILED,
    OBJECT_NOT_FOUND,
    OBJECT_CHECKSUM_MISMATCH,
    PRIMARY_WAL_WRITE_FAILED,
    PRIMARY_WAL_READ_FAILED,
    PRIMARY_WAL_TARGET_NOT_FOUND,
    PRIMARY_WAL_CHECKSUM_MISMATCH,
    UNSUPPORTED_READ_TARGET,
    OFFSET_TRIMMED,
    OFFSET_NOT_AVAILABLE,
    READ_LIMIT_TOO_SMALL,
    METADATA_UNAVAILABLE,
    METADATA_CONDITION_FAILED,
    METADATA_INVARIANT_VIOLATION,
    READ_RESOLUTION_FAILED,
    UNSUPPORTED_STORAGE_PROFILE,
    UNSUPPORTED_DURABILITY_LEVEL,
    UNSUPPORTED_FORMAT,
    METADATA_LIMIT_EXCEEDED,
    UNSUPPORTED_APPEND_PRECONDITION,
    UNSUPPORTED_READ_SEMANTICS,
    UNSUPPORTED_APPEND_AUTHORITY
}
