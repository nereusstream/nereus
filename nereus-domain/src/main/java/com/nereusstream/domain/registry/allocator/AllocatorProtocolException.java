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

/** Closed production allocator rejection with a stable machine-readable code. */
public final class AllocatorProtocolException extends IllegalArgumentException {
    private final Code code;

    public AllocatorProtocolException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public AllocatorProtocolException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() {
        return code;
    }

    public enum Code {
        SELECTION_NOT_ELIGIBLE,
        SOURCE_MISMATCH,
        PROTOCOL_VERSION,
        MODE_MISMATCH,
        SLICE_NOT_ACTIVE,
        SLICE_IDENTITY_DRIFT,
        SLICE_EXHAUSTED,
        RESERVATION_BUSY,
        RANGE_TAIL_NOT_EXHAUSTED,
        CELL_STATE_DRIFT,
        HEAD_IDENTITY,
        HEAD_GEOMETRY,
        HEAD_STATE_DRIFT,
        OWNER_FENCED,
        GRANT_NOT_INSTALLED,
        RANGE_EXHAUSTED,
        CANDIDATE_CONFLICT,
        CANDIDATE_OCCUPANCY_NOT_PROVEN,
        STALE_CANDIDATE_REQUIRED,
        REQUEST_CONTEXT_DRIFT,
        DESCRIPTOR_MISMATCH,
        RECONCILE_RETRY_EXHAUSTED,
        WORKFLOW_DEADLINE_EXCEEDED,
        RETRY_BACKOFF_EXCEEDED,
        NON_CANONICAL_WIRE
    }
}
