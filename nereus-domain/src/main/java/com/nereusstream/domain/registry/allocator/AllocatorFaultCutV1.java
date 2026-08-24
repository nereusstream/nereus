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

/** Exact ADR-0094 fault cut encoded in raw-event flag bits {@code 0..3}. */
public enum AllocatorFaultCutV1 {
    RESERVE_RESPONSE_LOSS(1),
    MODE_GRANT_READY_RESPONSE_LOSS_OR_STRICT_NO_INSTALL(2),
    NODE_CREATE_RESPONSE_LOSS(3),
    HEAD_PUBLISH_RESPONSE_LOSS(4),
    CELL_CLEAR_RESPONSE_LOSS(5),
    SINGLE_OWNER_TAKEOVER(6),
    LATE_OLD_OWNER_WRITE(7),
    BROKER_SESSION_CRASH_MASS_TAKEOVER(8),
    SYNCHRONIZED_STORM(9);

    private final int code;

    AllocatorFaultCutV1(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    static AllocatorFaultCutV1 fromFlags(int flags) {
        int code = flags & AllocatorRawEvidenceEventV1.FLAG_FAULT_CUT_MASK;
        for (AllocatorFaultCutV1 value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw AllocatorSelectionReceiptV1.invalid("allocator fault event has no exact ADR-0094 cut kind");
    }
}
