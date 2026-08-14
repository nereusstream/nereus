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

package com.nereusstream.storage.api.bookkeeper;

import java.util.Objects;

/** One explicit-entry append request with replayable bytes retained by the caller. */
public record RunLedgerAppendRequestV1(RunLedgerHandleV1 handle, long expectedEntryId, RetainedStoragePayload payload) {
    public RunLedgerAppendRequestV1 {
        Objects.requireNonNull(handle, "handle");
        if (expectedEntryId < 0) {
            throw new IllegalArgumentException("entry ID must be non-negative");
        }
        Objects.requireNonNull(payload, "payload");
        if (payload.readableBytes() <= 0 || payload.readOnlyBuffer().remaining() != payload.readableBytes()) {
            throw new IllegalArgumentException("payload must be non-empty and expose its exact readable length");
        }
    }
}
