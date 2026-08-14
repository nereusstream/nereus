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
import java.util.Optional;

/** Exact run-ledger open result. */
public record RunLedgerOpenResultV1(RunLedgerOpenOutcomeV1 outcome, Optional<RunLedgerHandleV1> exactHandle) {
    public RunLedgerOpenResultV1 {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(exactHandle, "exactHandle");
        if ((outcome == RunLedgerOpenOutcomeV1.OPENED_EXACT) != exactHandle.isPresent()) {
            throw new IllegalArgumentException("only OPENED_EXACT carries a handle");
        }
    }

    public static RunLedgerOpenResultV1 openedExact(RunLedgerHandleV1 handle) {
        return new RunLedgerOpenResultV1(RunLedgerOpenOutcomeV1.OPENED_EXACT, Optional.of(handle));
    }

    public static RunLedgerOpenResultV1 withoutHandle(RunLedgerOpenOutcomeV1 outcome) {
        if (outcome == RunLedgerOpenOutcomeV1.OPENED_EXACT) {
            throw new IllegalArgumentException("OPENED_EXACT requires a handle");
        }
        return new RunLedgerOpenResultV1(outcome, Optional.empty());
    }
}
