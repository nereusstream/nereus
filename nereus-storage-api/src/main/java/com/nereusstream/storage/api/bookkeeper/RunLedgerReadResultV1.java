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

/** Exact entry read result carrying bytes only for FOUND_EXACT. */
public record RunLedgerReadResultV1(RunLedgerReadOutcomeV1 outcome, Optional<ExactLedgerEntryV1> exactEntry) {
    public RunLedgerReadResultV1 {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(exactEntry, "exactEntry");
        if ((outcome == RunLedgerReadOutcomeV1.FOUND_EXACT) != exactEntry.isPresent()) {
            throw new IllegalArgumentException("only FOUND_EXACT carries entry bytes");
        }
    }

    public static RunLedgerReadResultV1 foundExact(ExactLedgerEntryV1 entry) {
        return new RunLedgerReadResultV1(RunLedgerReadOutcomeV1.FOUND_EXACT, Optional.of(entry));
    }

    public static RunLedgerReadResultV1 withoutEntry(RunLedgerReadOutcomeV1 outcome) {
        if (outcome == RunLedgerReadOutcomeV1.FOUND_EXACT) {
            throw new IllegalArgumentException("FOUND_EXACT requires entry bytes");
        }
        return new RunLedgerReadResultV1(outcome, Optional.empty());
    }
}
