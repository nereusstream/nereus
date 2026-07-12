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

package com.nereusstream.managedledger.projection;

import java.util.Objects;
import org.apache.bookkeeper.mledger.Position;

/** One internally consistent Position view over a captured L0 stream snapshot. */
public record StreamPositionBounds(
        long trimOffset,
        long committedEndOffset,
        Position beforeFirstAvailable,
        Position firstAvailable,
        Position lastConfirmed,
        Position onePastLast) {
    public StreamPositionBounds {
        Objects.requireNonNull(beforeFirstAvailable, "beforeFirstAvailable");
        Objects.requireNonNull(firstAvailable, "firstAvailable");
        Objects.requireNonNull(lastConfirmed, "lastConfirmed");
        Objects.requireNonNull(onePastLast, "onePastLast");
        if (trimOffset < 0 || committedEndOffset < trimOffset) {
            throw new ProjectionValidationException("invalid stream position bounds");
        }
        long ledgerId = beforeFirstAvailable.getLedgerId();
        if (ledgerId < VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID || ledgerId >= Long.MAX_VALUE) {
            throw new ProjectionValidationException("position bounds use an invalid virtual ledger");
        }
        if (firstAvailable.getLedgerId() != ledgerId
                || lastConfirmed.getLedgerId() != ledgerId
                || onePastLast.getLedgerId() != ledgerId) {
            throw new ProjectionValidationException("position bounds use different virtual ledgers");
        }
        if (beforeFirstAvailable.getEntryId() != trimOffset - 1
                || firstAvailable.getEntryId() != trimOffset
                || lastConfirmed.getEntryId() != committedEndOffset - 1
                || onePastLast.getEntryId() != committedEndOffset) {
            throw new ProjectionValidationException("position bounds do not match stream offsets");
        }
    }
}
