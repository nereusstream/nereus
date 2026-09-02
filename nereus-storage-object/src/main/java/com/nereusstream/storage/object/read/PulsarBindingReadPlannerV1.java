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

package com.nereusstream.storage.object.read;

import java.util.Objects;

/** Pure Pulsar ledger/entry planner that never aliases a virtual-ledger ID into one long offset. */
public final class PulsarBindingReadPlannerV1 {
    private PulsarBindingReadPlannerV1() {}

    public static BindingReadPlannerV1.Outcome plan(
            PulsarBindingReadRouteTableV1 table,
            long virtualLedgerId,
            long requestedStartEntryIdInclusive,
            long requestedEndEntryIdExclusive,
            long protocolUpperBoundEntryIdExclusive,
            PulsarBindingReadPlanBufferV1 output) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(output, "output");
        output.reset();
        if (virtualLedgerId <= 0
                || requestedStartEntryIdInclusive < 0
                || requestedEndEntryIdExclusive < requestedStartEntryIdInclusive
                || protocolUpperBoundEntryIdExclusive < 0) {
            return BindingReadPlannerV1.Outcome.INVALID_RANGE;
        }
        long boundedEnd = Math.min(requestedEndEntryIdExclusive, protocolUpperBoundEntryIdExclusive);
        if (requestedStartEntryIdInclusive >= boundedEnd) {
            return BindingReadPlannerV1.Outcome.EMPTY;
        }
        long cursor = requestedStartEntryIdInclusive;
        for (int index = 0; index < table.size() && cursor < boundedEnd; index++) {
            PulsarBindingReadRouteV1 route = table.route(index);
            if (route.virtualLedgerId() < virtualLedgerId
                    || route.virtualLedgerId() == virtualLedgerId && route.endEntryIdExclusive() <= cursor) {
                continue;
            }
            if (route.virtualLedgerId() > virtualLedgerId || route.startEntryIdInclusive() > cursor) {
                output.reset();
                return BindingReadPlannerV1.Outcome.SAFE_FAILURE_GAP_OR_AMBIGUITY;
            }
            long intervalEnd = Math.min(route.endEntryIdExclusive(), boundedEnd);
            if (!output.append(cursor, intervalEnd, route)) {
                output.reset();
                return BindingReadPlannerV1.Outcome.SAFE_FAILURE_CAPACITY;
            }
            cursor = intervalEnd;
        }
        if (cursor != boundedEnd) {
            output.reset();
            return BindingReadPlannerV1.Outcome.SAFE_FAILURE_GAP_OR_AMBIGUITY;
        }
        return BindingReadPlannerV1.Outcome.PLANNED;
    }
}
