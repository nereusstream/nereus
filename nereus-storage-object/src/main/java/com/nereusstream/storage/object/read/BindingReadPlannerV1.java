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

/** Pure deterministic route derivation into caller-owned bounded storage. */
public final class BindingReadPlannerV1 {
    public enum Outcome {
        PLANNED,
        EMPTY,
        INVALID_RANGE,
        SAFE_FAILURE_GAP_OR_AMBIGUITY,
        SAFE_FAILURE_CAPACITY
    }

    private BindingReadPlannerV1() {}

    public static Outcome plan(
            BindingReadPublicationCellV1 cell,
            long requestedStartInclusive,
            long requestedEndExclusive,
            long protocolUpperBound,
            BindingReadPlanBufferV1 output) {
        Objects.requireNonNull(cell, "cell");
        Objects.requireNonNull(output, "output");
        output.reset();
        if (requestedStartInclusive < 0
                || requestedEndExclusive < requestedStartInclusive
                || protocolUpperBound < 0
                || protocolUpperBound > cell.readableUpperBound()) {
            return Outcome.INVALID_RANGE;
        }
        long boundedEnd = Math.min(requestedEndExclusive, protocolUpperBound);
        if (requestedStartInclusive >= boundedEnd) {
            return Outcome.EMPTY;
        }

        long cursor = requestedStartInclusive;
        BindingReadRouteTableV1 table = cell.routes();
        for (int index = 0; index < table.size() && cursor < boundedEnd; index++) {
            BindingReadRouteV1 route = table.route(index);
            if (route.endExclusive() <= cursor) {
                continue;
            }
            if (route.startInclusive() > cursor) {
                output.reset();
                return Outcome.SAFE_FAILURE_GAP_OR_AMBIGUITY;
            }
            long intervalEnd = Math.min(route.endExclusive(), boundedEnd);
            if (!output.append(cursor, intervalEnd, route)) {
                output.reset();
                return Outcome.SAFE_FAILURE_CAPACITY;
            }
            cursor = intervalEnd;
        }
        if (cursor != boundedEnd) {
            output.reset();
            return Outcome.SAFE_FAILURE_GAP_OR_AMBIGUITY;
        }
        return Outcome.PLANNED;
    }
}
