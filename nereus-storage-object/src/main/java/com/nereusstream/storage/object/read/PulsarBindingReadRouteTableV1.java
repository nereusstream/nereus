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

import java.util.List;
import java.util.Objects;

/** Immutable lexicographically ordered Pulsar virtual-ledger source routes. */
public final class PulsarBindingReadRouteTableV1 {
    private final PulsarBindingReadRouteV1[] routes;

    public PulsarBindingReadRouteTableV1(List<PulsarBindingReadRouteV1> routes) {
        Objects.requireNonNull(routes, "routes");
        this.routes = routes.toArray(PulsarBindingReadRouteV1[]::new);
        long previousLedger = -1;
        long previousEnd = -1;
        for (PulsarBindingReadRouteV1 route : this.routes) {
            Objects.requireNonNull(route, "route");
            if (route.virtualLedgerId() < previousLedger
                    || route.virtualLedgerId() == previousLedger && route.startEntryIdInclusive() < previousEnd) {
                throw new IllegalArgumentException("Pulsar routes overlap or are not ledger/entry ordered");
            }
            if (route.virtualLedgerId() != previousLedger) {
                previousLedger = route.virtualLedgerId();
            }
            previousEnd = route.endEntryIdExclusive();
        }
    }

    public int size() {
        return routes.length;
    }

    public PulsarBindingReadRouteV1 route(int index) {
        return routes[index];
    }
}
