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

/** Immutable, position-ordered routes published only at a low-frequency generation cut. */
public final class BindingReadRouteTableV1 {
    private final BindingReadRouteV1[] routes;

    public BindingReadRouteTableV1(List<BindingReadRouteV1> routes) {
        Objects.requireNonNull(routes, "routes");
        this.routes = routes.toArray(BindingReadRouteV1[]::new);
        long previousEnd = -1;
        for (BindingReadRouteV1 route : this.routes) {
            Objects.requireNonNull(route, "route");
            if (previousEnd > route.startInclusive()) {
                throw new IllegalArgumentException("read routes overlap or are not position ordered");
            }
            previousEnd = route.endExclusive();
        }
    }

    public int size() {
        return routes.length;
    }

    public BindingReadRouteV1 route(int index) {
        return routes[index];
    }
}
