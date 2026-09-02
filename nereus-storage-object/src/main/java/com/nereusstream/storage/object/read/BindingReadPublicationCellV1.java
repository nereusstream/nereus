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

/** Single-reference coherent publication cell; its referenced graph is immutable. */
public record BindingReadPublicationCellV1(
        long sourceGeneration,
        long readableUpperBound,
        long activeTailViewVersion,
        BindingReadRouteTableV1 routes,
        Object protocolStateReference) {
    public BindingReadPublicationCellV1 {
        Objects.requireNonNull(routes, "routes");
        Objects.requireNonNull(protocolStateReference, "protocolStateReference");
        if (sourceGeneration <= 0 || readableUpperBound < 0 || activeTailViewVersion < 0) {
            throw new IllegalArgumentException("publication cell generation/frontier is outside its domain");
        }
    }
}
