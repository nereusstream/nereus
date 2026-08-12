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

package com.nereusstream.metadata.oxia.v2.codec;

import java.util.Objects;

/** Explicit codec composition; production uses the fail-closed unavailable set in O2. */
public record OxiaV2CodecSet(
        AggregateAuthorityCodec aggregate, SelectorAuthorityCodec selector, RegistryAuthorityCodec registry) {
    public OxiaV2CodecSet {
        Objects.requireNonNull(aggregate, "aggregate");
        Objects.requireNonNull(selector, "selector");
        Objects.requireNonNull(registry, "registry");
    }

    public static OxiaV2CodecSet productionUnavailable() {
        return new OxiaV2CodecSet(
                new UnavailableAggregateAuthorityCodec(),
                new UnavailableSelectorAuthorityCodec(),
                new UnavailableRegistryAuthorityCodec());
    }

    public boolean allAvailable() {
        return aggregate.available() && selector.available() && registry.available();
    }
}
