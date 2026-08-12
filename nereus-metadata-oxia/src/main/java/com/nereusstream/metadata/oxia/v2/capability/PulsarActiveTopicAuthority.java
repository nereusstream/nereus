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

package com.nereusstream.metadata.oxia.v2.capability;

import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.util.Objects;

/** Exact ACTIVE selector plus the immutable aggregate it selects. */
public record PulsarActiveTopicAuthority(VersionedSelectorSnapshot selector, VersionedAggregateSnapshot aggregate) {
    public PulsarActiveTopicAuthority {
        Objects.requireNonNull(selector, "selector");
        Objects.requireNonNull(aggregate, "aggregate");
        if (selector.value().state() != PulsarTopicGenerationSelectorStateV1.ACTIVE) {
            throw new IllegalArgumentException("active authority requires an ACTIVE selector");
        }
        if (!selector.value()
                        .aggregateBindingId()
                        .equals(aggregate.aggregate().binding().bindingId())
                || !selector.value().aggregateCanonicalStoredDigest().equals(aggregate.canonicalStoredDigest())) {
            throw new IllegalArgumentException("ACTIVE selector does not select the supplied aggregate");
        }
    }
}
