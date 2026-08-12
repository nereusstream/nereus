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
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import java.util.Objects;

/** Closed semantic validator for NPS1 create and CAS transitions. */
public final class PulsarTopicGenerationTransitionsV1 {
    private PulsarTopicGenerationTransitionsV1() {}

    public static void requireFirstCreate(PulsarTopicGenerationSelectorValueV1 candidate) {
        Objects.requireNonNull(candidate, "candidate");
        if (candidate.state() != PulsarTopicGenerationSelectorStateV1.RESERVED
                || candidate.generation().value() != 1) {
            throw new IllegalArgumentException("first selector create must be RESERVED generation 1");
        }
    }

    public static void requireCas(
            PulsarTopicGenerationSelectorValueV1 predecessor, PulsarTopicGenerationSelectorValueV1 candidate) {
        Objects.requireNonNull(predecessor, "predecessor");
        Objects.requireNonNull(candidate, "candidate");
        if (!predecessor.persistenceName().equals(candidate.persistenceName())) {
            throw new IllegalArgumentException("selector CAS cannot change persistence-name identity");
        }

        if (predecessor.state() == PulsarTopicGenerationSelectorStateV1.DELETED
                && candidate.state() == PulsarTopicGenerationSelectorStateV1.RESERVED) {
            if (candidate.generation().value()
                    != Math.addExact(predecessor.generation().value(), 1)) {
                throw new IllegalArgumentException("selector recreation must use exactly generation + 1");
            }
            return;
        }

        if (!predecessor.generation().equals(candidate.generation())
                || !predecessor.aggregateBindingId().equals(candidate.aggregateBindingId())
                || !predecessor.aggregateCanonicalStoredDigest().equals(candidate.aggregateCanonicalStoredDigest())) {
            throw new IllegalArgumentException("non-recreation selector transition changed immutable identity");
        }
        boolean allowed = predecessor.state() == PulsarTopicGenerationSelectorStateV1.RESERVED
                        && candidate.state() == PulsarTopicGenerationSelectorStateV1.ACTIVE
                || predecessor.state() == PulsarTopicGenerationSelectorStateV1.ACTIVE
                        && candidate.state() == PulsarTopicGenerationSelectorStateV1.DELETING
                || predecessor.state() == PulsarTopicGenerationSelectorStateV1.DELETING
                        && candidate.state() == PulsarTopicGenerationSelectorStateV1.DELETED;
        if (!allowed) {
            throw new IllegalArgumentException(
                    "illegal selector transition " + predecessor.state() + " -> " + candidate.state());
        }
    }
}
