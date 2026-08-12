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

package com.nereusstream.metadata.oxia.v2.registry;

import com.nereusstream.domain.registry.PulsarVirtualLedgerRegistryV1;
import java.util.Objects;
import java.util.Optional;

/** Exact domain predecessor/candidate pair protected by one writer-interlock cut. */
public record RegistryMutationRequestV1(
        Optional<PulsarVirtualLedgerRegistryV1> predecessor, PulsarVirtualLedgerRegistryV1 candidate) {
    public RegistryMutationRequestV1 {
        Objects.requireNonNull(predecessor, "predecessor");
        Objects.requireNonNull(candidate, "candidate");
        if (predecessor.isEmpty() != (candidate.registryEpoch() == 1)) {
            throw new IllegalArgumentException("Registry create/CAS shape differs from candidate epoch");
        }
    }
}
