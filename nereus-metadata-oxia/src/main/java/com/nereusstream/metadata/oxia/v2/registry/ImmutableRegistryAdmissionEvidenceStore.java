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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.registry.RegistryAdmissionEvidenceV1;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.ExactRecordResolver;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Internal create-only content-addressed RAE1 store; it is not a fifth metadata SPI. */
final class ImmutableRegistryAdmissionEvidenceStore {
    private final OxiaV2AuthorityKeys keys;
    private final ConditionalMutationEngine mutationEngine;

    ImmutableRegistryAdmissionEvidenceStore(OxiaV2AuthorityKeys keys, ConditionalMutationEngine mutationEngine) {
        this.keys = Objects.requireNonNull(keys, "keys");
        this.mutationEngine = Objects.requireNonNull(mutationEngine, "mutationEngine");
    }

    CompletionStage<Void> ensureExact(RegistryAdmissionEvidenceV1 evidence) {
        Objects.requireNonNull(evidence, "evidence");
        CanonicalBytes bytes = evidence.canonicalBytes();
        String key = keys.registryAdmissionEvidenceKey(evidence.reference().digest());
        ExactRecordResolver<CanonicalBytes> resolver = new ExactRecordResolver<>() {
            @Override
            public CanonicalBytes decode(AuthorityRecord record) {
                if (!key.equals(record.key())) {
                    throw new IllegalArgumentException("RAE1 reread returned a different content-addressed key");
                }
                return record.storedBytes();
            }

            @Override
            public boolean isCandidateExact(CanonicalBytes snapshot) {
                return bytes.equals(snapshot);
            }

            @Override
            public boolean isPredecessorExact(CanonicalBytes snapshot) {
                return false;
            }
        };
        return mutationEngine.create(key, bytes, resolver).thenCompose(result -> {
            if (result.outcome() == CreateMutationOutcome.CREATED
                    || result.outcome() == CreateMutationOutcome.EXISTING_EXACT) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "RAE1 content-addressed create did not converge exactly: " + result.outcome()));
        });
    }
}
