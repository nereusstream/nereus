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
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.metadata.spi.model.VersionedRegistrySnapshot;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Production R1 admission: exact domain transition, held interlock, immutable evidence, then Registry mutation. */
public final class VerifiedRegistryMutationAdmissionV1 implements RegistryMutationAdmission {
    private final RegistryWriterInterlock writerInterlock;
    private final ImmutableRegistryAdmissionEvidenceStore evidenceStore;

    public VerifiedRegistryMutationAdmissionV1(
            RegistryWriterInterlock writerInterlock,
            OxiaV2AuthorityKeys keys,
            ConditionalMutationEngine mutationEngine) {
        this.writerInterlock = Objects.requireNonNull(writerInterlock, "writerInterlock");
        evidenceStore = new ImmutableRegistryAdmissionEvidenceStore(keys, mutationEngine);
    }

    @Override
    public <T> CompletionStage<T> executeCreate(
            PulsarVirtualLedgerNamespaceRegistryValueV1 candidate, Supplier<CompletionStage<T>> protectedMutation) {
        Objects.requireNonNull(candidate, "candidate");
        return execute(new RegistryMutationRequestV1(Optional.empty(), candidate.domainValue()), protectedMutation);
    }

    @Override
    public <T> CompletionStage<T> executeCompareAndSet(
            VersionedRegistrySnapshot predecessor,
            PulsarVirtualLedgerNamespaceRegistryValueV1 candidate,
            Supplier<CompletionStage<T>> protectedMutation) {
        Objects.requireNonNull(predecessor, "predecessor");
        Objects.requireNonNull(candidate, "candidate");
        PulsarVirtualLedgerRegistryV1 predecessorDomain = predecessor.value().domainValue();
        return execute(
                new RegistryMutationRequestV1(Optional.of(predecessorDomain), candidate.domainValue()),
                protectedMutation);
    }

    private <T> CompletionStage<T> execute(
            RegistryMutationRequestV1 request, Supplier<CompletionStage<T>> protectedMutation) {
        Objects.requireNonNull(protectedMutation, "protectedMutation");
        CompletionStage<T> result;
        try {
            result = writerInterlock.withPermit(request, snapshot -> {
                snapshot.validateFor(request);
                return evidenceStore.ensureExact(snapshot.evidence()).thenCompose(ignored -> invoke(protectedMutation));
            });
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return Objects.requireNonNull(result, "writer-interlock stage");
    }

    private static <T> CompletionStage<T> invoke(Supplier<CompletionStage<T>> operation) {
        try {
            return Objects.requireNonNull(operation.get(), "protected Registry mutation stage");
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
