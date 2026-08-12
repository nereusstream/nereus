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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.registry.RegistryAdmissionEvidenceV1;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.registry.VerifiedRegistryMutationAdmissionV1;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.metadata.oxia.v2.testing.R1TestValues;
import com.nereusstream.metadata.oxia.v2.testing.R1TestValues.DeterministicInterlock;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class R1RegistryAuthorityTest {
    private DeterministicOxiaConditionalClient client;
    private OxiaV2AuthorityKeys keys;
    private DeterministicInterlock interlock;
    private OxiaPulsarVirtualLedgerNamespaceRegistryStore store;

    @BeforeEach
    void setUp() {
        client = new DeterministicOxiaConditionalClient();
        keys = new OxiaV2AuthorityKeys("/nereus/r1-test");
        interlock = new DeterministicInterlock();
        ConditionalMutationEngine engine = new ConditionalMutationEngine(client, new MutationFailureClassifier());
        store = new OxiaPulsarVirtualLedgerNamespaceRegistryStore(
                () -> {},
                keys,
                OxiaV2CodecSet.productionR1().registry(),
                client,
                engine,
                new VerifiedRegistryMutationAdmissionV1(interlock, keys, engine));
    }

    @Test
    void createPersistsEvidenceBeforeExactRegistry() {
        RegistryAdmissionEvidenceV1 evidence = R1TestValues.initialEvidence(2);
        interlock.register(R1TestValues.snapshot(evidence));
        var candidate = R1TestValues.storedValue(evidence, List.of(R1TestValues.assignment(0)));

        var result = store.createRegistry(candidate).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.CREATED);
        assertThat(client.createCount()).isEqualTo(2);
        assertThat(client.stored(
                        keys.registryAdmissionEvidenceKey(evidence.reference().digest())))
                .get()
                .extracting(record -> record.storedBytes())
                .isEqualTo(evidence.canonicalBytes());
        assertThat(client.stored(keys.registryKey(
                        candidate.deploymentId(),
                        candidate.reservationDomainId(),
                        candidate.ledgerIdCompatibilityNamespaceId())))
                .get()
                .extracting(record -> record.storedBytes())
                .isEqualTo(candidate.canonicalStoredBytes());
    }

    @Test
    void registryResponseLossConvergesAppliedExactAfterEvidenceExists() {
        RegistryAdmissionEvidenceV1 initialEvidence = R1TestValues.initialEvidence(2);
        interlock.register(R1TestValues.snapshot(initialEvidence));
        var initial = R1TestValues.storedValue(initialEvidence, List.of());
        var created = store.createRegistry(initial).toCompletableFuture().join();
        var predecessor = created.exactSnapshot().orElseThrow();

        RegistryAdmissionEvidenceV1 successorEvidence =
                R1TestValues.evidence(2, initial.canonicalStoredDigest(), R1TestValues.writers(2), List.of());
        interlock.register(R1TestValues.snapshot(successorEvidence));
        var successor = R1TestValues.storedValue(successorEvidence, List.of(R1TestValues.assignment(0)));
        client.seed(
                keys.registryAdmissionEvidenceKey(successorEvidence.reference().digest()),
                successorEvidence.canonicalBytes(),
                0);
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        var result = store.compareAndSetRegistry(predecessor, successor)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
        assertThat(result.exactSnapshot()
                        .orElseThrow()
                        .sliceView(R1TestValues.assignment(0).pulsarCellId())
                        .canonicalRegistryDigest())
                .isEqualTo(successor.canonicalStoredDigest());
    }

    @Test
    void responseLossWithoutApplyKeepsExactPredecessor() {
        RegistryAdmissionEvidenceV1 initialEvidence = R1TestValues.initialEvidence(2);
        interlock.register(R1TestValues.snapshot(initialEvidence));
        var initial = R1TestValues.storedValue(initialEvidence, List.of());
        var predecessor = store.createRegistry(initial)
                .toCompletableFuture()
                .join()
                .exactSnapshot()
                .orElseThrow();
        RegistryAdmissionEvidenceV1 successorEvidence =
                R1TestValues.evidence(2, initial.canonicalStoredDigest(), R1TestValues.writers(2), List.of());
        interlock.register(R1TestValues.snapshot(successorEvidence));
        var successor = R1TestValues.storedValue(successorEvidence, List.of());
        client.seed(
                keys.registryAdmissionEvidenceKey(successorEvidence.reference().digest()),
                successorEvidence.canonicalBytes(),
                0);
        client.nextMutation(MutationMode.RESPONSE_LOSS_WITHOUT_APPLY);

        var result = store.compareAndSetRegistry(predecessor, successor)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.PREDECESSOR_UNCHANGED);
    }

    @Test
    void incompleteInterlockFailsBeforeAnyOxiaMutation() {
        RegistryAdmissionEvidenceV1 evidence = R1TestValues.initialEvidence(2);
        interlock.register(R1TestValues.snapshot(evidence, true, true, true, false, true));

        assertThatThrownBy(() -> store.createRegistry(R1TestValues.storedValue(evidence, List.of()))
                        .toCompletableFuture()
                        .join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("Registry writer interlock proof is incomplete");
        assertThat(client.createCount()).isZero();
    }

    @Test
    void mismatchedEvidenceFailsBeforeAnyOxiaMutation() {
        RegistryAdmissionEvidenceV1 evidence = R1TestValues.initialEvidence(2);
        RegistryAdmissionEvidenceV1 other = R1TestValues.initialEvidence(4);
        DeterministicInterlock wrong = new DeterministicInterlock() {
            @Override
            public <T> java.util.concurrent.CompletionStage<T> withPermit(
                    com.nereusstream.metadata.oxia.v2.registry.RegistryMutationRequestV1 request,
                    java.util.function.Function<
                                    com.nereusstream.metadata.oxia.v2.registry.RegistryInterlockSnapshotV1,
                                    java.util.concurrent.CompletionStage<T>>
                            protectedMutation) {
                return protectedMutation.apply(R1TestValues.snapshot(other));
            }
        };
        ConditionalMutationEngine engine = new ConditionalMutationEngine(client, new MutationFailureClassifier());
        var mismatchedStore = new OxiaPulsarVirtualLedgerNamespaceRegistryStore(
                () -> {},
                keys,
                OxiaV2CodecSet.productionR1().registry(),
                client,
                engine,
                new VerifiedRegistryMutationAdmissionV1(wrong, keys, engine));

        assertThatThrownBy(() -> mismatchedStore
                        .createRegistry(R1TestValues.storedValue(evidence, List.of()))
                        .toCompletableFuture()
                        .join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(client.createCount()).isZero();
    }
}
