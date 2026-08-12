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

package com.nereusstream.metadata.oxia.v2.mutation;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConditionalMutationEngineTest {
    private static final String KEY = "/authority";
    private static final CanonicalBytes CANDIDATE = O2TestValues.bytes("candidate");
    private static final CanonicalBytes PREDECESSOR = O2TestValues.bytes("predecessor");
    private static final CanonicalBytes DIFFERENT = O2TestValues.bytes("different");

    private DeterministicOxiaConditionalClient client;
    private ConditionalMutationEngine engine;

    @BeforeEach
    void setUp() {
        client = new DeterministicOxiaConditionalClient();
        engine = new ConditionalMutationEngine(client, new MutationFailureClassifier());
    }

    @Test
    void createSuccessIsVerifiedByOneReread() {
        var result =
                engine.create(KEY, CANDIDATE, resolver()).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.CREATED);
        assertThat(result.exactSnapshot()).contains(CANDIDATE);
        assertThat(client.createCount()).isOne();
        assertThat(client.readCount()).isOne();
    }

    @Test
    void existingExactConditionFailureIsClosed() {
        client.seed(KEY, CANDIDATE, 4);

        var result =
                engine.create(KEY, CANDIDATE, resolver()).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.EXISTING_EXACT);
    }

    @Test
    void existingDifferentConditionFailureIsDefinitiveConflict() {
        client.seed(KEY, DIFFERENT, 4);

        var result =
                engine.create(KEY, CANDIDATE, resolver()).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.DEFINITIVE_CONFLICT);
    }

    @Test
    void createResponseLossAfterApplyResolvesExistingExact() {
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        var result =
                engine.create(KEY, CANDIDATE, resolver()).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.EXISTING_EXACT);
        assertThat(client.readCount()).isOne();
    }

    @Test
    void createResponseLossWithoutApplyIsIndeterminate() {
        client.nextMutation(MutationMode.RESPONSE_LOSS_WITHOUT_APPLY);

        var result =
                engine.create(KEY, CANDIDATE, resolver()).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.INDETERMINATE);
    }

    @Test
    void createSuccessWithDifferentRereadIsIndeterminate() {
        client.nextSuccessStores(DIFFERENT);

        var result =
                engine.create(KEY, CANDIDATE, resolver()).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.INDETERMINATE);
    }

    @Test
    void createUnreadableRereadIsIndeterminate() {
        client.failNextRead();

        var result =
                engine.create(KEY, CANDIDATE, resolver()).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.INDETERMINATE);
    }

    @Test
    void casSuccessIsVerifiedByOneReread() {
        client.seed(KEY, PREDECESSOR, 7);

        var result = engine.compareAndSet(KEY, CANDIDATE, 7, resolver())
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
        assertThat(client.casCount()).isOne();
        assertThat(client.readCount()).isOne();
    }

    @Test
    void casResponseLossAfterApplyResolvesAppliedExact() {
        client.seed(KEY, PREDECESSOR, 7);
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        var result = engine.compareAndSet(KEY, CANDIDATE, 7, resolver())
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
    }

    @Test
    void casVersionConflictWithExactPredecessorIsUnchanged() {
        client.seed(KEY, PREDECESSOR, 7);

        var result = engine.compareAndSet(KEY, CANDIDATE, 6, resolver())
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.PREDECESSOR_UNCHANGED);
        assertThat(result.exactSnapshot()).contains(PREDECESSOR);
    }

    @Test
    void casVersionConflictWithDifferentRecordIsDefinitive() {
        client.seed(KEY, DIFFERENT, 8);

        var result = engine.compareAndSet(KEY, CANDIDATE, 7, resolver())
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.DEFINITIVE_CONFLICT);
    }

    @Test
    void casResponseLossWithoutApplyCanProvePredecessorUnchanged() {
        client.seed(KEY, PREDECESSOR, 7);
        client.nextMutation(MutationMode.RESPONSE_LOSS_WITHOUT_APPLY);

        var result = engine.compareAndSet(KEY, CANDIDATE, 7, resolver())
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.PREDECESSOR_UNCHANGED);
    }

    @Test
    void casSuccessWithDifferentRereadIsIndeterminate() {
        client.seed(KEY, PREDECESSOR, 7);
        client.nextSuccessStores(DIFFERENT);

        var result = engine.compareAndSet(KEY, CANDIDATE, 7, resolver())
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.INDETERMINATE);
    }

    @Test
    void malformedDecodeIsIndeterminateAndNeverRepaired() {
        client.seed(KEY, DIFFERENT, 8);
        ExactRecordResolver<CanonicalBytes> malformed = new ExactRecordResolver<>() {
            @Override
            public CanonicalBytes decode(AuthorityRecord record) {
                throw new IllegalArgumentException("malformed");
            }

            @Override
            public boolean isCandidateExact(CanonicalBytes snapshot) {
                return false;
            }

            @Override
            public boolean isPredecessorExact(CanonicalBytes snapshot) {
                return false;
            }
        };

        var result =
                engine.create(KEY, CANDIDATE, malformed).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.INDETERMINATE);
        assertThat(client.stored(KEY))
                .get()
                .extracting(AuthorityRecord::storedBytes)
                .isEqualTo(DIFFERENT);
    }

    private static ExactRecordResolver<CanonicalBytes> resolver() {
        return new ExactRecordResolver<>() {
            @Override
            public CanonicalBytes decode(AuthorityRecord record) {
                return record.storedBytes();
            }

            @Override
            public boolean isCandidateExact(CanonicalBytes snapshot) {
                return CANDIDATE.equals(snapshot);
            }

            @Override
            public boolean isPredecessorExact(CanonicalBytes snapshot) {
                return PREDECESSOR.equals(snapshot);
            }
        };
    }
}
