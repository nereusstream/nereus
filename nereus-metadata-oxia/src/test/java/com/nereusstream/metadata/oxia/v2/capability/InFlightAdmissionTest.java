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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicAuthorityCodecs;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class InFlightAdmissionTest {
    @Test
    void acceptedMutationMayResolveAfterCloseButNewAdmissionStaysClosed() {
        var candidate = O2TestValues.aggregateCandidate("aggregate");
        var keys = new OxiaV2AuthorityKeys("/nereus/test");
        String key = keys.aggregateKey(candidate.aggregate().binding().incarnationIdentity());
        CompletableFuture<Void> mutation = new CompletableFuture<>();
        AtomicBoolean open = new AtomicBoolean(true);
        OxiaConditionalClient client = new OxiaConditionalClient() {
            @Override
            public CompletionStage<Optional<AuthorityRecord>> read(String ignored) {
                return CompletableFuture.completedFuture(
                        Optional.of(new AuthorityRecord(key, candidate.canonicalStoredBytes(), 0)));
            }

            @Override
            public CompletionStage<Void> createIfAbsent(String ignored, CanonicalBytes storedBytes) {
                return mutation;
            }

            @Override
            public CompletionStage<Void> compareAndSet(
                    String ignored, CanonicalBytes storedBytes, long expectedVersionId) {
                throw new UnsupportedOperationException();
            }
        };
        var codecs = new DeterministicAuthorityCodecs();
        var publisher = new OxiaTopicBindingAggregatePublisher(
                () -> {
                    if (!open.get()) {
                        throw new IllegalStateException("closed");
                    }
                },
                keys,
                codecs.aggregate(),
                new ConditionalMutationEngine(client, new MutationFailureClassifier()));

        var inFlight = publisher.publishIfAbsent(candidate);
        open.set(false);
        mutation.complete(null);

        assertThat(inFlight.toCompletableFuture().join().outcome()).isEqualTo(CreateMutationOutcome.CREATED);
        assertThatThrownBy(() -> publisher
                        .publishIfAbsent(candidate)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("closed");
    }
}
