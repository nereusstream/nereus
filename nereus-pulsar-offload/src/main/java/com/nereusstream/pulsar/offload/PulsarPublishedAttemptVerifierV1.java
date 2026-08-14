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

package com.nereusstream.pulsar.offload;

import com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1.Publication;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.crypto.SecretKey;

/** P3 completion verifier backed by the production P4 Object read handle. */
public final class PulsarPublishedAttemptVerifierV1 implements PulsarSealedLedgerPublisherV1.PublishedAttemptVerifier {
    @FunctionalInterface
    public interface AttemptKeyResolver {
        SecretKey resolve(PulsarSealedLedgerAttemptV1 attempt);
    }

    private final PulsarOffloadObjectStoreV1 objectStore;
    private final PulsarOffloadLimitCandidateV1 limits;
    private final AttemptKeyResolver keyResolver;

    public PulsarPublishedAttemptVerifierV1(
            PulsarOffloadObjectStoreV1 objectStore,
            PulsarOffloadLimitCandidateV1 limits,
            AttemptKeyResolver keyResolver) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.keyResolver = Objects.requireNonNull(keyResolver, "keyResolver");
    }

    @Override
    public CompletionStage<Void> verify(Publication publication) {
        Objects.requireNonNull(publication, "publication");
        SecretKey attemptKey;
        try {
            attemptKey = Objects.requireNonNull(keyResolver.resolve(publication.attempt()), "attemptKey");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return PulsarObjectReadHandleV1.open(objectStore, limits, publication.attempt(), attemptKey)
                .thenCompose(handle -> {
                    CompletableFuture<Void> result = new CompletableFuture<>();
                    if (!handle.root().equals(publication.root())) {
                        handle.close()
                                .whenComplete((ignored, closeFailure) -> result.completeExceptionally(
                                        new IllegalStateException("production reader root differs from publication")));
                        return result;
                    }
                    handle.verifyCompleteLedger().whenComplete((report, verificationFailure) -> handle.close()
                            .whenComplete((ignored, closeFailure) -> {
                                if (verificationFailure != null) {
                                    if (closeFailure != null) {
                                        verificationFailure.addSuppressed(closeFailure);
                                    }
                                    result.completeExceptionally(verificationFailure);
                                } else if (closeFailure != null) {
                                    result.completeExceptionally(closeFailure);
                                } else {
                                    result.complete(null);
                                }
                            }));
                    return result;
                });
    }
}
