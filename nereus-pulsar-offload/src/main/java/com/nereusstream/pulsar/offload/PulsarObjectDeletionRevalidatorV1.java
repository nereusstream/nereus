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

import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.MetadataSnapshot;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.crypto.SecretKey;

/** Final pre-INTENT Object pair and production-reader revalidation. */
public final class PulsarObjectDeletionRevalidatorV1
        implements PulsarBookKeeperDeletionCoordinatorV1.ObjectRevalidator {
    public record RevalidationInput(PulsarSealedLedgerAttemptV1 attempt, SecretKey attemptKey) {
        public RevalidationInput {
            Objects.requireNonNull(attempt, "attempt");
            Objects.requireNonNull(attemptKey, "attemptKey");
        }
    }

    @FunctionalInterface
    public interface InputResolver {
        RevalidationInput resolve(MetadataSnapshot expected);
    }

    private final PulsarOffloadObjectStoreV1 objectStore;
    private final PulsarOffloadLimitCandidateV1 limits;
    private final InputResolver inputResolver;

    public PulsarObjectDeletionRevalidatorV1(
            PulsarOffloadObjectStoreV1 objectStore, PulsarOffloadLimitCandidateV1 limits, InputResolver inputResolver) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.inputResolver = Objects.requireNonNull(inputResolver, "inputResolver");
    }

    @Override
    public CompletionStage<Void> revalidate(MetadataSnapshot expected) {
        RevalidationInput input;
        try {
            input = Objects.requireNonNull(inputResolver.resolve(expected), "revalidation input");
            if (input.attempt().ledgerId() != expected.ledgerId()
                    || !input.attempt().attemptUuid().equals(expected.attemptUuid())
                    || input.attempt().metadataVersion() != expected.version()) {
                throw new IllegalStateException("revalidation input differs from exact native metadata version");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return PulsarObjectReadHandleV1.open(objectStore, limits, input.attempt(), input.attemptKey())
                .thenCompose(handle -> {
                    CompletableFuture<Void> result = new CompletableFuture<>();
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
