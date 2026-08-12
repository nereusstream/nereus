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

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Deployment admission interlock for all writers sharing one ledger-ID compatibility namespace.
 *
 * <p>The implementation must keep the returned snapshot generation selected and prevent writer-start, principal
 * resurrection, INSTANCEID mutation, or membership changes until {@code protectedMutation} completes.
 */
@FunctionalInterface
public interface RegistryWriterInterlock {
    <T> CompletionStage<T> withPermit(
            RegistryMutationRequestV1 request,
            Function<RegistryInterlockSnapshotV1, CompletionStage<T>> protectedMutation);

    static <T> CompletionStage<T> applyProtected(
            RegistryInterlockSnapshotV1 snapshot,
            Function<RegistryInterlockSnapshotV1, CompletionStage<T>> protectedMutation) {
        return Objects.requireNonNull(protectedMutation, "protectedMutation")
                .apply(Objects.requireNonNull(snapshot, "snapshot"));
    }
}
