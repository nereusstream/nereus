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

import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.metadata.spi.model.VersionedRegistrySnapshot;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Fail-closed admission used by O2/P1 compositions where R1 is not activated. */
public final class UnavailableRegistryMutationAdmission implements RegistryMutationAdmission {
    @Override
    public <T> CompletionStage<T> executeCreate(
            PulsarVirtualLedgerNamespaceRegistryValueV1 candidate, Supplier<CompletionStage<T>> protectedMutation) {
        return CompletableFuture.failedFuture(new IllegalStateException("R1 Registry admission is unavailable"));
    }

    @Override
    public <T> CompletionStage<T> executeCompareAndSet(
            VersionedRegistrySnapshot predecessor,
            PulsarVirtualLedgerNamespaceRegistryValueV1 candidate,
            Supplier<CompletionStage<T>> protectedMutation) {
        return CompletableFuture.failedFuture(new IllegalStateException("R1 Registry admission is unavailable"));
    }
}
