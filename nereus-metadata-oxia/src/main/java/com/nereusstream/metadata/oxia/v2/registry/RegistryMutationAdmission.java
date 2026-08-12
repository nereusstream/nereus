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
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Wraps one exact Registry mutation in its mandatory writer-interlock/evidence cut. */
public interface RegistryMutationAdmission {
    <T> CompletionStage<T> executeCreate(
            PulsarVirtualLedgerNamespaceRegistryValueV1 candidate, Supplier<CompletionStage<T>> protectedMutation);

    <T> CompletionStage<T> executeCompareAndSet(
            VersionedRegistrySnapshot predecessor,
            PulsarVirtualLedgerNamespaceRegistryValueV1 candidate,
            Supplier<CompletionStage<T>> protectedMutation);
}
