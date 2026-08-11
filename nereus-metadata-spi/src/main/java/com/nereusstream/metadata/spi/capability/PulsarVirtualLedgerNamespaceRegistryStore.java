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

package com.nereusstream.metadata.spi.capability;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.metadata.spi.model.VersionedRegistrySnapshot;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Owns exact create/read/CAS operations for one virtual-ledger compatibility namespace Registry. */
public interface PulsarVirtualLedgerNamespaceRegistryStore {
    CompletionStage<Optional<VersionedRegistrySnapshot>> readRegistry(
            DeploymentId deploymentId,
            ReservationDomainId reservationDomainId,
            Sha256Digest ledgerIdCompatibilityNamespaceId);

    CompletionStage<CreateMutationResult<VersionedRegistrySnapshot>> createRegistry(
            PulsarVirtualLedgerNamespaceRegistryValueV1 candidate);

    CompletionStage<ConditionalCasResult<VersionedRegistrySnapshot>> compareAndSetRegistry(
            VersionedRegistrySnapshot exactPredecessor, PulsarVirtualLedgerNamespaceRegistryValueV1 candidate);
}
