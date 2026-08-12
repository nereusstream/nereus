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

package com.nereusstream.metadata.oxia.v2.codec;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.metadata.spi.model.VersionedRegistrySnapshot;

final class UnavailableRegistryAuthorityCodec implements RegistryAuthorityCodec {
    @Override
    public boolean available() {
        return false;
    }

    @Override
    public CanonicalBytes encode(PulsarVirtualLedgerNamespaceRegistryValueV1 candidate) {
        throw new UnavailableProductionCodecException("Registry");
    }

    @Override
    public VersionedRegistrySnapshot decode(
            String expectedAuthorityKey,
            DeploymentId expectedDeploymentId,
            ReservationDomainId expectedReservationDomainId,
            Sha256Digest expectedNamespaceId,
            CanonicalBytes storedBytes,
            MetadataVersion metadataVersion) {
        throw new UnavailableProductionCodecException("Registry");
    }
}
