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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.metadata.oxia.v2.OperationAdmission;
import com.nereusstream.metadata.oxia.v2.codec.RegistryAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.ExactRecordResolver;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.capability.PulsarVirtualLedgerNamespaceRegistryStore;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.metadata.spi.model.VersionedRegistrySnapshot;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Single-key Registry scaffold without allocator, writer capacity, or admission policy. */
public final class OxiaPulsarVirtualLedgerNamespaceRegistryStore implements PulsarVirtualLedgerNamespaceRegistryStore {
    private final OperationAdmission admission;
    private final OxiaV2AuthorityKeys keys;
    private final RegistryAuthorityCodec codec;
    private final OxiaConditionalClient client;
    private final ConditionalMutationEngine mutationEngine;

    public OxiaPulsarVirtualLedgerNamespaceRegistryStore(
            OperationAdmission admission,
            OxiaV2AuthorityKeys keys,
            RegistryAuthorityCodec codec,
            OxiaConditionalClient client,
            ConditionalMutationEngine mutationEngine) {
        this.admission = Objects.requireNonNull(admission, "admission");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.client = Objects.requireNonNull(client, "client");
        this.mutationEngine = Objects.requireNonNull(mutationEngine, "mutationEngine");
    }

    @Override
    public CompletionStage<Optional<VersionedRegistrySnapshot>> readRegistry(
            DeploymentId deploymentId,
            ReservationDomainId reservationDomainId,
            Sha256Digest ledgerIdCompatibilityNamespaceId) {
        return AdapterFutures.localValidation(() -> {
            prepare();
            String key = keys.registryKey(deploymentId, reservationDomainId, ledgerIdCompatibilityNamespaceId);
            return client.read(key)
                    .thenApply(record -> record.map(value ->
                            decode(key, deploymentId, reservationDomainId, ledgerIdCompatibilityNamespaceId, value)));
        });
    }

    @Override
    public CompletionStage<CreateMutationResult<VersionedRegistrySnapshot>> createRegistry(
            PulsarVirtualLedgerNamespaceRegistryValueV1 candidate) {
        return AdapterFutures.localValidation(() -> {
            prepare();
            Objects.requireNonNull(candidate, "candidate");
            String key = key(candidate);
            CanonicalBytes encoded = codec.encode(candidate);
            requireExactCandidateBytes(encoded, candidate);
            return mutationEngine.create(key, encoded, resolver(key, candidate, null));
        });
    }

    @Override
    public CompletionStage<ConditionalCasResult<VersionedRegistrySnapshot>> compareAndSetRegistry(
            VersionedRegistrySnapshot exactPredecessor, PulsarVirtualLedgerNamespaceRegistryValueV1 candidate) {
        return AdapterFutures.localValidation(() -> {
            prepare();
            Objects.requireNonNull(exactPredecessor, "exactPredecessor");
            Objects.requireNonNull(candidate, "candidate");
            if (!sameIdentity(exactPredecessor.value(), candidate)) {
                throw new IllegalArgumentException("Registry predecessor and candidate authority identities differ");
            }
            String key = key(candidate);
            CanonicalBytes encoded = codec.encode(candidate);
            requireExactCandidateBytes(encoded, candidate);
            long expectedVersion = MetadataVersionMapper.toOxia(exactPredecessor.metadataVersion());
            return mutationEngine.compareAndSet(
                    key, encoded, expectedVersion, resolver(key, candidate, exactPredecessor));
        });
    }

    private void prepare() {
        admission.requireOpen();
        codec.requireAvailable("Registry");
    }

    private String key(PulsarVirtualLedgerNamespaceRegistryValueV1 value) {
        return keys.registryKey(
                value.deploymentId(), value.reservationDomainId(), value.ledgerIdCompatibilityNamespaceId());
    }

    private ExactRecordResolver<VersionedRegistrySnapshot> resolver(
            String key, PulsarVirtualLedgerNamespaceRegistryValueV1 candidate, VersionedRegistrySnapshot predecessor) {
        return new ExactRecordResolver<>() {
            @Override
            public VersionedRegistrySnapshot decode(AuthorityRecord record) {
                return OxiaPulsarVirtualLedgerNamespaceRegistryStore.this.decode(
                        key,
                        candidate.deploymentId(),
                        candidate.reservationDomainId(),
                        candidate.ledgerIdCompatibilityNamespaceId(),
                        record);
            }

            @Override
            public boolean isCandidateExact(VersionedRegistrySnapshot snapshot) {
                return snapshot.value().equals(candidate);
            }

            @Override
            public boolean isPredecessorExact(VersionedRegistrySnapshot snapshot) {
                return predecessor != null && snapshot.equals(predecessor);
            }
        };
    }

    private VersionedRegistrySnapshot decode(
            String key,
            DeploymentId deploymentId,
            ReservationDomainId reservationDomainId,
            Sha256Digest namespaceId,
            AuthorityRecord record) {
        if (!key.equals(record.key())) {
            throw new IllegalArgumentException("Registry read returned a different authority key");
        }
        VersionedRegistrySnapshot snapshot = codec.decode(
                key,
                deploymentId,
                reservationDomainId,
                namespaceId,
                record.storedBytes(),
                MetadataVersionMapper.fromOxia(record.versionId()));
        if (!sameIdentity(snapshot.value(), new RegistryIdentity(deploymentId, reservationDomainId, namespaceId))
                || !key.equals(key(snapshot.value()))
                || !record.storedBytes().equals(snapshot.value().canonicalStoredBytes())) {
            throw new IllegalArgumentException("Registry key, identity, or exact bytes mismatch");
        }
        return snapshot;
    }

    private static boolean sameIdentity(
            PulsarVirtualLedgerNamespaceRegistryValueV1 first, PulsarVirtualLedgerNamespaceRegistryValueV1 second) {
        return sameIdentity(
                first,
                new RegistryIdentity(
                        second.deploymentId(),
                        second.reservationDomainId(),
                        second.ledgerIdCompatibilityNamespaceId()));
    }

    private static boolean sameIdentity(PulsarVirtualLedgerNamespaceRegistryValueV1 value, RegistryIdentity identity) {
        return value.deploymentId().equals(identity.deploymentId())
                && value.reservationDomainId().equals(identity.reservationDomainId())
                && value.ledgerIdCompatibilityNamespaceId().equals(identity.namespaceId());
    }

    private static void requireExactCandidateBytes(
            CanonicalBytes encoded, PulsarVirtualLedgerNamespaceRegistryValueV1 candidate) {
        if (!encoded.equals(candidate.canonicalStoredBytes())) {
            throw new IllegalArgumentException("Registry codec did not preserve exact candidate bytes");
        }
    }

    private record RegistryIdentity(
            DeploymentId deploymentId, ReservationDomainId reservationDomainId, Sha256Digest namespaceId) {}
}
