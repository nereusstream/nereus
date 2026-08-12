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
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.metadata.oxia.v2.OperationAdmission;
import com.nereusstream.metadata.oxia.v2.codec.SelectorAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.ExactRecordResolver;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.capability.PulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Single-key selector read/create/CAS scaffold without transition or ownership policy. */
public final class OxiaPulsarTopicGenerationSelectorStore implements PulsarTopicGenerationSelectorStore {
    private final OperationAdmission admission;
    private final OxiaV2AuthorityKeys keys;
    private final SelectorAuthorityCodec codec;
    private final OxiaConditionalClient client;
    private final ConditionalMutationEngine mutationEngine;

    public OxiaPulsarTopicGenerationSelectorStore(
            OperationAdmission admission,
            OxiaV2AuthorityKeys keys,
            SelectorAuthorityCodec codec,
            OxiaConditionalClient client,
            ConditionalMutationEngine mutationEngine) {
        this.admission = Objects.requireNonNull(admission, "admission");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.client = Objects.requireNonNull(client, "client");
        this.mutationEngine = Objects.requireNonNull(mutationEngine, "mutationEngine");
    }

    @Override
    public CompletionStage<Optional<VersionedSelectorSnapshot>> readSelector(PulsarPersistenceName persistenceName) {
        return AdapterFutures.localValidation(() -> {
            prepare();
            String key = keys.selectorKey(persistenceName);
            return client.read(key).thenApply(record -> record.map(value -> decode(key, persistenceName, value)));
        });
    }

    @Override
    public CompletionStage<CreateMutationResult<VersionedSelectorSnapshot>> createSelector(
            PulsarTopicGenerationSelectorValueV1 candidate) {
        return AdapterFutures.localValidation(() -> {
            prepare();
            Objects.requireNonNull(candidate, "candidate");
            String key = keys.selectorKey(candidate.persistenceName());
            CanonicalBytes encoded = codec.encode(candidate);
            requireExactCandidateBytes(encoded, candidate);
            return mutationEngine.create(key, encoded, resolver(key, candidate, null));
        });
    }

    @Override
    public CompletionStage<ConditionalCasResult<VersionedSelectorSnapshot>> compareAndSetSelector(
            VersionedSelectorSnapshot exactPredecessor, PulsarTopicGenerationSelectorValueV1 candidate) {
        return AdapterFutures.localValidation(() -> {
            prepare();
            Objects.requireNonNull(exactPredecessor, "exactPredecessor");
            Objects.requireNonNull(candidate, "candidate");
            if (!exactPredecessor.value().persistenceName().equals(candidate.persistenceName())) {
                throw new IllegalArgumentException("selector predecessor and candidate authority identities differ");
            }
            String key = keys.selectorKey(candidate.persistenceName());
            CanonicalBytes encoded = codec.encode(candidate);
            requireExactCandidateBytes(encoded, candidate);
            long expectedVersion = MetadataVersionMapper.toOxia(exactPredecessor.metadataVersion());
            return mutationEngine.compareAndSet(
                    key, encoded, expectedVersion, resolver(key, candidate, exactPredecessor));
        });
    }

    private void prepare() {
        admission.requireOpen();
        codec.requireAvailable("selector");
    }

    private ExactRecordResolver<VersionedSelectorSnapshot> resolver(
            String key, PulsarTopicGenerationSelectorValueV1 candidate, VersionedSelectorSnapshot predecessor) {
        return new ExactRecordResolver<>() {
            @Override
            public VersionedSelectorSnapshot decode(AuthorityRecord record) {
                return OxiaPulsarTopicGenerationSelectorStore.this.decode(key, candidate.persistenceName(), record);
            }

            @Override
            public boolean isCandidateExact(VersionedSelectorSnapshot snapshot) {
                return snapshot.value().equals(candidate);
            }

            @Override
            public boolean isPredecessorExact(VersionedSelectorSnapshot snapshot) {
                return predecessor != null && snapshot.equals(predecessor);
            }
        };
    }

    private VersionedSelectorSnapshot decode(
            String key, PulsarPersistenceName persistenceName, AuthorityRecord record) {
        if (!key.equals(record.key())) {
            throw new IllegalArgumentException("selector read returned a different authority key");
        }
        VersionedSelectorSnapshot snapshot = codec.decode(
                key, persistenceName, record.storedBytes(), MetadataVersionMapper.fromOxia(record.versionId()));
        if (!persistenceName.equals(snapshot.value().persistenceName())
                || !key.equals(keys.selectorKey(snapshot.value().persistenceName()))
                || !record.storedBytes().equals(snapshot.value().canonicalStoredBytes())) {
            throw new IllegalArgumentException("selector key, identity, or exact bytes mismatch");
        }
        return snapshot;
    }

    private static void requireExactCandidateBytes(
            CanonicalBytes encoded, PulsarTopicGenerationSelectorValueV1 candidate) {
        if (!encoded.equals(candidate.canonicalStoredBytes())) {
            throw new IllegalArgumentException("selector codec did not preserve exact candidate bytes");
        }
    }
}
