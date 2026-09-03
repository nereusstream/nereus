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

import com.nereusstream.domain.aggregate.TopicBindingAggregateValidatorV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.oxia.v2.OperationAdmission;
import com.nereusstream.metadata.oxia.v2.codec.AggregateAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.ExactRecordResolver;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregatePublisher;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Single-key Oxia scaffold for immutable Pulsar aggregate publication. */
public final class OxiaTopicBindingAggregatePublisher implements TopicBindingAggregatePublisher {
    private final OperationAdmission admission;
    private final OxiaV2AuthorityKeys keys;
    private final AggregateAuthorityCodec codec;
    private final ConditionalMutationEngine mutationEngine;

    public OxiaTopicBindingAggregatePublisher(
            OperationAdmission admission,
            OxiaV2AuthorityKeys keys,
            AggregateAuthorityCodec codec,
            ConditionalMutationEngine mutationEngine) {
        this.admission = Objects.requireNonNull(admission, "admission");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.mutationEngine = Objects.requireNonNull(mutationEngine, "mutationEngine");
    }

    @Override
    public CompletionStage<CreateMutationResult<VersionedAggregateSnapshot>> publishIfAbsent(
            AggregatePublicationCandidate candidate) {
        return AdapterFutures.localValidation(() -> publishValidated(candidate));
    }

    private CompletionStage<CreateMutationResult<VersionedAggregateSnapshot>> publishValidated(
            AggregatePublicationCandidate candidate) {
        admission.requireOpen();
        Objects.requireNonNull(candidate, "candidate");
        TopicBindingAggregateValidatorV1.validate(candidate.aggregate());
        if (!(candidate.aggregate().binding().incarnationIdentity()
                instanceof PulsarTopicIncarnationIdentity incarnation)) {
            throw new IllegalArgumentException("O2 Oxia aggregate publisher accepts only Pulsar incarnations");
        }
        codec.requireAvailable("aggregate");
        String key = keys.aggregateKey(incarnation);
        CanonicalBytes encoded = codec.encode(candidate);
        if (!encoded.equals(candidate.canonicalStoredBytes())) {
            throw new IllegalArgumentException("aggregate codec did not preserve exact candidate bytes");
        }
        return mutationEngine.create(key, encoded, resolver(key, incarnation, candidate));
    }

    private ExactRecordResolver<VersionedAggregateSnapshot> resolver(
            String key, PulsarTopicIncarnationIdentity incarnation, AggregatePublicationCandidate candidate) {
        return new ExactRecordResolver<>() {
            @Override
            public VersionedAggregateSnapshot decode(AuthorityRecord record) {
                return decodeAndValidate(key, incarnation, record);
            }

            @Override
            public boolean isCandidateExact(VersionedAggregateSnapshot snapshot) {
                return snapshot.aggregate().equals(candidate.aggregate())
                        && snapshot.canonicalStoredBytes().equals(candidate.canonicalStoredBytes())
                        && snapshot.canonicalStoredDigest().equals(candidate.canonicalStoredDigest());
            }

            @Override
            public boolean isPredecessorExact(VersionedAggregateSnapshot snapshot) {
                return false;
            }
        };
    }

    private VersionedAggregateSnapshot decodeAndValidate(
            String key, PulsarTopicIncarnationIdentity incarnation, AuthorityRecord record) {
        if (!key.equals(record.key())) {
            throw new IllegalArgumentException("aggregate reread returned a different authority key");
        }
        VersionedAggregateSnapshot snapshot = codec.decode(
                key, incarnation, record.storedBytes(), MetadataVersionMapper.fromOxia(record.versionId()));
        TopicBindingAggregateValidatorV1.validate(snapshot.aggregate());
        if (!incarnation.equals(snapshot.binding().incarnationIdentity())
                || !key.equals(keys.aggregateKey(snapshot.binding().incarnationIdentity()))
                || !snapshot.binding()
                        .bindingId()
                        .equals(DeterministicTopicIdsV1.deriveBindingId(
                                snapshot.binding().cellIdentity(), incarnation))
                || !codec.projectStoredBytes(record.storedBytes()).equals(snapshot.canonicalStoredBytes())) {
            throw new IllegalArgumentException(
                    "aggregate authority key, incarnation, rederived binding ID, or stored bytes mismatch");
        }
        return snapshot;
    }
}
