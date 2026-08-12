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
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import com.nereusstream.metadata.oxia.v2.OperationAdmission;
import com.nereusstream.metadata.oxia.v2.codec.AggregateAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregateReader;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Exact same-authority reader; it accepts no binding-ID index or discovery path. */
public final class OxiaTopicBindingAggregateReader implements TopicBindingAggregateReader {
    private final OperationAdmission admission;
    private final OxiaV2AuthorityKeys keys;
    private final AggregateAuthorityCodec codec;
    private final OxiaConditionalClient client;

    public OxiaTopicBindingAggregateReader(
            OperationAdmission admission,
            OxiaV2AuthorityKeys keys,
            AggregateAuthorityCodec codec,
            OxiaConditionalClient client) {
        this.admission = Objects.requireNonNull(admission, "admission");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public CompletionStage<Optional<VersionedAggregateSnapshot>> readAggregate(
            TopicIncarnationIdentity incarnationIdentity) {
        return AdapterFutures.localValidation(() -> readValidated(incarnationIdentity));
    }

    private CompletionStage<Optional<VersionedAggregateSnapshot>> readValidated(
            TopicIncarnationIdentity incarnationIdentity) {
        admission.requireOpen();
        if (!(Objects.requireNonNull(incarnationIdentity, "incarnationIdentity")
                instanceof PulsarTopicIncarnationIdentity pulsar)) {
            throw new IllegalArgumentException("O2 Oxia aggregate reader accepts only Pulsar incarnations");
        }
        codec.requireAvailable("aggregate");
        String key = keys.aggregateKey(pulsar);
        return client.read(key).thenApply(record -> record.map(value -> decodeAndValidate(key, pulsar, value)));
    }

    private VersionedAggregateSnapshot decodeAndValidate(
            String key, PulsarTopicIncarnationIdentity incarnation, AuthorityRecord record) {
        if (!key.equals(record.key())) {
            throw new IllegalArgumentException("aggregate read returned a different authority key");
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
                || !record.storedBytes().equals(snapshot.canonicalStoredBytes())) {
            throw new IllegalArgumentException(
                    "aggregate authority key, incarnation, rederived binding ID, or stored bytes mismatch");
        }
        return snapshot;
    }
}
