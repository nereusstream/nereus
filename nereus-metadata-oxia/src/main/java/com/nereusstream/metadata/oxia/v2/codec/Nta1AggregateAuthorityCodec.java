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

import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import java.util.Objects;

/** Production Oxia aggregate adapter over the domain-owned canonical NTA1 v1 codec. */
public final class Nta1AggregateAuthorityCodec implements AggregateAuthorityCodec {
    @Override
    public boolean available() {
        return true;
    }

    @Override
    public CanonicalBytes encode(AggregatePublicationCandidate candidate) {
        Objects.requireNonNull(candidate, "candidate");
        requireExpectedPulsarIncarnation(candidate.aggregate(), null);
        CanonicalBytes canonical = Nta1CodecV1.encode(candidate.aggregate());
        Sha256Digest digest = Sha256Digest.hash(canonical);
        if (!canonical.equals(candidate.canonicalStoredBytes()) || !digest.equals(candidate.canonicalStoredDigest())) {
            throw new IllegalArgumentException("aggregate candidate bytes/digest are not canonical production NTA1");
        }
        return canonical;
    }

    @Override
    public CanonicalBytes projectStoredBytes(CanonicalBytes storedBytes) {
        return M5PulsarAggregateAuthorityCodecV1.projectAggregate(storedBytes);
    }

    @Override
    public VersionedAggregateSnapshot decode(
            String expectedAuthorityKey,
            PulsarTopicIncarnationIdentity expectedIncarnation,
            CanonicalBytes storedBytes,
            MetadataVersion metadataVersion) {
        if (Objects.requireNonNull(expectedAuthorityKey, "expectedAuthorityKey").isBlank()) {
            throw new IllegalArgumentException("expected aggregate authority key must not be blank");
        }
        Objects.requireNonNull(expectedIncarnation, "expectedIncarnation");
        Objects.requireNonNull(storedBytes, "storedBytes");
        Objects.requireNonNull(metadataVersion, "metadataVersion");

        CanonicalBytes projectedBytes = projectStoredBytes(storedBytes);
        TopicBindingAggregateV1 aggregate = Nta1CodecV1.decode(projectedBytes);
        PulsarTopicIncarnationIdentity decodedIncarnation =
                requireExpectedPulsarIncarnation(aggregate, expectedIncarnation);
        if (!aggregate
                .binding()
                .bindingId()
                .equals(DeterministicTopicIdsV1.deriveBindingId(
                        aggregate.binding().cellIdentity(), decodedIncarnation))) {
            throw new IllegalArgumentException("aggregate binding ID does not match its Pulsar incarnation");
        }
        CanonicalBytes canonical = Nta1CodecV1.encode(aggregate);
        if (!canonical.equals(projectedBytes)) {
            throw new IllegalArgumentException("stored aggregate projection is not canonical NTA1");
        }
        return new VersionedAggregateSnapshot(aggregate, canonical, Sha256Digest.hash(canonical), metadataVersion);
    }

    private static PulsarTopicIncarnationIdentity requireExpectedPulsarIncarnation(
            TopicBindingAggregateV1 aggregate, PulsarTopicIncarnationIdentity expected) {
        if (!(Objects.requireNonNull(aggregate, "aggregate").binding().incarnationIdentity()
                instanceof PulsarTopicIncarnationIdentity pulsar)) {
            throw new IllegalArgumentException("O2 aggregate codec accepts only Pulsar incarnations");
        }
        if (expected != null && !expected.equals(pulsar)) {
            throw new IllegalArgumentException("decoded aggregate incarnation does not match the authority key input");
        }
        return pulsar;
    }
}
