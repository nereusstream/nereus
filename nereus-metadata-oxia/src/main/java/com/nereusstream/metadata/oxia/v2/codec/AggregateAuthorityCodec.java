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
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;

/** Narrow internal aggregate codec port; O2 production uses canonical NTA1 v1 only. */
public interface AggregateAuthorityCodec extends AuthorityValueCodec {
    CanonicalBytes encode(AggregatePublicationCandidate candidate);

    VersionedAggregateSnapshot decode(
            String expectedAuthorityKey,
            PulsarTopicIncarnationIdentity expectedIncarnation,
            CanonicalBytes storedBytes,
            MetadataVersion metadataVersion);
}
