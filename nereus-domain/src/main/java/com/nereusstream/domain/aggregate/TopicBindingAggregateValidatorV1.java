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

package com.nereusstream.domain.aggregate;

import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.protocol.PulsarClassicNameV1;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import java.util.Objects;

/** Complete production legality and bound validation for Topic Binding Aggregate v1. */
public final class TopicBindingAggregateValidatorV1 {
    private TopicBindingAggregateValidatorV1() {}

    public static void validate(TopicBindingAggregateV1 aggregate) {
        Objects.requireNonNull(aggregate, "aggregate");
        TopicBindingAggregateFoundationValidatorV1.validate(aggregate);

        FrameEncodingPolicyValueV1 policy = aggregate.initialEpoch().frameEncodingPolicy();
        FrameEncodingPolicyCatalogV1.validate(policy);
        FrameEncodingPolicyValueV1 required = FrameEncodingPolicyCatalogV1.requiredFor(
                aggregate.initialEpoch().storageProfile());
        if (!required.equals(policy)) {
            throw new IllegalArgumentException("storage profile has an illegal NTA1 v1 frame policy");
        }

        if (aggregate.binding().incarnationIdentity() instanceof PulsarTopicIncarnationIdentity pulsar) {
            PulsarClassicNameV1.validate(pulsar);
        }

        int cellBytes = ProtocolCellIdentityCodecV1.encode(aggregate.binding().cellIdentity())
                .length();
        if (cellBytes > Nta1CodecV1.MAX_CELL_BYTES) {
            throw new IllegalArgumentException("NPC1 Cell identity exceeds the NTA1 v1 cap");
        }
        int incarnationBytes = TopicIncarnationIdentityCodecV1.encode(
                        aggregate.binding().incarnationIdentity())
                .length();
        if (incarnationBytes > Nta1CodecV1.MAX_INCARNATION_BYTES) {
            throw new IllegalArgumentException("NTI1 incarnation identity exceeds the NTA1 v1 cap");
        }
        int total = Math.addExact(Nta1CodecV1.FIXED_BYTES, Math.addExact(cellBytes, incarnationBytes));
        if (total > Nta1CodecV1.MAX_NTA1_BYTES) {
            throw new IllegalArgumentException("aggregate exceeds the NTA1 v1 cap");
        }
    }
}
