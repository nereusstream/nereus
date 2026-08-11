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

import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import java.util.Objects;

/** Direct object-graph validation for the M1.1a-A aggregate foundation. */
public final class TopicBindingAggregateFoundationValidatorV1 {
    private TopicBindingAggregateFoundationValidatorV1() {}

    public static void validate(TopicBindingAggregateV1 aggregate) {
        Objects.requireNonNull(aggregate, "aggregate");
        if (aggregate.aggregateSchemaVersion() != TopicBindingAggregateV1.SCHEMA_VERSION) {
            throw new IllegalArgumentException("aggregate schema version must be one");
        }

        TopicBindingV1 binding = aggregate.binding();
        if (binding.protocolKind() != binding.cellIdentity().protocolKind()
                || binding.protocolKind() != binding.incarnationIdentity().protocolKind()) {
            throw new IllegalArgumentException("aggregate protocol variants do not agree");
        }

        ProtocolCellIdentityCodecV1.encode(binding.cellIdentity());
        TopicIncarnationIdentityCodecV1.encode(binding.incarnationIdentity());
        TopicBindingId expectedBindingId =
                DeterministicTopicIdsV1.deriveBindingId(binding.cellIdentity(), binding.incarnationIdentity());
        if (!expectedBindingId.equals(binding.bindingId())) {
            throw new IllegalArgumentException("stored Topic Binding ID does not match NTB1 derivation");
        }

        InitialStorageEpochV1 initialEpoch = aggregate.initialEpoch();
        if (initialEpoch.epochOrdinal() != 0) {
            throw new IllegalArgumentException("M1 foundation accepts only initial epoch ordinal zero");
        }
        StorageEpochId expectedEpochId =
                DeterministicTopicIdsV1.deriveStorageEpochId(binding.bindingId(), initialEpoch.epochOrdinal());
        if (!expectedEpochId.equals(initialEpoch.storageEpochId())) {
            throw new IllegalArgumentException("stored Storage Epoch ID does not match NSE1 derivation");
        }

        boolean policyIsNone = initialEpoch.frameEncodingPolicy().isNone();
        switch (initialEpoch.storageProfile()) {
            case OBJECT_WAL -> {
                if (policyIsNone) {
                    throw new IllegalArgumentException("OBJECT_WAL requires a non-NONE frame policy");
                }
            }
            case BOOKKEEPER_WAL_ONLY, BOOKKEEPER_WAL_ASYNC_OBJECT -> {
                if (!policyIsNone) {
                    throw new IllegalArgumentException("BookKeeper profiles require a NONE frame policy");
                }
            }
        }
    }
}
