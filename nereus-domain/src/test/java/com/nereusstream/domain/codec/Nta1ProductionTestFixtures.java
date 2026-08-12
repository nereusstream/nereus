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

package com.nereusstream.domain.codec;

import static com.nereusstream.domain.DomainTestFixtures.kafkaCell;
import static com.nereusstream.domain.DomainTestFixtures.pulsarCell;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyCatalogV1;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

final class Nta1ProductionTestFixtures {
    private Nta1ProductionTestFixtures() {}

    static TopicBindingAggregateV1 kafka(String name, StorageProfileV1 profile) {
        var incarnation = new KafkaTopicIncarnationIdentity(
                new KafkaTopicId(com.nereusstream.domain.DomainTestFixtures.id128("404142434445464748494a4b4c4d4e4f")),
                new KafkaTopicName(name));
        return aggregate(ProtocolKindV1.KAFKA, kafkaCell(), incarnation, profile, requiredPolicy(profile));
    }

    static TopicBindingAggregateV1 pulsar(String tenant, String namespace, String localName, StorageProfileV1 profile) {
        String persistenceName =
                tenant + "/" + namespace + "/persistent/" + URLEncoder.encode(localName, StandardCharsets.UTF_8);
        String topicName = "persistent://" + tenant + "/" + namespace + "/" + localName;
        return pulsar(persistenceName, topicName, profile, requiredPolicy(profile));
    }

    static TopicBindingAggregateV1 pulsar(
            String persistenceName, String topicName, StorageProfileV1 profile, FrameEncodingPolicyValueV1 policy) {
        var incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString(persistenceName),
                PulsarTopicName.fromString(topicName),
                new PulsarBindingGeneration(42));
        return aggregate(ProtocolKindV1.PULSAR, pulsarCell(), incarnation, profile, policy);
    }

    static TopicBindingAggregateV1 aggregate(
            ProtocolKindV1 protocolKind,
            ProtocolCellIdentity cell,
            TopicIncarnationIdentity incarnation,
            StorageProfileV1 profile,
            FrameEncodingPolicyValueV1 policy) {
        TopicBindingId bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        StorageEpochId epochId = DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0);
        return new TopicBindingAggregateV1(
                1,
                new TopicBindingV1(protocolKind, bindingId, cell, incarnation),
                new InitialStorageEpochV1(
                        epochId,
                        0,
                        profile,
                        ProfileOriginV1.TOPIC_EXPLICIT,
                        new PolicyCatalogDigest(Sha256Digest.hash(
                                CanonicalBytes.copyOf("policy-catalog-v1".getBytes(StandardCharsets.US_ASCII)))),
                        policy));
    }

    static TopicBindingAggregateV1 kafkaMinimum() {
        return kafka("a", StorageProfileV1.BOOKKEEPER_WAL_ONLY);
    }

    static TopicBindingAggregateV1 kafkaTypical() {
        return kafka("orders.v1", StorageProfileV1.OBJECT_WAL);
    }

    static TopicBindingAggregateV1 kafkaBoundary() {
        return kafka("k".repeat(KafkaTopicName.MAX_LENGTH), StorageProfileV1.OBJECT_WAL);
    }

    static TopicBindingAggregateV1 pulsarMinimum() {
        return pulsar("a", "b", "c", StorageProfileV1.BOOKKEEPER_WAL_ONLY);
    }

    static TopicBindingAggregateV1 pulsarTypical() {
        return pulsar("tenant", "ns", "orders-\u03b1", StorageProfileV1.OBJECT_WAL);
    }

    static TopicBindingAggregateV1 pulsarBoundary() {
        String prefix = "persistent://t/n/";
        return pulsar("t", "n", "p".repeat(4096 - prefix.length()), StorageProfileV1.OBJECT_WAL);
    }

    static FrameEncodingPolicyValueV1 requiredPolicy(StorageProfileV1 profile) {
        return FrameEncodingPolicyCatalogV1.requiredFor(profile);
    }
}
