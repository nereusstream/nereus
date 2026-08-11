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

import static com.nereusstream.domain.DomainTestFixtures.kafkaCell;
import static com.nereusstream.domain.DomainTestFixtures.kafkaIncarnation;
import static com.nereusstream.domain.DomainTestFixtures.pulsarCell;
import static com.nereusstream.domain.DomainTestFixtures.pulsarIncarnation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class TopicBindingAggregateFoundationValidatorV1Test {
    @Test
    void directlyValidatesIndependentKafkaAndPulsarAggregateVariants() {
        TopicBindingAggregateV1 kafka = aggregate(
                ProtocolKindV1.KAFKA,
                kafkaCell(),
                kafkaIncarnation(),
                StorageProfileV1.OBJECT_WAL,
                opaqueNonNonePolicy());
        TopicBindingAggregateV1 pulsar = aggregate(
                ProtocolKindV1.PULSAR,
                pulsarCell(),
                pulsarIncarnation(),
                StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                FrameEncodingPolicyValueV1.none());

        TopicBindingAggregateFoundationValidatorV1.validate(kafka);
        TopicBindingAggregateFoundationValidatorV1.validate(pulsar);
        assertThat(kafka.binding()).isSameAs(kafka.binding());
        assertThat(kafka.initialEpoch()).isSameAs(kafka.initialEpoch());
    }

    @Test
    void rejectsSchemaProtocolBindingAndEpochIdentityMismatches() {
        TopicBindingAggregateV1 valid = aggregate(
                ProtocolKindV1.KAFKA,
                kafkaCell(),
                kafkaIncarnation(),
                StorageProfileV1.OBJECT_WAL,
                opaqueNonNonePolicy());
        TopicBindingId wrongBindingId = new TopicBindingId(Sha256Digest.copyOf(new byte[32]));
        StorageEpochId wrongEpochId = new StorageEpochId(Sha256Digest.copyOf(new byte[32]));

        assertInvalid(new TopicBindingAggregateV1(2, valid.binding(), valid.initialEpoch()));
        assertInvalid(new TopicBindingAggregateV1(
                1,
                new TopicBindingV1(
                        ProtocolKindV1.PULSAR,
                        valid.binding().bindingId(),
                        valid.binding().cellIdentity(),
                        valid.binding().incarnationIdentity()),
                valid.initialEpoch()));
        assertInvalid(new TopicBindingAggregateV1(
                1,
                new TopicBindingV1(
                        ProtocolKindV1.KAFKA,
                        wrongBindingId,
                        valid.binding().cellIdentity(),
                        valid.binding().incarnationIdentity()),
                valid.initialEpoch()));
        assertInvalid(new TopicBindingAggregateV1(
                1,
                valid.binding(),
                new InitialStorageEpochV1(
                        wrongEpochId,
                        0,
                        valid.initialEpoch().storageProfile(),
                        valid.initialEpoch().profileOrigin(),
                        valid.initialEpoch().policyCatalogDigest(),
                        valid.initialEpoch().frameEncodingPolicy())));
    }

    @Test
    void rejectsNonZeroOrdinalAndProfilePolicyMismatches() {
        TopicBindingAggregateV1 objectWalNone = aggregate(
                ProtocolKindV1.KAFKA,
                kafkaCell(),
                kafkaIncarnation(),
                StorageProfileV1.OBJECT_WAL,
                FrameEncodingPolicyValueV1.none());
        TopicBindingAggregateV1 bookKeeperNonNone = aggregate(
                ProtocolKindV1.KAFKA,
                kafkaCell(),
                kafkaIncarnation(),
                StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT,
                opaqueNonNonePolicy());
        TopicBindingAggregateV1 valid = aggregate(
                ProtocolKindV1.KAFKA,
                kafkaCell(),
                kafkaIncarnation(),
                StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                FrameEncodingPolicyValueV1.none());
        InitialStorageEpochV1 ordinalOne = new InitialStorageEpochV1(
                valid.initialEpoch().storageEpochId(),
                1,
                valid.initialEpoch().storageProfile(),
                valid.initialEpoch().profileOrigin(),
                valid.initialEpoch().policyCatalogDigest(),
                valid.initialEpoch().frameEncodingPolicy());

        assertInvalid(objectWalNone);
        assertInvalid(bookKeeperNonNone);
        assertInvalid(new TopicBindingAggregateV1(1, valid.binding(), ordinalOne));
    }

    @Test
    void framePolicyChecksOnlyFrozenStructuralShape() {
        assertThat(FrameEncodingPolicyValueV1.none().isNone()).isTrue();
        assertThat(opaqueNonNonePolicy().isNone()).isFalse();
        assertThatThrownBy(() -> new FrameEncodingPolicyValueV1(0, 1, CanonicalBytes.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FrameEncodingPolicyValueV1(1, 0, CanonicalBytes.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FrameEncodingPolicyValueV1(0, 0, CanonicalBytes.copyOf(new byte[] {1})))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FrameEncodingPolicyValueV1(65536, 1, CanonicalBytes.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aggregateApiHasNoDerivedMutableOrCodecSurface() {
        assertThat(Arrays.stream(TopicBindingAggregateV1.class.getMethods()).map(Method::getName))
                .doesNotContain(
                        "positionDomain",
                        "payloadKind",
                        "nativeAuthority",
                        "primaryWal",
                        "attributes",
                        "owner",
                        "state",
                        "encode",
                        "decode");
        assertThatThrownBy(() -> Class.forName("com.nereusstream.domain.codec.TopicBindingAggregateCodecV1"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    private static TopicBindingAggregateV1 aggregate(
            ProtocolKindV1 protocolKind,
            ProtocolCellIdentity cell,
            TopicIncarnationIdentity incarnation,
            StorageProfileV1 profile,
            FrameEncodingPolicyValueV1 framePolicy) {
        TopicBindingId bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        StorageEpochId epochId = DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0);
        TopicBindingV1 binding = new TopicBindingV1(protocolKind, bindingId, cell, incarnation);
        InitialStorageEpochV1 epoch = new InitialStorageEpochV1(
                epochId,
                0,
                profile,
                ProfileOriginV1.TOPIC_EXPLICIT,
                new PolicyCatalogDigest(Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {9, 8, 7}))),
                framePolicy);
        return new TopicBindingAggregateV1(1, binding, epoch);
    }

    private static FrameEncodingPolicyValueV1 opaqueNonNonePolicy() {
        return new FrameEncodingPolicyValueV1(37, 11, CanonicalBytes.copyOf(new byte[] {1, 2, 3}));
    }

    private static void assertInvalid(TopicBindingAggregateV1 aggregate) {
        assertThatThrownBy(() -> TopicBindingAggregateFoundationValidatorV1.validate(aggregate))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
