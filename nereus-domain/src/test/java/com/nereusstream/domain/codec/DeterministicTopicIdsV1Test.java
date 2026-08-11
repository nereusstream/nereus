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
import static com.nereusstream.domain.DomainTestFixtures.kafkaIncarnation;
import static com.nereusstream.domain.DomainTestFixtures.pulsarCell;
import static com.nereusstream.domain.DomainTestFixtures.pulsarIncarnation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import org.junit.jupiter.api.Test;

class DeterministicTopicIdsV1Test {
    private static final String KAFKA_BINDING_PREIMAGE = "4e544231000000264e5043310001000102030405060708090a"
            + "0b0c0d0e0f101112131415161718191a1b1c1d1e1f000000234e5449310001404142434445464748494a4b4c4d4e4f"
            + "000000096f72646572732e7631";
    private static final String PULSAR_BINDING_PREIMAGE = "4e544231000000364e5043310002000102030405060708090a"
            + "0b0c0d0e0f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f0000003c4e5449310002"
            + "0000001d70657273697374656e743a2f2f74656e616e742f6e732f6f7264657273000000096f72646572732dceb1"
            + "000000000000002a";

    @Test
    void freezesLiteralNtb1AndNse1PreimagesAndDigests() {
        TopicBindingId kafkaBinding = DeterministicTopicIdsV1.deriveBindingId(kafkaCell(), kafkaIncarnation());
        TopicBindingId pulsarBinding = DeterministicTopicIdsV1.deriveBindingId(pulsarCell(), pulsarIncarnation());

        assertThat(DeterministicTopicIdsV1.bindingPreimage(kafkaCell(), kafkaIncarnation())
                        .toHex())
                .isEqualTo(KAFKA_BINDING_PREIMAGE);
        assertThat(kafkaBinding.digest().toHex())
                .isEqualTo("317a6885924aad283a66101aef25b6b39f2007d300269f8e4beb7fd1f6380cbc");
        assertThat(DeterministicTopicIdsV1.storageEpochPreimage(kafkaBinding, 0).toHex())
                .isEqualTo("4e534531317a6885924aad283a66101aef25b6b39f2007d300269f8e4beb7fd1f6380cbc0000000000000000");
        assertThat(DeterministicTopicIdsV1.deriveStorageEpochId(kafkaBinding, 0)
                        .digest()
                        .toHex())
                .isEqualTo("c165fc9f0aecf6e5596ad56c3718f5811506f2a272662885f2d053a5be0bfda9");

        assertThat(DeterministicTopicIdsV1.bindingPreimage(pulsarCell(), pulsarIncarnation())
                        .toHex())
                .isEqualTo(PULSAR_BINDING_PREIMAGE);
        assertThat(pulsarBinding.digest().toHex())
                .isEqualTo("f0d69b7a2478702e8d36785adcd16ccaa9410d55b2c8aabdbdcfab74b565b289");
        assertThat(DeterministicTopicIdsV1.deriveStorageEpochId(pulsarBinding, 0)
                        .digest()
                        .toHex())
                .isEqualTo("a606e61cfc3c89c1371477dab10d08284b5aaddbcef8c46962c05a0be2f4a357");
    }

    @Test
    void isRetryDeterministicAndSeparatesEveryIdentityInput() {
        TopicBindingId baseline = DeterministicTopicIdsV1.deriveBindingId(kafkaCell(), kafkaIncarnation());
        KafkaProtocolCellIdentity rebuiltCell = new KafkaProtocolCellIdentity(
                kafkaCell().deploymentId(), new KafkaCellId(new com.nereusstream.domain.identity.Id128(7, 8)));
        KafkaTopicIncarnationIdentity renamed =
                new KafkaTopicIncarnationIdentity(kafkaIncarnation().topicId(), new KafkaTopicName("orders.v2"));

        assertThat(DeterministicTopicIdsV1.deriveBindingId(kafkaCell(), kafkaIncarnation()))
                .isEqualTo(baseline);
        assertThat(DeterministicTopicIdsV1.deriveBindingId(rebuiltCell, kafkaIncarnation()))
                .isNotEqualTo(baseline);
        assertThat(DeterministicTopicIdsV1.deriveBindingId(kafkaCell(), renamed))
                .isNotEqualTo(baseline);
    }

    @Test
    void rejectsProtocolMismatchAndNonZeroM1Ordinal() {
        assertThatThrownBy(() -> DeterministicTopicIdsV1.deriveBindingId(kafkaCell(), pulsarIncarnation()))
                .isInstanceOf(IllegalArgumentException.class);
        TopicBindingId bindingId = DeterministicTopicIdsV1.deriveBindingId(kafkaCell(), kafkaIncarnation());
        assertThatThrownBy(() -> DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
