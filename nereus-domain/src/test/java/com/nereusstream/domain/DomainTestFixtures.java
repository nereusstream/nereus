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

package com.nereusstream.domain;

import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import java.util.HexFormat;

/** Shared literal values for domain golden-vector tests. */
public final class DomainTestFixtures {
    public static final String KAFKA_NPC1_HEX =
            "4e5043310001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    public static final String PULSAR_NPC1_HEX = "4e5043310002000102030405060708090a0b0c0d0e0f"
            + "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f";
    public static final String KAFKA_NTI1_HEX =
            "4e5449310001404142434445464748494a4b4c4d4e4f000000096f72646572732e7631";
    public static final String PULSAR_NTI1_HEX = "4e54493100020000001d"
            + "70657273697374656e743a2f2f74656e616e742f6e732f6f7264657273"
            + "000000096f72646572732dceb1000000000000002a";

    private DomainTestFixtures() {}

    public static byte[] hex(String value) {
        return HexFormat.of().parseHex(value);
    }

    public static Id128 id128(String value) {
        return Id128.fromBytes(hex(value));
    }

    public static KafkaProtocolCellIdentity kafkaCell() {
        return new KafkaProtocolCellIdentity(
                new DeploymentId(id128("000102030405060708090a0b0c0d0e0f")),
                new KafkaCellId(id128("101112131415161718191a1b1c1d1e1f")));
    }

    public static PulsarProtocolCellIdentity pulsarCell() {
        return new PulsarProtocolCellIdentity(
                new DeploymentId(id128("000102030405060708090a0b0c0d0e0f")),
                new ReservationDomainId(id128("202122232425262728292a2b2c2d2e2f")),
                new PulsarCellId(id128("303132333435363738393a3b3c3d3e3f")));
    }

    public static KafkaTopicIncarnationIdentity kafkaIncarnation() {
        return new KafkaTopicIncarnationIdentity(
                new KafkaTopicId(id128("404142434445464748494a4b4c4d4e4f")), new KafkaTopicName("orders.v1"));
    }

    public static PulsarTopicIncarnationIdentity pulsarIncarnation() {
        return new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString("persistent://tenant/ns/orders"),
                PulsarTopicName.fromString("orders-\u03b1"),
                new PulsarBindingGeneration(42));
    }
}
