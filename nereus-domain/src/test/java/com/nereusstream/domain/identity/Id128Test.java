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

package com.nereusstream.domain.identity;

import static com.nereusstream.domain.DomainTestFixtures.hex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import org.junit.jupiter.api.Test;

class Id128Test {
    @Test
    void encodesExactlySixteenBigEndianBytes() {
        Id128 identity = new Id128(0x0001020304050607L, 0x08090a0b0c0d0e0fL);

        assertThat(identity.bytes().toByteArray()).containsExactly(hex("000102030405060708090a0b0c0d0e0f"));
        assertThat(Id128.fromBytes(identity.bytes().toByteArray())).isEqualTo(identity);
        assertThatThrownBy(() -> Id128.fromBytes(new byte[15])).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalByteAccessIsDefensive() {
        byte[] source = hex("00010203");
        CanonicalBytes bytes = CanonicalBytes.copyOf(source);
        source[0] = 99;
        byte[] returned = bytes.toByteArray();
        returned[1] = 99;

        assertThat(bytes.toHex()).isEqualTo("00010203");
    }

    @Test
    void typedBootstrapIdentitiesRejectZero() {
        assertThatThrownBy(() -> new DeploymentId(Id128.zero())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReservationDomainId(Id128.zero())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaCellId(Id128.zero())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PulsarCellId(Id128.zero())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void kafkaTopicIdentityRejectsBothReservedUuids() {
        assertThatThrownBy(() -> new KafkaTopicId(Id128.zero())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaTopicId(Id128.one())).isInstanceOf(IllegalArgumentException.class);
        assertThat(new KafkaTopicId(new Id128(0, 2)).value()).isEqualTo(new Id128(0, 2));
    }
}
