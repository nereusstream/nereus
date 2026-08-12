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

package com.nereusstream.metadata.oxia.v2.mutation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MetadataVersionMapperTest {
    @ParameterizedTest
    @ValueSource(longs = {0, 1, 255, 256, Long.MAX_VALUE})
    void roundTripsNonNegativeOxiaVersions(long versionId) {
        MetadataVersion version = MetadataVersionMapper.fromOxia(versionId);

        assertThat(version.value().length()).isEqualTo(Long.BYTES);
        assertThat(MetadataVersionMapper.toOxia(version)).isEqualTo(versionId);
    }

    @Test
    void encodingIsEightByteBigEndian() {
        assertThat(MetadataVersionMapper.fromOxia(0x0102030405060708L).value().toHex())
                .isEqualTo("0102030405060708");
    }

    @Test
    void rejectsNegativeBackendVersion() {
        assertThatThrownBy(() -> MetadataVersionMapper.fromOxia(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsWrongLengthMetadataVersion() {
        MetadataVersion shortVersion = new MetadataVersion(CanonicalBytes.copyOf(new byte[7]));

        assertThatThrownBy(() -> MetadataVersionMapper.toOxia(shortVersion))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeEncodedMetadataVersion() {
        MetadataVersion negative = new MetadataVersion(CanonicalBytes.copyOf(
                ByteBuffer.allocate(Long.BYTES).putLong(-1).array()));

        assertThatThrownBy(() -> MetadataVersionMapper.toOxia(negative)).isInstanceOf(IllegalArgumentException.class);
    }
}
