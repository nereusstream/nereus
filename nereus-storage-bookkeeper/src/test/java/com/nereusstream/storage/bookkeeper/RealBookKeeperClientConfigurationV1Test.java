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

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.jupiter.api.Test;

class RealBookKeeperClientConfigurationV1Test {
    @Test
    void projectsTheExactV3FrameAndTimeoutConfiguration() throws Exception {
        BookKeeperCapabilitySnapshotV1 capability =
                capability(new BookKeeperTimeoutClassV1(10_000, 5_000, 5_000, 30_000));

        ClientConfiguration configuration =
                RealBookKeeperClientConfigurationV1.from("zk://localhost:2181/ledgers", capability);

        assertThat(configuration.getMetadataServiceUri()).isEqualTo("zk://localhost:2181/ledgers");
        assertThat(configuration.getUseV2WireProtocol()).isFalse();
        assertThat(configuration.getPreserveMdcForTaskExecution()).isFalse();
        assertThat(configuration.getClientConnectTimeoutMillis()).isEqualTo(10_000);
        assertThat(configuration.getAddEntryTimeout()).isEqualTo(5);
        assertThat(configuration.getReadEntryTimeout()).isEqualTo(5);
        assertThat(configuration.getNettyMaxFrameSizeBytes()).isEqualTo(5_242_880);
    }

    @Test
    void rejectsAnAmbiguousMetadataNamespace() {
        assertThatThrownBy(() -> RealBookKeeperClientConfigurationV1.from(
                        "localhost:2181", capability(new BookKeeperTimeoutClassV1(10_000, 5_000, 5_000, 30_000))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("explicit ZooKeeper ledger root");
    }

    @Test
    void rejectsTimeoutsThatBookKeeperWouldSilentlyRound() {
        assertThatThrownBy(() -> RealBookKeeperClientConfigurationV1.from(
                        "zk://localhost:2181/ledgers",
                        capability(new BookKeeperTimeoutClassV1(10_000, 5_001, 5_000, 30_000))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact whole-second");
    }

    private static BookKeeperCapabilitySnapshotV1 capability(BookKeeperTimeoutClassV1 timeouts) {
        int frameLimit = 5_242_880;
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(digest(1)),
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(2),
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(3),
                BookKeeperProtocolModeV1.V3,
                frameLimit,
                frameLimit,
                BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(frameLimit, frameLimit),
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                timeouts,
                "bk-k0-no-auth:v1",
                digest(4));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }
}
