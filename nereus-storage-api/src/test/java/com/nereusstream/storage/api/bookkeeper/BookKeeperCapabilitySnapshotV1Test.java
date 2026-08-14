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

package com.nereusstream.storage.api.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import org.junit.jupiter.api.Test;

class BookKeeperCapabilitySnapshotV1Test {
    @Test
    void admitsACompleteImmutableCapabilityTuple() {
        BookKeeperCapabilitySnapshotV1 snapshot = validSnapshot();

        assertThat(snapshot.maximumAddPayloadBytes()).isEqualTo(4_000_000);
        assertThat(snapshot.ackQuorumSize()).isEqualTo(2);
        assertThat(snapshot.credentialIdentityVersion()).isEqualTo("bk-credential:v7");
        assertThat(RunLedgerConfigurationV1.from(snapshot, new StorageRunId(new Id128(0, 9)))
                        .configurationDigest())
                .isEqualTo(snapshot.configurationDigest());
    }

    @Test
    void rejectsNonCanonicalSourceIdentity() {
        assertThatThrownBy(() -> copy(validSnapshot(), "ABC", 4_000_000, true, true, true, 3, 3, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("40 lowercase hex");
    }

    @Test
    void rejectsPayloadCapWithoutProtocolHeadroom() {
        assertThatThrownBy(() -> copy(
                        validSnapshot(), validSnapshot().clientSourceCommit(), 5_000_000, true, true, true, 3, 3, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("headroom");
    }

    @Test
    void rejectsMissingExplicitEntryFenceOrRecoveryCapability() {
        BookKeeperCapabilitySnapshotV1 valid = validSnapshot();

        assertThatThrownBy(() -> copy(valid, valid.clientSourceCommit(), 4_000_000, false, true, true, 3, 3, 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> copy(valid, valid.clientSourceCommit(), 4_000_000, true, false, true, 3, 3, 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> copy(valid, valid.clientSourceCommit(), 4_000_000, true, true, false, 3, 3, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInvalidQuorumOrder() {
        BookKeeperCapabilitySnapshotV1 valid = validSnapshot();

        assertThatThrownBy(() -> copy(valid, valid.clientSourceCommit(), 4_000_000, true, true, true, 2, 3, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ack <= write <= ensemble");
    }

    static BookKeeperCapabilitySnapshotV1 validSnapshot() {
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(digest(1)),
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(2),
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(3),
                BookKeeperProtocolModeV1.V3,
                5_000_000,
                5_000_000,
                4_000_000,
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(1_000, 2_000, 2_000, 5_000),
                "bk-credential:v7",
                digest(4));
    }

    static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }

    private static BookKeeperCapabilitySnapshotV1 copy(
            BookKeeperCapabilitySnapshotV1 source,
            String clientCommit,
            int maximumAddPayloadBytes,
            boolean explicitEntryIds,
            boolean fencing,
            boolean recovery,
            int ensemble,
            int write,
            int ack) {
        return new BookKeeperCapabilitySnapshotV1(
                source.providerScopeId(),
                clientCommit,
                source.clientArtifactSha256(),
                source.serverSourceCommit(),
                source.serverImageManifestSha256(),
                source.protocolMode(),
                source.clientFrameLimitBytes(),
                source.serverFrameLimitBytes(),
                maximumAddPayloadBytes,
                explicitEntryIds,
                ensemble,
                write,
                ack,
                source.digestType(),
                fencing,
                recovery,
                source.timeoutClass(),
                source.credentialIdentityVersion(),
                source.configurationDigest());
    }
}
