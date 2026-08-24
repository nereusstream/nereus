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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import java.util.Optional;
import java.util.function.LongSupplier;

public final class ObjectWalControlTestFixtures {
    private ObjectWalControlTestFixtures() {}

    public static WalRunRootRecord root(long epoch, Optional<WalRunPredecessor> predecessor) {
        return new WalRunRootRecord(
                7,
                epoch,
                new Id128(epoch + 1, epoch + 2),
                0,
                new KafkaProtocolCellIdentity(new DeploymentId(new Id128(1, 2)), new KafkaCellId(new Id128(3, 4))),
                new CellProviderScopeId(digest(1)),
                WalRunFormatContractV1.frozen(),
                new Nwg1RootAdmissionCaps(1024 * 1024, 4096, 3824, 16, 100, 100, 4080, 4096, 1024 * 1024, 1024 * 1024),
                new WalRunBounds(100, 1024 * 1024, 60_000, 4),
                new WalCheckpointPolicy(0, 16, 1024 * 1024, 5_000, 16, 8192),
                new ObjectProviderRootConfiguration(
                        ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST,
                        "test-adapter-v1",
                        "canonical-key-v1",
                        "cell-a/wal/run-" + epoch,
                        ProviderProofMode.NONE,
                        0,
                        1024 * 1024,
                        1024 * 1024,
                        4096,
                        1,
                        10_100,
                        digest(2)),
                new RecoveryEnvelopeLimits(
                        5,
                        4,
                        10,
                        102,
                        100_000,
                        0,
                        10_100,
                        10,
                        32L * 1024 * 1024,
                        10_000,
                        10_000,
                        10_000,
                        1024 * 1024,
                        4,
                        10,
                        60_000_000_000L),
                new WrappedRunKeyEnvelope(
                        "fake-kms", "aes-kw-v1", "kms/cell-a", "version-1", CanonicalBytes.copyOf(new byte[] {
                            1, 2, 3, (byte) epoch
                        })),
                predecessor);
    }

    public static WalRunReference reference(String key, WalRunRootRecord root) {
        return new WalRunReference(key, WalRunControlCodec.rootSha256(root), root.shardId(), root.shardRunEpoch());
    }

    /** Keeps the package-local lifecycle bypass confined to common-module tests. */
    public static WalRunObjectSession openIsolatedSession(
            WalRunRootRecord root, C1ObjectProviderSession provider, KmsCellSession kms, LongSupplier nanoTime) {
        return new WalRunObjectSession(root, provider, kms, nanoTime);
    }

    public static Sha256Digest digest(int seed) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return Sha256Digest.copyOf(value);
    }
}
