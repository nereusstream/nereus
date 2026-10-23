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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class WalRunKafkaProtocolEmptyClosureV1Test {
    @Test
    void distinctTerminalWireBindsActualPhysicalClosureWithoutInventingCheckpointState() {
        var fixture = fixture();
        var value = WalRunKafkaProtocolEmptyClosureV1.forClosure(fixture.root(), fixture.proof());
        var decoded = WalRunKafkaProtocolEmptyClosureV1.decode(value.encode());
        assertThat(WalRunKafkaProtocolEmptyClosureV1.hasMagic(value.encode())).isTrue();
        assertThat(decoded.root()).isEqualTo(fixture.root());
        assertThat(WalRunControlCodec.sealSha256(decoded.seal()))
                .isEqualTo(fixture.proof().sealSha256());
        assertThat(decoded.headKey()).isEqualTo(WalRunControlKeys.kafkaProtocolCheckpointHeadKey(7, 1));
        assertThat(decoded.encode()).isEqualTo(value.encode());
        assertThat(WalRunKafkaProtocolEmptyClosureV1.hasMagic(WalRunControlCodec.encodeRoot(fixture.root())))
                .isFalse();
    }

    @Test
    void closureWireRejectsUnknownVersionTruncationExpansionAndSubstitutedRoot() {
        var fixture = fixture();
        var raw = WalRunKafkaProtocolEmptyClosureV1.forClosure(fixture.root(), fixture.proof())
                .encode();
        byte[] version = raw.toByteArray();
        ByteBuffer.wrap(version).putInt(4, 2);
        for (byte[] invalid : List.of(
                version,
                Arrays.copyOf(raw.toByteArray(), raw.length() - 1),
                Arrays.copyOf(raw.toByteArray(), raw.length() + 1),
                new byte[WalRunKafkaProtocolEmptyClosureV1.MAX_BYTES + 1])) {
            assertThatThrownBy(() -> WalRunKafkaProtocolEmptyClosureV1.decode(CanonicalBytes.copyOf(invalid)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> WalRunKafkaProtocolEmptyClosureV1.forClosure(
                        ObjectWalControlTestFixtures.root(2, Optional.empty()), fixture.proof()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Fixture fixture() {
        var store = new TestControlMetadataStore();
        var manager = new WalRunLifecycleManager(store);
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var reference = manager.createRoot(WalRunControlKeys.rootKey(7, 1), root);
        var head = WalCheckpointHeadV1.empty(reference.rootSha256(), 1, 1);
        var bytes = WalRunControlCodec.encodeCheckpointHead(head);
        store.putExact(WalRunControlKeys.checkpointHeadKey(7, 1), bytes);
        var seal = new WalRunSealRecord(
                reference,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                Sha256Digest.hash(bytes),
                0,
                0);
        var runtime = new WalRunRuntime(root);
        runtime.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
        runtime.seal();
        var proof = manager.publishSeal(WalRunControlKeys.sealKey(7, 1), seal, runtime);
        return new Fixture(root, proof);
    }

    private record Fixture(WalRunRootRecord root, WalRunTerminalClosureProofV1 proof) {}
}
