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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectStateCodecV1;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectActiveTailStateV1;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Opaque active-tail/source-protection cut derived only by authenticated physical checkpoint replay. */
public final class KafkaObjectRecoveredTailV1 {
    private final Sha256Digest walRunRootSha;
    private final String physicalCheckpointHeadKey;
    private final Sha256Digest physicalCheckpointHeadSha;
    private final KafkaObjectActiveTailStateV1 activeTail;
    private final Sha256Digest sourceProtectionDigest;

    KafkaObjectRecoveredTailV1(
            Sha256Digest walRunRootSha,
            String physicalCheckpointHeadKey,
            Sha256Digest physicalCheckpointHeadSha,
            KafkaObjectActiveTailStateV1 activeTail) {
        this.walRunRootSha = Objects.requireNonNull(walRunRootSha, "walRunRootSha");
        this.physicalCheckpointHeadKey = Objects.requireNonNull(physicalCheckpointHeadKey, "physicalCheckpointHeadKey");
        this.physicalCheckpointHeadSha = Objects.requireNonNull(physicalCheckpointHeadSha, "physicalCheckpointHeadSha");
        this.activeTail = Objects.requireNonNull(activeTail, "activeTail");
        if (walRunRootSha.isZero()
                || physicalCheckpointHeadSha.isZero()
                || physicalCheckpointHeadKey.isEmpty()
                || physicalCheckpointHeadKey.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("recovered Object tail authority is outside its exact domain");
        }
        activeTail.locators().forEach(locator -> {
            if (!locator.extent().walRunRootSha().equals(walRunRootSha)) {
                throw new IllegalArgumentException("recovered Object locator belongs to another WalRun Root");
            }
        });
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.writeBytes("M3-KAFKA-OBJECT-SOURCE-PROTECTION-V1".getBytes(StandardCharsets.UTF_8));
        bytes.writeBytes(walRunRootSha.bytes().toByteArray());
        bytes.writeBytes(physicalCheckpointHeadKey.getBytes(StandardCharsets.UTF_8));
        bytes.writeBytes(physicalCheckpointHeadSha.bytes().toByteArray());
        bytes.writeBytes(KafkaObjectStateCodecV1.activeTail(activeTail).toByteArray());
        this.sourceProtectionDigest = Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
    }

    public Sha256Digest walRunRootSha() {
        return walRunRootSha;
    }

    public KafkaObjectActiveTailStateV1 activeTail() {
        return activeTail;
    }

    public Sha256Digest sourceProtectionDigest() {
        return sourceProtectionDigest;
    }
}
