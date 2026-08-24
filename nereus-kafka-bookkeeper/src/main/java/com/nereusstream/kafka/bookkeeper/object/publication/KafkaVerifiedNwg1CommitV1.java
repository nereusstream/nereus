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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Opaque result of the production NWG1 writer/provider/full-reader/native verifier pipeline. */
public final class KafkaVerifiedNwg1CommitV1 {
    private final KafkaObjectExtentLocatorV1 locator;
    private final Sha256Digest assignedPayloadSha;
    private final int verifiedFrameCount;

    KafkaVerifiedNwg1CommitV1(
            KafkaObjectExtentLocatorV1 locator, Sha256Digest assignedPayloadSha, int verifiedFrameCount) {
        this.locator = Objects.requireNonNull(locator, "locator");
        this.assignedPayloadSha = Objects.requireNonNull(assignedPayloadSha, "assignedPayloadSha");
        if (assignedPayloadSha.isZero() || verifiedFrameCount <= 0) {
            throw new IllegalArgumentException("verified NWG1 commit result is outside its exact domain");
        }
        this.verifiedFrameCount = verifiedFrameCount;
    }

    public KafkaObjectExtentLocatorV1 locator() {
        return locator;
    }

    public Sha256Digest assignedPayloadSha() {
        return assignedPayloadSha;
    }

    public int verifiedFrameCount() {
        return verifiedFrameCount;
    }
}
