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
import com.nereusstream.storage.object.control.LaneSequenceVector;
import java.util.Arrays;
import java.util.Objects;

/** Root-scoped lane physical-resolution vector, deliberately not a Kafka offset frontier. */
public final class KafkaObjectPhysicalFrontiersV1 {
    private final Sha256Digest rootSha;
    private final long[] resolvedThrough = {-1, -1, -1};

    public KafkaObjectPhysicalFrontiersV1(Sha256Digest rootSha) {
        this(rootSha, LaneSequenceVector.empty());
    }

    /** Restores only an authenticated physical checkpoint vector for the exact same WalRun Root. */
    public KafkaObjectPhysicalFrontiersV1(Sha256Digest rootSha, LaneSequenceVector recovered) {
        this.rootSha = Objects.requireNonNull(rootSha, "rootSha");
        Objects.requireNonNull(recovered, "recovered");
        if (rootSha.isZero()) {
            throw new IllegalArgumentException("physical frontier Root SHA is zero");
        }
        long[] vector = recovered.toArray();
        if (vector.length != resolvedThrough.length) {
            throw new IllegalArgumentException("physical recovery vector differs from the three-lane contract");
        }
        System.arraycopy(vector, 0, resolvedThrough, 0, resolvedThrough.length);
    }

    public synchronized void resolve(KafkaObjectExtentIdentityV1 extent) {
        requireNext(extent);
        resolvedThrough[extent.laneId()] = extent.laneSequence();
    }

    /** No-effect preflight used before the common WalRun runtime commits the same physical resolution. */
    public synchronized void requireNext(KafkaObjectExtentIdentityV1 extent) {
        Objects.requireNonNull(extent, "extent");
        if (!rootSha.equals(extent.walRunRootSha())) {
            throw new IllegalArgumentException("physical extent belongs to another Root");
        }
        int lane = extent.laneId();
        long expected = Math.addExact(resolvedThrough[lane], 1);
        if (extent.laneSequence() != expected) {
            throw new IllegalStateException("physical lane resolution is not contiguous");
        }
    }

    public synchronized long resolvedThrough(int laneId) {
        if (laneId < 0 || laneId > 2) {
            throw new IllegalArgumentException("lane ID is outside [0,2]");
        }
        return resolvedThrough[laneId];
    }

    public synchronized long[] snapshot() {
        return Arrays.copyOf(resolvedThrough, resolvedThrough.length);
    }

    public synchronized LaneSequenceVector snapshotVector() {
        return LaneSequenceVector.of(resolvedThrough[0], resolvedThrough[1], resolvedThrough[2]);
    }

    public Sha256Digest walRunRootSha() {
        return rootSha;
    }
}
