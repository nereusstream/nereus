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

package com.nereusstream.kafka.bookkeeper.read;

import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import java.util.Objects;

/** Disposable sequential-read continuation; it owns no provider/source-generation pin. */
public record KafkaBookKeeperReadCursorV1(
        Nbke2RunBindingV1 runIdentity,
        KafkaPartitionFenceV1 capturedFence,
        long sourceGeneration,
        long indexBlockIdentity,
        int locatorOrdinal,
        long nextEntryId,
        long nextKafkaOffset,
        long snapshotStateVersion) {
    public KafkaBookKeeperReadCursorV1 {
        Objects.requireNonNull(runIdentity, "runIdentity");
        Objects.requireNonNull(capturedFence, "capturedFence");
        if (sourceGeneration < 0
                || indexBlockIdentity < -1
                || locatorOrdinal < 0
                || nextEntryId <= 0
                || nextKafkaOffset < 0
                || snapshotStateVersion < 0) {
            throw new IllegalArgumentException("read cursor is outside its domain");
        }
    }
}
