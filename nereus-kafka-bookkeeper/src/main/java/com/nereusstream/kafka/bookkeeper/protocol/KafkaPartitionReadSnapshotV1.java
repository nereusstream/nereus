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

package com.nereusstream.kafka.bookkeeper.protocol;

/** Allocation-free logical read view implemented by the immutable partition state root itself. */
public interface KafkaPartitionReadSnapshotV1 {
    KafkaPartitionFenceV1 fence();

    long stateVersion();

    KafkaPartitionFrontiersV1 frontiers();

    KafkaPartitionStateReferencesV1 references();

    default long readUpperBound(KafkaReadIsolationV1 isolation) {
        return switch (isolation) {
            case REPLICA -> frontiers().readableEndOffset();
            case READ_UNCOMMITTED -> frontiers().highWatermark();
            case READ_COMMITTED -> frontiers().lastStableOffset();
        };
    }
}
