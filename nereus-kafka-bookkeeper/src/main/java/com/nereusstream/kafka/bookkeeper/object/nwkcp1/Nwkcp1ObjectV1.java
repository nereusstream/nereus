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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** One bounded canonical NWKCP1 object containing partition-local protocol checkpoint rows. */
public record Nwkcp1ObjectV1(Sha256Digest walRunRootSha, List<KafkaProtocolCheckpointStateV1> rows) {
    public Nwkcp1ObjectV1 {
        Objects.requireNonNull(walRunRootSha, "walRunRootSha");
        rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
        if (walRunRootSha.isZero() || rows.isEmpty() || rows.size() > Nwkcp1ConstantsV1.FORMAT_MAX_ROWS) {
            throw new IllegalArgumentException("NWKCP1 Root or row count is outside the v1 domain");
        }
        Set<PartitionKey> identities = new HashSet<>();
        PartitionKey previous = null;
        for (KafkaProtocolCheckpointStateV1 row : rows) {
            PartitionKey current = PartitionKey.from(row);
            if (!identities.add(current)) {
                throw new IllegalArgumentException("NWKCP1 contains a duplicate partition row");
            }
            if (previous != null && previous.compareTo(current) >= 0) {
                throw new IllegalArgumentException("NWKCP1 partition rows are not in canonical order");
            }
            previous = current;
        }
    }

    private record PartitionKey(String bindingSha, String topicId, int partition) implements Comparable<PartitionKey> {
        static PartitionKey from(KafkaProtocolCheckpointStateV1 state) {
            var binding = state.vector().runBinding();
            return new PartitionKey(
                    binding.bindingId().digest().toHex(),
                    binding.topicIncarnation().topicId().value().toHex(),
                    binding.partitionId());
        }

        @Override
        public int compareTo(PartitionKey other) {
            int result = bindingSha.compareTo(other.bindingSha);
            if (result == 0) {
                result = topicId.compareTo(other.topicId);
            }
            return result == 0 ? Integer.compare(partition, other.partition) : result;
        }
    }
}
