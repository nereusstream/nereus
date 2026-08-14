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

package com.nereusstream.kafka.bookkeeper.replication;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferencesV1;
import java.util.Objects;

/** Exact producer/transaction/leader/checkpoint content identity for source replacement and apply. */
public record KafkaReplicaProtocolProofV1(
        Sha256Digest producerStateDigest,
        Sha256Digest transactionStateDigest,
        Sha256Digest leaderEpochDigest,
        Sha256Digest checkpointVectorDigest) {
    public KafkaReplicaProtocolProofV1 {
        Objects.requireNonNull(producerStateDigest, "producerStateDigest");
        Objects.requireNonNull(transactionStateDigest, "transactionStateDigest");
        Objects.requireNonNull(leaderEpochDigest, "leaderEpochDigest");
        Objects.requireNonNull(checkpointVectorDigest, "checkpointVectorDigest");
        if (producerStateDigest.isZero()
                || transactionStateDigest.isZero()
                || leaderEpochDigest.isZero()
                || checkpointVectorDigest.isZero()) {
            throw new IllegalArgumentException("replica protocol proof contains a zero content identity");
        }
    }

    public static KafkaReplicaProtocolProofV1 from(KafkaPartitionStateReferencesV1 references) {
        Objects.requireNonNull(references, "references");
        return new KafkaReplicaProtocolProofV1(
                references.committedProducerState().contentDigest(),
                references.transactionIndex().contentDigest(),
                references.leaderEpochIndex().contentDigest(),
                references.checkpointVector().contentDigest());
    }
}
