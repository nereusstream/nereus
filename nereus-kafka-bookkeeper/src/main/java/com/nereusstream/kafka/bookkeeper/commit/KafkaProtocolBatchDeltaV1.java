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

package com.nereusstream.kafka.bookkeeper.commit;

import java.util.Objects;
import java.util.Optional;

/** One native-validated RecordBatch delta before Kafka offsets are assigned. */
public record KafkaProtocolBatchDeltaV1(
        long logicalOffsetCount,
        Optional<KafkaBatchDuplicateIdentityV1> duplicateIdentity,
        KafkaTransactionBatchKindV1 transactionKind,
        long transactionalProducerId,
        int coordinatorEpoch) {
    public KafkaProtocolBatchDeltaV1 {
        duplicateIdentity = Objects.requireNonNull(duplicateIdentity, "duplicateIdentity");
        Objects.requireNonNull(transactionKind, "transactionKind");
        if (logicalOffsetCount <= 0 || logicalOffsetCount > 1L << 31) {
            throw new IllegalArgumentException("logical offset count must fit one non-negative Kafka lastOffsetDelta");
        }
        if (transactionKind == KafkaTransactionBatchKindV1.NONE) {
            if (transactionalProducerId != -1 || coordinatorEpoch != -1) {
                throw new IllegalArgumentException("non-transactional batches cannot carry transaction fields");
            }
        } else {
            if (transactionalProducerId < 0 || duplicateIdentity.isEmpty()) {
                throw new IllegalArgumentException("transactional batches require an idempotent producer identity");
            }
            if (duplicateIdentity.orElseThrow().producerId() != transactionalProducerId) {
                throw new IllegalArgumentException("transactional and duplicate producer identities differ");
            }
            boolean marker = transactionKind == KafkaTransactionBatchKindV1.COMMIT_MARKER
                    || transactionKind == KafkaTransactionBatchKindV1.ABORT_MARKER;
            if (marker && coordinatorEpoch < 0 || !marker && coordinatorEpoch != -1) {
                throw new IllegalArgumentException("only transaction markers carry a coordinator epoch");
            }
        }
        duplicateIdentity.ifPresent(identity -> {
            long expectedLast = ((long) identity.baseSequence() + logicalOffsetCount - 1L) % (1L << 31);
            if (identity.lastSequence() != expectedLast) {
                throw new IllegalArgumentException("producer sequence coverage differs from logical offset count");
            }
        });
    }

    public static KafkaProtocolBatchDeltaV1 nonIdempotent(long logicalOffsetCount) {
        return new KafkaProtocolBatchDeltaV1(
                logicalOffsetCount, Optional.empty(), KafkaTransactionBatchKindV1.NONE, -1, -1);
    }
}
