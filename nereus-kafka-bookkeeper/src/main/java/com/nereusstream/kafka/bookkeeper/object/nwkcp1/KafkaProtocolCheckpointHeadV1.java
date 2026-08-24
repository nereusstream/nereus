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
import java.util.List;
import java.util.Objects;

/** Exact independently-CASed logical selector for one NWKCP1 checkpoint Object. */
public record KafkaProtocolCheckpointHeadV1(
        Sha256Digest walRunRootSha,
        long publisherEpoch,
        KafkaProtocolCheckpointHeadStateV1 state,
        long checkpointOrdinal,
        Sha256Digest predecessorCheckpointDigest,
        String checkpointObjectKey,
        long checkpointObjectLength,
        Sha256Digest checkpointObjectDigest,
        List<KafkaCheckpointCoverageV1> coveredThroughVector) {
    private static final Sha256Digest ZERO = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);

    public KafkaProtocolCheckpointHeadV1 {
        Objects.requireNonNull(walRunRootSha, "walRunRootSha");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(predecessorCheckpointDigest, "predecessorCheckpointDigest");
        Objects.requireNonNull(checkpointObjectKey, "checkpointObjectKey");
        Objects.requireNonNull(checkpointObjectDigest, "checkpointObjectDigest");
        Objects.requireNonNull(coveredThroughVector, "coveredThroughVector");
        coveredThroughVector.forEach(element -> Objects.requireNonNull(element, "coveredThroughVector element"));
        coveredThroughVector = List.copyOf(coveredThroughVector);
        if (walRunRootSha.isZero()
                || publisherEpoch <= 0
                || checkpointOrdinal < 0
                || checkpointOrdinal == Long.MAX_VALUE
                || checkpointObjectLength <= 0
                || checkpointObjectLength > Nwkcp1ConstantsV1.FORMAT_MAX_OBJECT_BYTES
                || checkpointObjectDigest.isZero()
                || coveredThroughVector.isEmpty()
                || coveredThroughVector.size() > Nwkcp1ConstantsV1.FORMAT_MAX_HEAD_VECTOR_ROWS) {
            throw new IllegalArgumentException("Kafka protocol checkpoint Head is outside the v1 domain");
        }
        if (checkpointOrdinal == 0 && !predecessorCheckpointDigest.equals(ZERO)
                || checkpointOrdinal > 0 && predecessorCheckpointDigest.equals(ZERO)) {
            throw new IllegalArgumentException("Kafka protocol checkpoint Head predecessor/ordinal mismatch");
        }
        Nwkcp1ObjectKeyV1.requireCanonicalEmbeddedObjectKey(checkpointObjectKey, checkpointObjectDigest);
        KafkaCheckpointCoverageV1 previous = null;
        for (KafkaCheckpointCoverageV1 coverage : coveredThroughVector) {
            if (previous != null && previous.compareTo(coverage) >= 0) {
                throw new IllegalArgumentException("Kafka protocol checkpoint Head vector is not canonical");
            }
            previous = coverage;
        }
    }

    public static KafkaProtocolCheckpointHeadV1 open(
            Sha256Digest rootSha,
            long publisherEpoch,
            KafkaProtocolCheckpointHeadV1 previous,
            Nwkcp1EncodedObjectV1 object,
            List<KafkaCheckpointCoverageV1> vector) {
        Objects.requireNonNull(object, "object");
        long ordinal = previous == null ? 0 : Math.addExact(previous.checkpointOrdinal, 1);
        Sha256Digest predecessor = previous == null ? ZERO : previous.checkpointObjectDigest;
        KafkaProtocolCheckpointHeadV1 candidate = new KafkaProtocolCheckpointHeadV1(
                rootSha,
                publisherEpoch,
                KafkaProtocolCheckpointHeadStateV1.OPEN,
                ordinal,
                predecessor,
                object.key(),
                object.length(),
                object.digest(),
                vector);
        if (previous != null) {
            candidate.requireLegalSuccessor(previous);
        }
        return candidate;
    }

    public KafkaProtocolCheckpointHeadV1 takeover(long newPublisherEpoch) {
        if (state != KafkaProtocolCheckpointHeadStateV1.OPEN || newPublisherEpoch <= publisherEpoch) {
            throw new IllegalArgumentException("only an OPEN checkpoint Head permits an advancing takeover");
        }
        return new KafkaProtocolCheckpointHeadV1(
                walRunRootSha,
                newPublisherEpoch,
                state,
                checkpointOrdinal,
                predecessorCheckpointDigest,
                checkpointObjectKey,
                checkpointObjectLength,
                checkpointObjectDigest,
                coveredThroughVector);
    }

    public KafkaProtocolCheckpointHeadV1 terminal() {
        if (state != KafkaProtocolCheckpointHeadStateV1.OPEN) {
            throw new IllegalStateException("Kafka protocol checkpoint Head is already terminal");
        }
        return new KafkaProtocolCheckpointHeadV1(
                walRunRootSha,
                publisherEpoch,
                KafkaProtocolCheckpointHeadStateV1.TERMINAL,
                checkpointOrdinal,
                predecessorCheckpointDigest,
                checkpointObjectKey,
                checkpointObjectLength,
                checkpointObjectDigest,
                coveredThroughVector);
    }

    public void requireLegalSuccessor(KafkaProtocolCheckpointHeadV1 previous) {
        Objects.requireNonNull(previous, "previous");
        if (previous.state != KafkaProtocolCheckpointHeadStateV1.OPEN
                || !walRunRootSha.equals(previous.walRunRootSha)
                || publisherEpoch != previous.publisherEpoch
                || checkpointOrdinal != Math.addExact(previous.checkpointOrdinal, 1)
                || !predecessorCheckpointDigest.equals(previous.checkpointObjectDigest)
                || coveredThroughVector.size() != previous.coveredThroughVector.size()) {
            throw new IllegalArgumentException("Kafka protocol checkpoint Head successor is fenced or forked");
        }
        for (int index = 0; index < coveredThroughVector.size(); index++) {
            if (!coveredThroughVector.get(index).doesNotRegress(previous.coveredThroughVector.get(index))) {
                throw new IllegalArgumentException("Kafka protocol checkpoint Head vector regresses or changes shape");
            }
        }
    }
}
