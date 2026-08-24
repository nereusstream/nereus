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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Head-selected recovery with one bounded authenticated NWG1 replay fallback. */
public final class KafkaObjectCheckpointRecoveryV1 {
    public enum Source {
        NWKCP1,
        BOUNDED_NWG1_SUFFIX_REPLAY
    }

    public record Result(KafkaProtocolCheckpointStateV1 state, Source source, boolean terminalHead) {
        public Result {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(source, "source");
        }
    }

    private final String walRunPrefix;
    private final Sha256Digest rootSha;
    private final com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1 expectedRunBinding;
    private final Nwkcp1BackendV1 backend;
    private final Optional<KafkaNwg1SuffixReplayV1> replay;

    public KafkaObjectCheckpointRecoveryV1(
            String walRunPrefix,
            KafkaNwkcp1WalRunContextV1 walRunContext,
            Nwkcp1BackendV1 backend,
            Optional<KafkaNwg1SuffixReplayV1> replay) {
        this.walRunPrefix = Objects.requireNonNull(walRunPrefix, "walRunPrefix");
        Objects.requireNonNull(walRunContext, "walRunContext");
        this.rootSha = walRunContext.rootSha();
        this.expectedRunBinding = walRunContext.kafkaRunBinding();
        this.backend = Objects.requireNonNull(backend, "backend");
        this.replay = Objects.requireNonNull(replay, "replay");
        if (rootSha.isZero()) {
            throw new IllegalArgumentException("Kafka checkpoint recovery Root SHA is zero");
        }
        Nwkcp1ObjectKeyV1.headKey(walRunPrefix);
    }

    public CompletionStage<Result> recover() {
        backend.chargeControlMetadata(Nwkcp1ConstantsV1.FORMAT_MAX_HEAD_BYTES);
        return backend.readHead(Nwkcp1ObjectKeyV1.headKey(walRunPrefix)).thenCompose(value -> {
            if (value.isEmpty()) {
                return fallback();
            }
            KafkaProtocolCheckpointHeadV1 head;
            try {
                head = KafkaProtocolCheckpointHeadCodecV1.decode(walRunPrefix, rootSha, value.get());
            } catch (RuntimeException failure) {
                return fallback();
            }
            Nwkcp1BackendV1.SelectedObjectToken selectedToken;
            try {
                selectedToken = backend.selectObjectFromHead(
                        Nwkcp1ObjectKeyV1.headKey(walRunPrefix), value.get(), Sha256Digest.hash(value.get()));
            } catch (RuntimeException failure) {
                return fallback();
            }
            return backend.readSelectedObject(selectedToken, true).thenCompose(body -> {
                if (body.isEmpty()) {
                    return fallback();
                }
                try {
                    Nwkcp1ObjectV1 object = Nwkcp1CodecV1.decodeVerified(
                            walRunPrefix,
                            head.checkpointObjectKey(),
                            head.checkpointObjectLength(),
                            head.checkpointObjectDigest(),
                            body.get());
                    if (!object.walRunRootSha().equals(rootSha)
                            || object.rows().size()
                                    != head.coveredThroughVector().size()) {
                        throw new KafkaObjectCheckpointException("NWKCP1 object differs from its selected Head");
                    }
                    List<KafkaCheckpointCoverageV1> actual = object.rows().stream()
                            .map(row -> KafkaCheckpointCoverageV1.from(row.vector()))
                            .toList();
                    if (!actual.equals(head.coveredThroughVector())) {
                        throw new KafkaObjectCheckpointException("NWKCP1 row vector differs from its selected Head");
                    }
                    if (object.rows().size() != 1) {
                        throw new KafkaObjectCheckpointException(
                                "profile-neutral single-partition recovery cannot select a batched checkpoint");
                    }
                    if (!object.rows().get(0).vector().runBinding().equals(expectedRunBinding)) {
                        throw new KafkaObjectCheckpointException(
                                "NWKCP1 row substituted the expected ProviderScope/StorageRun context");
                    }
                    backend.chargeDecoded(1, 0, 1);
                    return CompletableFuture.completedFuture(new Result(
                            object.rows().get(0),
                            Source.NWKCP1,
                            head.state() == KafkaProtocolCheckpointHeadStateV1.TERMINAL));
                } catch (RuntimeException failure) {
                    return fallback();
                }
            });
        });
    }

    private CompletionStage<Result> fallback() {
        try {
            backend.enterFallback();
            KafkaNwg1SuffixReplayV1 productionReplay = replay.orElseThrow(
                    () -> new KafkaObjectCheckpointException("authenticated NWG1 suffix replay is not configured"));
            return CompletableFuture.completedFuture(
                    new Result(productionReplay.replay().state(), Source.BOUNDED_NWG1_SUFFIX_REPLAY, false));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
