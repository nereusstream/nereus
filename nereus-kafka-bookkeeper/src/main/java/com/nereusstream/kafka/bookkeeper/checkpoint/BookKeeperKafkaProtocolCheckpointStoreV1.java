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

package com.nereusstream.kafka.bookkeeper.checkpoint;

import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** NBKE2 control-entry implementation of the profile-neutral checkpoint store. */
public final class BookKeeperKafkaProtocolCheckpointStoreV1 implements KafkaProtocolCheckpointStoreV1 {
    private final KafkaBookKeeperRunLifecycleV1 lifecycle;
    private KafkaProtocolCheckpointStateV1 published;
    private boolean publicationInFlight;

    public BookKeeperKafkaProtocolCheckpointStoreV1(KafkaBookKeeperRunLifecycleV1 lifecycle) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    }

    @Override
    public CompletionStage<KafkaProtocolCheckpointPublicationV1> publish(KafkaProtocolCheckpointStateV1 checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        synchronized (this) {
            if (publicationInFlight
                    || !checkpoint.vector().isAlignedCompoundCheckpoint()
                    || !checkpoint
                            .vector()
                            .runBinding()
                            .equals(lifecycle.snapshot().runBinding())
                    || checkpoint.vector().recoveryCoveredThrough()
                            < lifecycle.snapshot().root().kafkaStartOffset()
                    || published != null && !checkpoint.vector().doesNotRegress(published.vector())) {
                throw new IllegalArgumentException(
                        "checkpoint is concurrent, unaligned, regressing, or belongs to another run");
            }
            if (published != null && checkpoint.vector().equals(published.vector()) && !checkpoint.equals(published)) {
                throw new IllegalArgumentException("the same checkpoint vector cannot name different protocol state");
            }
            publicationInFlight = true;
        }
        CompletionStage<KafkaProtocolCheckpointPublicationV1> operation;
        try {
            operation = lifecycle
                    .appendProtocolCheckpoint(checkpoint.toNbke2())
                    .thenApply(entryId -> new KafkaProtocolCheckpointPublicationV1(entryId, checkpoint));
        } catch (RuntimeException failure) {
            synchronized (this) {
                publicationInFlight = false;
            }
            throw failure;
        }
        return operation.whenComplete((publication, failure) -> {
            synchronized (BookKeeperKafkaProtocolCheckpointStoreV1.this) {
                publicationInFlight = false;
                if (failure == null) {
                    published = publication.state();
                }
            }
        });
    }

    public synchronized java.util.Optional<KafkaProtocolCheckpointStateV1> latestPublished() {
        return java.util.Optional.ofNullable(published);
    }
}
