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

import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import java.util.Objects;
import java.util.Optional;

/** M3 production bridge: the immutable locator/native cut becomes visible only through the M2 root CAS. */
public final class KafkaObjectPublicationBridgeV1 {
    private final KafkaObjectBindingKeyV1 binding;
    private final KafkaObjectCompletionTrackerV1 tracker;
    private final KafkaCoherentCommitCoordinatorV1 coordinator;
    private PendingAck pendingAck;

    public KafkaObjectPublicationBridgeV1(
            KafkaObjectBindingKeyV1 binding,
            long startOffset,
            KafkaObjectCompletionTrackerV1 tracker,
            KafkaCoherentCommitCoordinatorV1 coordinator) {
        this.binding = Objects.requireNonNull(binding, "binding");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        KafkaObjectCoherentProtocolSnapshotV1 initial = coordinator.captureObject();
        if (startOffset < 0
                || initial.root().frontiers().durableEndOffset() != startOffset
                || !initial.activeTail().binding().equals(binding)) {
            throw new IllegalArgumentException("Kafka Object bridge differs from the coherent Object root");
        }
    }

    /** Publishes at most once, emits the protocol ACK, then releases the exact ticket. */
    public synchronized Optional<KafkaObjectCoherentProtocolSnapshotV1> publishNext(Runnable protocolAck) {
        Objects.requireNonNull(protocolAck, "protocolAck");
        if (pendingAck != null) {
            KafkaObjectCoherentProtocolSnapshotV1 selected = coordinator.captureObject();
            if (!selected.equals(pendingAck.published)) {
                throw new IllegalStateException("root changed while a Kafka Object ACK remained unresolved");
            }
            protocolAck.run();
            tracker.releaseAfterProtocolAck(pendingAck.completion.assigned(), pendingAck.completion.commitSet());
            KafkaObjectCoherentProtocolSnapshotV1 converged = pendingAck.published;
            pendingAck = null;
            return Optional.of(converged);
        }
        KafkaObjectCoherentProtocolSnapshotV1 before = coordinator.captureObject();
        long durableFrontier = before.root().frontiers().durableEndOffset();
        if (!before.activeTail().binding().equals(binding)
                || before.root().frontiers().readableEndOffset() != durableFrontier) {
            throw new IllegalStateException("coherent Object root is outside the bridge binding frontier");
        }
        Optional<KafkaObjectCompletionTrackerV1.ReadyCompletion> ready = tracker.readyAt(durableFrontier);
        if (ready.isEmpty()) {
            return Optional.empty();
        }
        KafkaObjectCompletionTrackerV1.ReadyCompletion completion = ready.orElseThrow();
        KafkaObjectCoherentProtocolSnapshotV1 published = coordinator.publishObject(completion);
        tracker.rootPublished(completion.assigned(), completion.commitSet());
        pendingAck = new PendingAck(completion, published);
        protocolAck.run();
        tracker.releaseAfterProtocolAck(completion.assigned(), completion.commitSet());
        pendingAck = null;
        return Optional.of(published);
    }

    public synchronized long bindingDurableFrontier() {
        return coordinator.captureObject().root().frontiers().durableEndOffset();
    }

    private record PendingAck(
            KafkaObjectCompletionTrackerV1.ReadyCompletion completion,
            KafkaObjectCoherentProtocolSnapshotV1 published) {}
}
