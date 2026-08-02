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

package com.nereusstream.kafka.retention;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Publishes durable trim facts to the binding root before advancing the exact local leader view.
 */
public final class KafkaRetentionDurableTrimListener implements KafkaTrimBarrier.DurableTrimListener {
    private static final int MAX_CAS_ATTEMPTS = 32;

    private final KafkaPartitionMetadataStore bindings;
    private final LocalLogStartUpdater localLogStartUpdater;
    private final Clock clock;

    public KafkaRetentionDurableTrimListener(
            KafkaPartitionMetadataStore bindings, LocalLogStartUpdater localLogStartUpdater, Clock clock) {
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.localLogStartUpdater = Objects.requireNonNull(localLogStartUpdater, "localLogStartUpdater");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<Void> onDurableTrim(
            KafkaTrimBarrier.Snapshot revalidated, long durableTrimOffset, KafkaCheckpointReferenceRecord checkpoint) {
        Objects.requireNonNull(revalidated, "revalidated");
        Objects.requireNonNull(checkpoint, "checkpoint");
        if (durableTrimOffset < checkpoint.logStartOffsetAtCheckpoint()
                || durableTrimOffset < revalidated.sourceHead().trimOffset()
                || durableTrimOffset > revalidated.sourceHead().committedEndOffset()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("durable Kafka trim offset is outside the revalidated stream window"));
        }
        return publishObservedTrim(revalidated, durableTrimOffset, checkpoint, 0)
                .thenCompose(published -> {
                    try {
                        return Objects.requireNonNull(
                                localLogStartUpdater.advance(revalidated, durableTrimOffset, published),
                                "Kafka local log-start updater returned a null future");
                    } catch (Throwable failure) {
                        return CompletableFuture.failedFuture(failure);
                    }
                });
    }

    private CompletableFuture<VersionedKafkaPartitionBinding> publishObservedTrim(
            KafkaTrimBarrier.Snapshot revalidated,
            long durableTrimOffset,
            KafkaCheckpointReferenceRecord checkpoint,
            int attempt) {
        if (attempt >= MAX_CAS_ATTEMPTS) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "Kafka durable-trim binding CAS retry budget exhausted"));
        }
        return bindings.get(revalidated.identity().durableId()).thenCompose(optional -> {
            VersionedKafkaPartitionBinding current =
                    optional.orElseThrow(() -> invariant("Kafka binding disappeared while publishing durable trim"));
            validateRoot(revalidated, current, checkpoint);
            if (current.value().observedLogStartOffset() >= durableTrimOffset) {
                return CompletableFuture.completedFuture(current);
            }
            KafkaPartitionBindingRecord root = current.value();
            if (durableTrimOffset > root.observedStableEndOffset()) {
                return CompletableFuture.failedFuture(invariant("durable Kafka trim exceeds the binding stable end"));
            }
            long now = Math.max(clock.millis(), root.updatedAtMillis());
            KafkaPartitionBindingRecord update = KafkaPartitionMetadataTransitions.observe(
                    root,
                    root.observedTopicName(),
                    root.lastAppliedMetadataOffset(),
                    root.observedLeaderId(),
                    root.observedLeaderEpoch(),
                    root.observedBrokerEpoch(),
                    durableTrimOffset,
                    root.observedStableEndOffset(),
                    now);
            return bindings.compareAndSet(current, update)
                    .exceptionallyCompose(failure -> conditionFailure(failure)
                            ? publishObservedTrim(revalidated, durableTrimOffset, checkpoint, attempt + 1)
                            : CompletableFuture.failedFuture(unwrap(failure)));
        });
    }

    private static void validateRoot(
            KafkaTrimBarrier.Snapshot expected,
            VersionedKafkaPartitionBinding current,
            KafkaCheckpointReferenceRecord checkpoint) {
        KafkaPartitionBindingRecord before = expected.binding().value();
        KafkaPartitionBindingRecord after = current.value();
        KafkaCheckpointReferenceRecord rooted = after.checkpointReferences().stream()
                .filter(reference -> reference.objectId().equals(checkpoint.objectId()))
                .findFirst()
                .orElseThrow(() -> invariant("verified Kafka checkpoint disappeared before trim publication"));
        if (!rooted.equals(checkpoint)
                || !before.identity().equals(after.identity())
                || before.incarnation() != after.incarnation()
                || !before.streamId().equals(after.streamId())
                || before.payloadMappingId() != after.payloadMappingId()
                || !before.storageProfile().equals(after.storageProfile())
                || before.observedLeaderId() != after.observedLeaderId()
                || before.observedLeaderEpoch() != after.observedLeaderEpoch()
                || before.observedBrokerEpoch() != after.observedBrokerEpoch()
                || after.lifecycle() != KafkaPartitionLifecycle.ACTIVE) {
            throw invariant("Kafka binding or leader changed before durable trim publication");
        }
    }

    private static boolean conditionFailure(Throwable failure) {
        return unwrap(failure) instanceof KafkaMetadataConditionFailedException;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    @FunctionalInterface
    public interface LocalLogStartUpdater {
        CompletableFuture<Void> advance(
                KafkaTrimBarrier.Snapshot revalidated,
                long durableTrimOffset,
                VersionedKafkaPartitionBinding publishedBinding);
    }
}
