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

import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.IDENTITY;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.binding;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.checkpoint;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.head;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.retentionSnapshot;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.snapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KafkaRetentionDurableTrimListenerTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(20_000), ZoneOffset.UTC);

    @Test
    void publishesBindingFactBeforeAdvancingTheExactLocalLeader() {
        var checkpoint = checkpoint(40);
        var captured = snapshot(binding(checkpoint), head(0), retentionSnapshot(250, 2_500, 20, 5_000));
        AtomicReference<VersionedKafkaPartitionBinding> root = new AtomicReference<>(captured.binding());
        AtomicInteger localUpdates = new AtomicInteger();
        KafkaRetentionDurableTrimListener listener = new KafkaRetentionDurableTrimListener(
                store(root, new AtomicBoolean()),
                (snapshot, offset, published) -> {
                    assertThat(root.get().value().observedLogStartOffset()).isEqualTo(20);
                    assertThat(published).isEqualTo(root.get());
                    localUpdates.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                CLOCK);

        listener.onDurableTrim(captured, 20, checkpoint).join();

        assertThat(root.get().value().observedLogStartOffset()).isEqualTo(20);
        assertThat(root.get().value().observedStableEndOffset()).isEqualTo(40);
        assertThat(localUpdates).hasValue(1);
    }

    @Test
    void reloadsAppliedBindingCasAfterResponseLossAndNotifiesLocalStateOnce() {
        var checkpoint = checkpoint(40);
        var captured = snapshot(binding(checkpoint), head(0), retentionSnapshot(250, 2_500, 20, 5_000));
        AtomicReference<VersionedKafkaPartitionBinding> root = new AtomicReference<>(captured.binding());
        AtomicBoolean loseFirstResponse = new AtomicBoolean(true);
        AtomicInteger localUpdates = new AtomicInteger();
        KafkaRetentionDurableTrimListener listener = new KafkaRetentionDurableTrimListener(
                store(root, loseFirstResponse),
                (snapshot, offset, published) -> {
                    localUpdates.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                CLOCK);

        listener.onDurableTrim(captured, 20, checkpoint).join();

        assertThat(root.get().value().observedLogStartOffset()).isEqualTo(20);
        assertThat(localUpdates).hasValue(1);
    }

    @Test
    void refusesToPublishThroughAChangedLeaderTerm() {
        var checkpoint = checkpoint(40);
        var captured = snapshot(binding(checkpoint), head(0), retentionSnapshot(250, 2_500, 20, 5_000));
        var changedRecord = KafkaPartitionMetadataTransitions.observe(
                captured.binding().value(), IDENTITY.observedTopicName(), 8, 2, 4, 5, 0, 40, 20_000);
        var changed = new VersionedKafkaPartitionBinding(captured.binding().key(), changedRecord, 0, sha256('d'));
        AtomicReference<VersionedKafkaPartitionBinding> root = new AtomicReference<>(changed);
        AtomicInteger localUpdates = new AtomicInteger();
        KafkaRetentionDurableTrimListener listener = new KafkaRetentionDurableTrimListener(
                store(root, new AtomicBoolean()),
                (snapshot, offset, published) -> {
                    localUpdates.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                CLOCK);

        assertThatThrownBy(
                        () -> listener.onDurableTrim(captured, 20, checkpoint).join())
                .hasRootCauseMessage("Kafka binding or leader changed before durable trim publication");
        assertThat(root.get().value().observedLogStartOffset()).isZero();
        assertThat(localUpdates).hasValue(0);
    }

    @Test
    void localFailureLeavesTheDurableBindingFactForAnIdempotentRetry() {
        var checkpoint = checkpoint(40);
        var captured = snapshot(binding(checkpoint), head(0), retentionSnapshot(250, 2_500, 20, 5_000));
        AtomicReference<VersionedKafkaPartitionBinding> root = new AtomicReference<>(captured.binding());
        AtomicInteger localUpdates = new AtomicInteger();
        KafkaRetentionDurableTrimListener listener = new KafkaRetentionDurableTrimListener(
                store(root, new AtomicBoolean()),
                (snapshot, offset, published) -> {
                    localUpdates.incrementAndGet();
                    return CompletableFuture.failedFuture(new IllegalStateException("local leader already resigned"));
                },
                CLOCK);

        assertThatThrownBy(
                        () -> listener.onDurableTrim(captured, 20, checkpoint).join())
                .hasRootCauseMessage("local leader already resigned");
        assertThat(root.get().value().observedLogStartOffset()).isEqualTo(20);
        assertThat(localUpdates).hasValue(1);
    }

    private static KafkaPartitionMetadataStore store(
            AtomicReference<VersionedKafkaPartitionBinding> root, AtomicBoolean loseFirstResponse) {
        return (KafkaPartitionMetadataStore) Proxy.newProxyInstance(
                KafkaPartitionMetadataStore.class.getClassLoader(),
                new Class<?>[] {KafkaPartitionMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "get" -> CompletableFuture.completedFuture(Optional.of(root.get()));
                    case "compareAndSet" -> {
                        VersionedKafkaPartitionBinding expected = (VersionedKafkaPartitionBinding) arguments[0];
                        var update = (com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord) arguments[1];
                        if (root.get() != expected) {
                            yield CompletableFuture.failedFuture(
                                    new KafkaMetadataConditionFailedException("test binding CAS changed"));
                        }
                        long version = expected.metadataVersion() + 1;
                        var applied = new VersionedKafkaPartitionBinding(
                                expected.key(), update.withMetadataVersion(version), version, sha256('e'));
                        root.set(applied);
                        if (loseFirstResponse.compareAndSet(true, false)) {
                            yield CompletableFuture.failedFuture(
                                    new KafkaMetadataConditionFailedException("test binding CAS response lost"));
                        }
                        yield CompletableFuture.completedFuture(applied);
                    }
                    case "close" -> null;
                    case "toString" -> "KafkaRetentionDurableTrimListenerTest.store";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }
}
