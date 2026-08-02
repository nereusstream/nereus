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

package com.nereusstream.kafka.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.compaction.KafkaCompactionScheduler.Trigger;
import com.nereusstream.kafka.compaction.KafkaCompactionScheduler.TriggerBatch;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaCompactionSchedulerTest {
    @Test
    void runsStartupAndFixedDelayPassWithoutClosingBorrowedScheduler() {
        ManualScheduler borrowed = new ManualScheduler();
        List<TriggerBatch> batches = new ArrayList<>();
        List<CompletableFuture<Void>> sources = new ArrayList<>();
        KafkaCompactionScheduler owner = new KafkaCompactionScheduler(
                triggers -> {
                    batches.add(triggers);
                    CompletableFuture<Void> source = new CompletableFuture<>();
                    sources.add(source);
                    return source;
                },
                Duration.ofSeconds(30),
                borrowed,
                Runnable::run);

        try {
            owner.start().join();
            assertThat(borrowed.lastDelayNanos()).isZero();

            borrowed.fire();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0).reasons()).containsExactly(Trigger.STARTUP);
            sources.get(0).complete(null);

            assertThat(borrowed.lastDelayNanos())
                    .isEqualTo(Duration.ofSeconds(30).toNanos());
            borrowed.fire();
            assertThat(batches).hasSize(2);
            assertThat(batches.get(1).reasons()).containsExactly(Trigger.PERIODIC);
            sources.get(1).complete(null);

            owner.closeAsync().join();
            assertThat(owner.isRunning()).isFalse();
            assertThat(borrowed.isShutdown()).isFalse();
            assertThat(borrowed.current().isCancelled()).isTrue();
        } finally {
            borrowed.shutdownNow();
        }
    }

    @Test
    void coalescesTriggersBehindActivePassAndIsolatesCallerCancellation() {
        ManualScheduler borrowed = new ManualScheduler();
        List<TriggerBatch> batches = new ArrayList<>();
        List<CompletableFuture<Void>> sources = new ArrayList<>();
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger maximumConcurrent = new AtomicInteger();
        KafkaCompactionScheduler owner = new KafkaCompactionScheduler(
                triggers -> {
                    batches.add(triggers);
                    maximumConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
                    CompletableFuture<Void> source = new CompletableFuture<>();
                    sources.add(source);
                    return source;
                },
                Duration.ofMinutes(1),
                borrowed,
                Runnable::run);

        try {
            owner.start().join();
            borrowed.fire();

            CompletableFuture<Void> cancelled = owner.trigger(Trigger.DIRTY_BYTES);
            CompletableFuture<Void> survivor = owner.trigger(Trigger.POLICY_CHANGE);
            assertThat(cancelled.cancel(false)).isTrue();
            assertThat(sources).hasSize(1);

            concurrent.decrementAndGet();
            sources.get(0).complete(null);
            assertThat(batches).hasSize(2);
            assertThat(batches.get(1).reasons()).containsExactlyInAnyOrder(Trigger.DIRTY_BYTES, Trigger.POLICY_CHANGE);
            assertThat(batches.get(1).primary()).isEqualTo(Trigger.POLICY_CHANGE);
            assertThat(survivor).isNotDone();

            concurrent.decrementAndGet();
            sources.get(1).complete(null);
            survivor.join();
            assertThat(cancelled).isCancelled();
            assertThat(maximumConcurrent).hasValue(1);
            owner.closeAsync().join();
        } finally {
            borrowed.shutdownNow();
        }
    }

    @Test
    void closeCancelsActiveSourceAndFailsPendingPassWithoutClosingBorrowedResources() {
        ManualScheduler borrowed = new ManualScheduler();
        CompletableFuture<Void> activeSource = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        KafkaCompactionScheduler owner = new KafkaCompactionScheduler(
                triggers -> {
                    calls.incrementAndGet();
                    return activeSource;
                },
                Duration.ofMinutes(1),
                borrowed,
                Runnable::run);

        try {
            owner.start().join();
            borrowed.fire();
            CompletableFuture<Void> pending = owner.trigger(Trigger.ADMIN);

            owner.closeAsync().join();

            assertThat(activeSource).isCancelled();
            assertThat(calls).hasValue(1);
            assertThatThrownBy(pending::join)
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseInstanceOf(NereusException.class)
                    .rootCause()
                    .extracting(value -> ((NereusException) value).code())
                    .isEqualTo(ErrorCode.STORAGE_CLOSED);
            assertThat(borrowed.isShutdown()).isFalse();
        } finally {
            borrowed.shutdownNow();
        }
    }

    @Test
    void rejectsTriggerBeforeStartAndRestartAfterClose() {
        ManualScheduler borrowed = new ManualScheduler();
        AtomicInteger calls = new AtomicInteger();
        KafkaCompactionScheduler owner = new KafkaCompactionScheduler(
                triggers -> {
                    calls.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                Duration.ofSeconds(1),
                borrowed,
                Runnable::run);

        try {
            assertCode(owner.trigger(Trigger.ADMIN), ErrorCode.METADATA_CONDITION_FAILED);
            owner.start().join();
            owner.closeAsync().join();
            assertThat(calls).hasValue(0);
            assertThat(borrowed.current().isCancelled()).isTrue();
            assertCode(owner.start(), ErrorCode.STORAGE_CLOSED);
        } finally {
            borrowed.shutdownNow();
        }
    }

    private static void assertCode(CompletableFuture<?> future, ErrorCode expected) {
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(expected);
    }

    private static final class ManualScheduler extends ScheduledThreadPoolExecutor {
        private ManualScheduledFuture current;
        private long lastDelayNanos = -1;

        private ManualScheduler() {
            super(1);
        }

        @Override
        public synchronized ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            if (current != null && !current.isDone()) {
                throw new AssertionError("only one Kafka compaction deadline may be scheduled");
            }
            lastDelayNanos = unit.toNanos(delay);
            current = new ManualScheduledFuture(command);
            return current;
        }

        private synchronized void fire() {
            if (current == null || current.isDone()) {
                throw new AssertionError("Kafka compaction deadline was not scheduled");
            }
            current.run();
        }

        private synchronized long lastDelayNanos() {
            return lastDelayNanos;
        }

        private synchronized ManualScheduledFuture current() {
            return current;
        }
    }

    private static final class ManualScheduledFuture extends FutureTask<Void> implements ScheduledFuture<Void> {
        private ManualScheduledFuture(Runnable command) {
            super(command, null);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed other) {
            return 0;
        }
    }
}
