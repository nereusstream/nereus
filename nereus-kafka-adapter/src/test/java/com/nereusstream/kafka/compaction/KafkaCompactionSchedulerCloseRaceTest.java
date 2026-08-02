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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaCompactionSchedulerCloseRaceTest {
  @Test
  void closeWaitsForNonCancellableSourceReturnedAfterClosingStarts() {
    ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
    try {
      CompletableFuture<Void> enteredRun = new CompletableFuture<>();
      CompletableFuture<Void> allowRunReturn = new CompletableFuture<>();
      CompletableFuture<Void> cancellationAttempted = new CompletableFuture<>();
      AtomicInteger cancelCalls = new AtomicInteger();
      CompletableFuture<Void> source =
          new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
              cancelCalls.incrementAndGet();
              cancellationAttempted.complete(null);
              return false;
            }
          };
      KafkaCompactionScheduler scheduler =
          new KafkaCompactionScheduler(
              triggers -> {
                enteredRun.complete(null);
                allowRunReturn.join();
                return source;
              },
              Duration.ofDays(1),
              timer,
              Runnable::run);

      scheduler.start().join();
      enteredRun.orTimeout(5, TimeUnit.SECONDS).join();

      CompletableFuture<Void> close = scheduler.closeAsync();
      assertThat(close).isNotDone();

      allowRunReturn.complete(null);
      cancellationAttempted.orTimeout(5, TimeUnit.SECONDS).join();
      assertThat(cancelCalls).hasValue(1);
      assertThat(close).isNotDone();

      source.complete(null);
      close.orTimeout(5, TimeUnit.SECONDS).join();
      assertThat(close).isCompletedWithValue(null);
      assertThat(timer.isShutdown()).isFalse();
    } finally {
      timer.shutdownNow();
    }
  }
}
