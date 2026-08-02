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

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Process-level owner for non-overlapping Kafka compaction scan passes.
 *
 * <p>The scheduler deliberately keeps only one running and one coalesced pending pass. A trigger
 * that arrives while a pass is running is therefore not incorrectly acknowledged by that older
 * pass: its returned future completes only after the next pass has consumed the trigger. The
 * executor and scheduler are borrowed resources and are never closed here.
 */
public final class KafkaCompactionScheduler implements AutoCloseable {
  private static final System.Logger LOGGER =
      System.getLogger(KafkaCompactionScheduler.class.getName());

  private enum State {
    NEW,
    RUNNING,
    CLOSING,
    CLOSED
  }

  private final PassExecutor passExecutor;
  private final Duration interval;
  private final ScheduledExecutorService scheduler;
  private final Executor callbackExecutor;
  private final Object monitor = new Object();

  private State state = State.NEW;
  private ScheduledFuture<?> scheduled;
  private PendingPass active;
  private CompletableFuture<Void> activeSource;
  private PendingPass pending;
  private CompletableFuture<Void> closeFuture;

  public KafkaCompactionScheduler(
      PassExecutor passExecutor,
      Duration interval,
      ScheduledExecutorService scheduler,
      Executor callbackExecutor) {
    this.passExecutor = Objects.requireNonNull(passExecutor, "passExecutor");
    this.interval = requirePositive(interval, "interval");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
  }

  /**
   * Starts the owner and schedules an immediate startup pass.
   *
   * <p>Repeated calls while running are idempotent. A closed owner cannot be restarted.
   */
  public CompletableFuture<Void> start() {
    synchronized (monitor) {
      if (state == State.RUNNING) {
        return CompletableFuture.completedFuture(null);
      }
      if (state != State.NEW) {
        return CompletableFuture.failedFuture(closed("Kafka compaction scheduler cannot restart"));
      }
      state = State.RUNNING;
      try {
        scheduleLocked(Duration.ZERO, Trigger.STARTUP);
      } catch (RuntimeException failure) {
        state = State.CLOSED;
        return CompletableFuture.failedFuture(failure);
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Requests a pass and returns a cancellation-isolated view of the pass that consumes it.
   *
   * <p>Concurrent triggers during an active pass share the single pending pass and merge their
   * reasons. At most two pass futures are resident regardless of trigger volume.
   */
  public CompletableFuture<Void> trigger(Trigger trigger) {
    PendingPass target;
    boolean launch;
    synchronized (monitor) {
      if (state != State.RUNNING) {
        return CompletableFuture.failedFuture(
            state == State.NEW
                ? condition("Kafka compaction scheduler has not started")
                : closed("Kafka compaction scheduler is closing"));
      }
      cancelScheduledLocked();
      if (active == null) {
        target = new PendingPass(trigger);
        active = target;
        launch = true;
      } else {
        target = enqueuePendingLocked(trigger);
        launch = false;
      }
    }
    if (launch) {
      launch(target);
    }
    return view(target.completion);
  }

  public boolean isRunning() {
    synchronized (monitor) {
      return state == State.RUNNING;
    }
  }

  /**
   * Stops future passes, cancels the scheduler-owned active source, and never closes borrowed
   * executors.
   */
  public CompletableFuture<Void> closeAsync() {
    PendingPass pendingToFail;
    CompletableFuture<Void> sourceToCancel;
    synchronized (monitor) {
      if (closeFuture != null) {
        return closeFuture;
      }
      closeFuture = new CompletableFuture<>();
      if (state == State.CLOSED) {
        closeFuture.complete(null);
        return closeFuture;
      }
      state = State.CLOSING;
      cancelScheduledLocked();
      pendingToFail = pending;
      pending = null;
      sourceToCancel = activeSource;
      if (active == null) {
        completeCloseLocked();
      }
    }
    if (pendingToFail != null) {
      pendingToFail.completion.completeExceptionally(
          closed("Kafka compaction scheduler closed before the pending pass"));
    }
    if (sourceToCancel != null) {
      sourceToCancel.cancel(true);
    }
    return closeFuture;
  }

  @Override
  public void close() {
    closeAsync().join();
  }

  private void scheduledTrigger(Trigger trigger) {
    PendingPass target;
    synchronized (monitor) {
      scheduled = null;
      if (state != State.RUNNING) {
        return;
      }
      if (active != null) {
        enqueuePendingLocked(trigger);
        return;
      }
      target = new PendingPass(trigger);
      active = target;
    }
    launch(target);
  }

  private void launch(PendingPass target) {
    synchronized (monitor) {
      if (active != target || state != State.RUNNING) {
        target.completion.completeExceptionally(
            closed("Kafka compaction scheduler closed before launching the pass"));
        if (active == target) {
          active = null;
          completeCloseLocked();
        }
        return;
      }
    }
    TriggerBatch batch = target.freeze();
    CompletableFuture<Void> source;
    try {
      source =
          Objects.requireNonNull(
              passExecutor.run(batch), "Kafka compaction pass executor returned a null future");
    } catch (Throwable failure) {
      source = CompletableFuture.failedFuture(failure);
    }
    boolean cancelAfterPublish;
    synchronized (monitor) {
      if (active != target) {
        source.cancel(true);
        target.completion.completeExceptionally(
            closed("Kafka compaction scheduler no longer owns the pass"));
        return;
      }
      activeSource = source;
      cancelAfterPublish = state != State.RUNNING;
    }
    source.whenComplete(
        (ignored, failure) -> executeCallback(() -> finish(target, unwrapNullable(failure))));
    if (cancelAfterPublish) {
      source.cancel(true);
    }
  }

  private void finish(PendingPass target, Throwable failure) {
    PendingPass next = null;
    RuntimeException scheduleFailure = null;
    synchronized (monitor) {
      if (active != target) {
        return;
      }
      active = null;
      activeSource = null;
      if (state == State.RUNNING && pending != null) {
        next = pending;
        pending = null;
        active = next;
      } else if (state == State.RUNNING) {
        try {
          scheduleLocked(interval, Trigger.PERIODIC);
        } catch (RuntimeException rejected) {
          state = State.CLOSED;
          scheduleFailure = rejected;
        }
      } else {
        completeCloseLocked();
      }
    }

    Throwable completionFailure = failure == null ? scheduleFailure : failure;
    if (completionFailure == null) {
      target.completion.complete(null);
    } else {
      LOGGER.log(System.Logger.Level.ERROR, "Kafka compaction pass failed", completionFailure);
      target.completion.completeExceptionally(completionFailure);
    }
    if (next != null) {
      launch(next);
    }
  }

  private PendingPass enqueuePendingLocked(Trigger trigger) {
    if (pending == null) {
      pending = new PendingPass(trigger);
    } else {
      pending.add(trigger);
    }
    return pending;
  }

  private void scheduleLocked(Duration delay, Trigger trigger) {
    scheduled =
        scheduler.schedule(() -> scheduledTrigger(trigger), delay.toNanos(), TimeUnit.NANOSECONDS);
  }

  private void cancelScheduledLocked() {
    if (scheduled != null) {
      scheduled.cancel(false);
      scheduled = null;
    }
  }

  private void completeCloseLocked() {
    state = State.CLOSED;
    if (closeFuture != null) {
      closeFuture.complete(null);
    }
  }

  private void executeCallback(Runnable callback) {
    try {
      callbackExecutor.execute(callback);
    } catch (RejectedExecutionException rejected) {
      callback.run();
    }
  }

  private static CompletableFuture<Void> view(CompletableFuture<Void> operation) {
    return operation.thenApply(ignored -> null);
  }

  private static Throwable unwrapNullable(Throwable failure) {
    if (failure == null) {
      return null;
    }
    Throwable current = failure;
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static Duration requirePositive(Duration value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(field + " must be positive");
    }
    value.toNanos();
    return value;
  }

  private static NereusException condition(String message) {
    return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
  }

  private static NereusException closed(String message) {
    return new NereusException(ErrorCode.STORAGE_CLOSED, false, message);
  }

  @FunctionalInterface
  public interface PassExecutor {
    CompletableFuture<Void> run(TriggerBatch triggers);
  }

  /** Reasons are ordered by operational urgency, not enum declaration order. */
  public enum Trigger {
    PERIODIC(10),
    STARTUP(20),
    DIRTY_BYTES(30),
    ADMIN(40),
    MAX_LAG(50),
    POLICY_CHANGE(60),
    LEADERSHIP_CHANGE(70);

    private final int priority;

    Trigger(int priority) {
      this.priority = priority;
    }

    int priority() {
      return priority;
    }
  }

  /** Immutable aggregate consumed by one pass. */
  public record TriggerBatch(Set<Trigger> reasons, Trigger primary) {
    public TriggerBatch {
      Objects.requireNonNull(reasons, "reasons");
      primary = Objects.requireNonNull(primary, "primary");
      if (reasons.isEmpty() || !reasons.contains(primary)) {
        throw new IllegalArgumentException(
            "Kafka compaction trigger batch must contain its primary reason");
      }
      reasons = Collections.unmodifiableSet(EnumSet.copyOf(reasons));
      Trigger expected =
          reasons.stream()
              .max(
                  (left, right) -> {
                    int priority = Integer.compare(left.priority(), right.priority());
                    return priority != 0 ? priority : left.name().compareTo(right.name());
                  })
              .orElseThrow();
      if (primary != expected) {
        throw new IllegalArgumentException(
            "Kafka compaction primary trigger must be the highest-priority reason");
      }
    }
  }

  private static final class PendingPass {
    private final EnumSet<Trigger> triggers;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private boolean frozen;

    private PendingPass(Trigger trigger) {
      triggers = EnumSet.of(Objects.requireNonNull(trigger, "trigger"));
    }

    private void add(Trigger trigger) {
      if (frozen) {
        throw new IllegalStateException("Kafka compaction trigger batch is already frozen");
      }
      triggers.add(Objects.requireNonNull(trigger, "trigger"));
    }

    private TriggerBatch freeze() {
      frozen = true;
      Trigger primary =
          triggers.stream()
              .max(
                  (left, right) -> {
                    int priority = Integer.compare(left.priority(), right.priority());
                    return priority != 0 ? priority : left.name().compareTo(right.name());
                  })
              .orElseThrow();
      return new TriggerBatch(triggers, primary);
    }
  }
}
