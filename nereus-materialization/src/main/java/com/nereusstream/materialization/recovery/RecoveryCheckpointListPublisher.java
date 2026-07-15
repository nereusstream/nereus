/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Single-subscription synchronous list publisher with reentrant demand accounting. */
final class RecoveryCheckpointListPublisher<T> implements Flow.Publisher<T> {
    private final List<T> values;
    private final AtomicBoolean subscribed = new AtomicBoolean();

    RecoveryCheckpointListPublisher(List<T> values) {
        this.values = List.copyOf(Objects.requireNonNull(values, "values"));
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");
        if (!subscribed.compareAndSet(false, true)) {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long count) {
                }

                @Override
                public void cancel() {
                }
            });
            subscriber.onError(new IllegalStateException(
                    "recovery checkpoint list publisher permits one subscriber"));
            return;
        }
        subscriber.onSubscribe(new ListSubscription(subscriber));
    }

    private final class ListSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super T> subscriber;
        private final AtomicLong demand = new AtomicLong();
        private final AtomicInteger work = new AtomicInteger();
        private final AtomicBoolean terminal = new AtomicBoolean();
        private volatile boolean cancelled;
        private int index;

        private ListSubscription(Flow.Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long count) {
            if (count <= 0) {
                if (terminal.compareAndSet(false, true)) {
                    cancelled = true;
                    subscriber.onError(new IllegalArgumentException(
                            "publisher demand must be positive"));
                }
                return;
            }
            addDemand(count);
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        private void drain() {
            if (work.getAndIncrement() != 0) {
                return;
            }
            int missed = 1;
            do {
                long requested = demand.get();
                long emitted = 0;
                while (emitted != requested && !cancelled && index < values.size()) {
                    subscriber.onNext(values.get(index++));
                    emitted++;
                }
                if (emitted != 0 && requested != Long.MAX_VALUE) {
                    demand.addAndGet(-emitted);
                }
                if (!cancelled
                        && index == values.size()
                        && terminal.compareAndSet(false, true)) {
                    cancelled = true;
                    subscriber.onComplete();
                }
                missed = work.addAndGet(-missed);
            } while (missed != 0);
        }

        private void addDemand(long count) {
            while (true) {
                long current = demand.get();
                long updated = current + count;
                if (updated < 0) {
                    updated = Long.MAX_VALUE;
                }
                if (demand.compareAndSet(current, updated)) {
                    return;
                }
            }
        }
    }
}
