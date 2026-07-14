/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.callbacks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.NereusException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class CallbackPrimitivesTest {
    @Test
    void terminalCallbackInvokesExactlyOneWinnerAndAlwaysCleansUp() {
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger cleanups = new AtomicInteger();
        TerminalCallback<Integer> terminal = new TerminalCallback<>(
                ignored -> successes.incrementAndGet(),
                ignored -> failures.incrementAndGet(),
                cleanups::incrementAndGet);

        IntStream.range(0, 100).parallel().forEach(index -> {
            if ((index & 1) == 0) {
                terminal.tryComplete(index);
            } else {
                terminal.tryFail(new RuntimeException("failure"));
            }
        });

        assertThat(successes.get() + failures.get()).isEqualTo(1);
        assertThat(cleanups).hasValue(1);
        assertThat(terminal.isTerminal()).isTrue();
    }

    @Test
    void callbackExceptionStillRunsCleanup() {
        AtomicInteger cleanups = new AtomicInteger();
        TerminalCallback<Integer> terminal = new TerminalCallback<>(
                ignored -> { throw new IllegalStateException("callback"); },
                ignored -> { },
                cleanups::incrementAndGet);

        assertThatThrownBy(() -> terminal.tryComplete(1)).isInstanceOf(IllegalStateException.class);
        assertThat(cleanups).hasValue(1);
    }

    @Test
    void serialLanePreservesAdmissionOrderAcrossOutOfOrderCompletion() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            SerialCallbackLane lane = new SerialCallbackLane(executor, 3);
            long zero = lane.admit();
            long one = lane.admit();
            long two = lane.admit();
            List<Long> observed = Collections.synchronizedList(new ArrayList<>());

            lane.complete(two, () -> observed.add(two));
            lane.complete(one, () -> observed.add(one));
            lane.complete(zero, () -> observed.add(zero));
            lane.closeAfterDrain().get(5, TimeUnit.SECONDS);

            assertThat(observed).containsExactly(0L, 1L, 2L);
            assertThatThrownBy(lane::admit).isInstanceOf(NereusException.class);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void serialLaneRejectsUnknownDuplicateAndExcessAdmission() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            SerialCallbackLane lane = new SerialCallbackLane(executor, 1);
            long sequence = lane.admit();
            assertThatThrownBy(lane::admit).isInstanceOf(NereusException.class);
            assertThatThrownBy(() -> lane.complete(sequence + 1, () -> { }))
                    .isInstanceOf(NereusException.class);
            lane.complete(sequence, () -> { });
            assertThatThrownBy(() -> lane.complete(sequence, () -> { }))
                    .isInstanceOf(NereusException.class);
            CompletableFuture<Void> drained = lane.closeAfterDrain();
            drained.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void executorRejectionDrainsEveryAdmittedTerminalCallbackExactlyOnce() throws Exception {
        AtomicInteger callbacks = new AtomicInteger();
        SerialCallbackLane lane = new SerialCallbackLane(command -> {
            throw new RejectedExecutionException("callback executor stopped");
        }, 3);
        long zero = lane.admit();
        long one = lane.admit();

        lane.complete(one, callbacks::incrementAndGet);
        lane.complete(zero, callbacks::incrementAndGet);

        lane.closeAfterDrain().get(5, TimeUnit.SECONDS);
        assertThat(callbacks).hasValue(2);
    }
}
