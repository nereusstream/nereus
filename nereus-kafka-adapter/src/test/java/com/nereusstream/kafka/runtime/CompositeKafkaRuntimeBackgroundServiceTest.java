/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompositeKafkaRuntimeBackgroundServiceTest {
    @Test
    void startsInRegistrationOrderAndClosesInReverseOrder() {
        List<String> events = new ArrayList<>();
        KafkaRuntimeBackgroundService services =
                KafkaRuntimeBackgroundService.composite(
                        List.of(
                                service("materialization", events),
                                service("bookkeeper-retention", events)));

        services.start().toCompletableFuture().join();
        services.closeAsync().toCompletableFuture().join();

        assertThat(events)
                .containsExactly(
                        "start-materialization",
                        "start-bookkeeper-retention",
                        "close-bookkeeper-retention",
                        "close-materialization");
    }

    @Test
    void rollsBackStartedServicesWhenALaterStartFails() {
        List<String> events = new ArrayList<>();
        RuntimeException failure = new RuntimeException("retention start failed");
        KafkaRuntimeBackgroundService services =
                KafkaRuntimeBackgroundService.composite(
                        List.of(
                                service("materialization", events),
                                failingService(
                                        "bookkeeper-retention",
                                        events,
                                        failure)));

        assertThatThrownBy(
                        () ->
                                services.start()
                                        .toCompletableFuture()
                                        .join())
                .hasRootCause(failure);
        assertThat(events)
                .containsExactly(
                        "start-materialization",
                        "start-bookkeeper-retention",
                        "close-materialization");
    }

    private static KafkaRuntimeBackgroundService service(
            String name,
            List<String> events) {
        return new KafkaRuntimeBackgroundService() {
            @Override
            public CompletableFuture<Void> start() {
                events.add("start-" + name);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> closeAsync() {
                events.add("close-" + name);
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static KafkaRuntimeBackgroundService failingService(
            String name,
            List<String> events,
            RuntimeException failure) {
        return new KafkaRuntimeBackgroundService() {
            @Override
            public CompletableFuture<Void> start() {
                events.add("start-" + name);
                return CompletableFuture.failedFuture(failure);
            }

            @Override
            public CompletableFuture<Void> closeAsync() {
                events.add("close-" + name);
                return CompletableFuture.completedFuture(null);
            }
        };
    }
}
