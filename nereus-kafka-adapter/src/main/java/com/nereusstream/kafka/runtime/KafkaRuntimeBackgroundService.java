/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * One runtime-owned background service that must start before admission opens and drain before
 * partition sessions close.
 */
public interface KafkaRuntimeBackgroundService {
    CompletionStage<Void> start();

    CompletionStage<Void> closeAsync();

    static KafkaRuntimeBackgroundService composite(
            List<? extends KafkaRuntimeBackgroundService> services) {
        List<KafkaRuntimeBackgroundService> exact = List.copyOf(
                Objects.requireNonNull(services, "services"));
        exact.forEach(service -> Objects.requireNonNull(service, "backgroundService"));
        if (exact.isEmpty()) {
            return none();
        }
        if (exact.size() == 1) {
            return exact.get(0);
        }
        return new CompositeKafkaRuntimeBackgroundService(exact);
    }

    static KafkaRuntimeBackgroundService none() {
        return NoOpHolder.INSTANCE;
    }

    final class NoOpHolder {
        private static final KafkaRuntimeBackgroundService INSTANCE = new KafkaRuntimeBackgroundService() {
            @Override
            public CompletionStage<Void> start() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletionStage<Void> closeAsync() {
                return CompletableFuture.completedFuture(null);
            }
        };

        private NoOpHolder() {}
    }
}
