/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Startup boundary that may remove runtime admission after an asynchronous control-plane failure. */
@FunctionalInterface
public interface KafkaRuntimeStartup {
    CompletionStage<Void> start(KafkaStorageAdmission admission);

    static KafkaRuntimeStartup from(Supplier<? extends CompletionStage<Void>> action) {
        Supplier<? extends CompletionStage<Void>> exact = Objects.requireNonNull(action, "action");
        return ignored -> exact.get();
    }
}
