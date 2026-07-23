/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/** Explicit owned-provider and borrowed Kafka dependencies for Object-WAL runtime bootstrap. */
public record NereusKafkaObjectWalRuntimeContext(
        ObjectStoreProvider objectStoreProvider,
        ObjectStoreSecretResolver secretResolver,
        ScheduledExecutorService renewalScheduler,
        KafkaPartitionRecoveryLauncher recoveryLauncher,
        Clock clock,
        Supplier<? extends CompletionStage<Void>> startupAction) {
    public NereusKafkaObjectWalRuntimeContext {
        Objects.requireNonNull(objectStoreProvider, "objectStoreProvider");
        Objects.requireNonNull(secretResolver, "secretResolver");
        Objects.requireNonNull(renewalScheduler, "renewalScheduler");
        Objects.requireNonNull(recoveryLauncher, "recoveryLauncher");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(startupAction, "startupAction");
    }
}
