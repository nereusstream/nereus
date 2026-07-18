/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PhysicalGcLifecyclePassTest {
    @Test
    void runsMetadataRootsThenRegistrationRetirementThenListing() {
        ArrayList<String> order = new ArrayList<>();
        PhysicalGcLifecyclePass pass = new PhysicalGcLifecyclePass(
                () -> {
                    order.add("roots");
                    return CompletableFuture.completedFuture(roots());
                },
                () -> {
                    order.add("registrations");
                    return CompletableFuture.completedFuture(registrations());
                },
                () -> {
                    order.add("inventory");
                    return CompletableFuture.completedFuture(inventory());
                });

        PhysicalGcLifecyclePassResult result = pass.scan().join();

        assertThat(order).containsExactly("roots", "registrations", "inventory");
        assertThat(result.roots()).isEqualTo(roots());
        assertThat(result.registrations()).isEqualTo(registrations());
        assertThat(result.inventory()).isEqualTo(inventory());
    }

    @Test
    void failedMetadataStagePreventsEveryLaterStage() {
        ArrayList<String> order = new ArrayList<>();
        PhysicalGcLifecyclePass pass = new PhysicalGcLifecyclePass(
                () -> {
                    order.add("roots");
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("root failure"));
                },
                () -> {
                    order.add("registrations");
                    return CompletableFuture.completedFuture(registrations());
                },
                () -> {
                    order.add("inventory");
                    return CompletableFuture.completedFuture(inventory());
                });

        assertThatThrownBy(() -> pass.scan().join())
                .hasRootCauseMessage("root failure");
        assertThat(order).containsExactly("roots");
    }

    static PhysicalGcLifecyclePassResult result() {
        return new PhysicalGcLifecyclePassResult(
                roots(), registrations(), inventory());
    }

    private static PhysicalObjectRootScanResult roots() {
        return new PhysicalObjectRootScanResult(1, 2, 3, 4, 5);
    }

    private static StreamRegistrationRetirementScanResult registrations() {
        EnumMap<StreamRegistrationRetirementStatus, Long> statuses =
                new EnumMap<>(StreamRegistrationRetirementStatus.class);
        for (StreamRegistrationRetirementStatus status :
                StreamRegistrationRetirementStatus.values()) {
            statuses.put(status, 0L);
        }
        return new StreamRegistrationRetirementScanResult(64, 0, statuses);
    }

    private static ObjectInventoryScanResult inventory() {
        return new ObjectInventoryScanResult(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }
}
