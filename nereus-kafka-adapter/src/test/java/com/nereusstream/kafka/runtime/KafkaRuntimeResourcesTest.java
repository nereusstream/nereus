/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaRuntimeResourcesTest {
    @Test
    void ownedResourcesCloseOnceInReverseOrderAndBorrowedResourcesStayOpen() {
        List<String> closes = new ArrayList<>();
        AutoCloseable first = () -> closes.add("first");
        AutoCloseable borrowed = () -> closes.add("borrowed");
        AutoCloseable last = () -> closes.add("last");
        KafkaRuntimeResources resources = new KafkaRuntimeResources(List.of(
                KafkaRuntimeResources.Resource.owned("first", first),
                KafkaRuntimeResources.Resource.borrowed("borrowed", borrowed),
                KafkaRuntimeResources.Resource.owned("last", last)));

        resources.close();
        resources.close();

        assertThat(closes).containsExactly("last", "first");
        assertThat(resources.closed()).isTrue();
    }

    @Test
    void duplicateIdentityAndMixedOwnershipAreRejected() {
        AutoCloseable shared = () -> { };

        assertThatThrownBy(() -> new KafkaRuntimeResources(List.of(
                KafkaRuntimeResources.Resource.owned("owned", shared),
                KafkaRuntimeResources.Resource.borrowed("borrowed", shared))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("OWNED/BORROWED");
    }

    @Test
    void everyOwnedCloseIsAttemptedAndFailuresAreAggregated() {
        List<String> closes = new ArrayList<>();
        KafkaRuntimeResources resources = new KafkaRuntimeResources(List.of(
                KafkaRuntimeResources.Resource.owned("first", () -> {
                    closes.add("first");
                    throw new IllegalStateException("first failed");
                }),
                KafkaRuntimeResources.Resource.owned("last", () -> {
                    closes.add("last");
                    throw new IllegalStateException("last failed");
                })));

        assertThatThrownBy(resources::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("2 owned")
                .satisfies(failure -> assertThat(failure.getSuppressed()).hasSize(2));
        assertThat(closes).containsExactly("last", "first");
    }
}
