/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CompositeOwnedRuntimeTest {
    @Test
    void exposesTypedComponentsWithoutChangingTheSingleCloseBoundary() {
        AtomicInteger closes = new AtomicInteger();
        TestComponent component = new TestComponent(closes);

        CompositeOwnedRuntime owner = new CompositeOwnedRuntime(component, component);

        assertThat(owner.component(TestComponent.class)).containsSame(component);
        assertThat(owner.requireComponent(TestComponent.class)).isSameAs(component);
        assertThat(owner.component(OtherComponent.class)).isEmpty();
        assertThatThrownBy(() -> owner.requireComponent(OtherComponent.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OtherComponent.class.getName());

        owner.close();
        owner.close();
        assertThat(closes).hasValue(1);
    }

    private record TestComponent(AtomicInteger closes) implements AutoCloseable {
        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    private static final class OtherComponent implements AutoCloseable {
        @Override
        public void close() {}
    }
}
