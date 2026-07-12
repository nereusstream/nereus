/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import org.junit.jupiter.api.Test;

class DefaultNereusRuntimeProviderTest {
    @Test
    void rejectsClassThatDoesNotImplementProviderBeforeConstruction() {
        assertThatThrownBy(() -> DefaultNereusRuntimeProvider.instantiateObjectStoreProvider(
                String.class.getName(), getClass().getClassLoader()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ObjectStoreProvider");
    }

    @Test
    void requiresPublicNoArgConstructor() {
        assertThatThrownBy(() -> DefaultNereusRuntimeProvider.instantiateObjectStoreProvider(
                PrivateProvider.class.getName(), getClass().getClassLoader()))
                .isInstanceOf(NoSuchMethodException.class);
    }

    @Test
    void exposesProviderConstructorFailureWithoutCreatingClients() {
        assertThatThrownBy(() -> DefaultNereusRuntimeProvider.instantiateObjectStoreProvider(
                ThrowingProvider.class.getName(), getClass().getClassLoader()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("constructor sentinel");
    }

    private static final class PrivateProvider implements ObjectStoreProvider {
        private PrivateProvider() {
        }

        @Override
        public ObjectStore create(ObjectStoreConfiguration configuration, ObjectStoreSecretResolver resolver) {
            throw new AssertionError("must not be called");
        }

        @Override
        public void close() {
        }
    }

    public static final class ThrowingProvider implements ObjectStoreProvider {
        public ThrowingProvider() {
            throw new IllegalStateException("constructor sentinel");
        }

        @Override
        public ObjectStore create(ObjectStoreConfiguration configuration, ObjectStoreSecretResolver resolver) {
            throw new AssertionError("must not be called");
        }

        @Override
        public void close() {
        }
    }
}
