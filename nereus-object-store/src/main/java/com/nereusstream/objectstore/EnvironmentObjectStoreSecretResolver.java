/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public final class EnvironmentObjectStoreSecretResolver implements ObjectStoreSecretResolver {

    private final Function<String, String> environment;

    public EnvironmentObjectStoreSecretResolver() {
        this(System::getenv);
    }

    EnvironmentObjectStoreSecretResolver(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    @Override
    public Optional<char[]> resolve(String secretReference) {
        if (secretReference == null || secretReference.isBlank()) {
            return Optional.empty();
        }

        String value = environment.apply(secretReference);
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(value.toCharArray());
    }
}
