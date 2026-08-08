/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class EnvironmentObjectStoreSecretResolverTest {

    @Test
    void resolvesExactEnvironmentVariable() {
        Map<String, String> env = Map.of("SECRET_REF", "my-secret-value");
        EnvironmentObjectStoreSecretResolver resolver = new EnvironmentObjectStoreSecretResolver(env::get);
        Optional<char[]> result = resolver.resolve("SECRET_REF");
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("my-secret-value".toCharArray());
    }

    @Test
    void missingVariableReturnsEmpty() {
        EnvironmentObjectStoreSecretResolver resolver = new EnvironmentObjectStoreSecretResolver(k -> null);
        assertThat(resolver.resolve("MISSING")).isEmpty();
    }

    @Test
    void blankReferenceReturnsEmpty() {
        EnvironmentObjectStoreSecretResolver resolver = new EnvironmentObjectStoreSecretResolver(k -> "x");
        assertThat(resolver.resolve(null)).isEmpty();
        assertThat(resolver.resolve("")).isEmpty();
        assertThat(resolver.resolve("  ")).isEmpty();
    }

    @Test
    void returnsFreshArrayForEveryResolution() {
        Map<String, String> env = Map.of("KEY", "value");
        EnvironmentObjectStoreSecretResolver resolver = new EnvironmentObjectStoreSecretResolver(env::get);
        char[] first = resolver.resolve("KEY").orElseThrow();
        char[] second = resolver.resolve("KEY").orElseThrow();
        assertThat(first).isNotSameAs(second);
        Arrays.fill(first, '\0');
        assertThat(second).isEqualTo("value".toCharArray());
    }

    @Test
    void preservesWhitespaceInsideSecretValue() {
        Map<String, String> env = Map.of("KEY", " value with spaces ");
        EnvironmentObjectStoreSecretResolver resolver = new EnvironmentObjectStoreSecretResolver(env::get);
        assertThat(resolver.resolve("KEY").orElseThrow()).isEqualTo(" value with spaces ".toCharArray());
    }

    @Test
    void emptyEnvironmentVariableReturnsEmpty() {
        Map<String, String> env = Map.of("KEY", "");
        EnvironmentObjectStoreSecretResolver resolver = new EnvironmentObjectStoreSecretResolver(env::get);
        assertThat(resolver.resolve("KEY")).isEmpty();
    }
}
