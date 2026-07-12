/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class S3CompatibleObjectStoreProviderTest {
    @Test
    void clearsResolverOwnedSecretArraysAndAllowsExactlyOneStore() throws Exception {
        char[] access = "access-key".toCharArray();
        char[] secret = "secret-key".toCharArray();
        Map<String, char[]> values = Map.of("access", access, "secret", secret);
        ObjectStoreConfiguration config = new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                URI.create("http://127.0.0.1:9000"),
                "us-east-1",
                "nereus",
                "objects",
                true,
                Duration.ofSeconds(1),
                2,
                Optional.of("access"),
                Optional.of("secret"),
                Optional.empty());
        S3CompatibleObjectStoreProvider provider = new S3CompatibleObjectStoreProvider();
        ObjectStore store = provider.create(config, reference -> Optional.ofNullable(values.get(reference)));
        try {
            assertThat(access).containsOnly('\0');
            assertThat(secret).containsOnly('\0');
            assertThatThrownBy(() -> provider.create(config, reference -> Optional.empty()))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            store.close();
            provider.close();
        }
    }

    @Test
    void missingExplicitSecretFailsClosed() {
        ObjectStoreConfiguration config = new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                URI.create("https://s3.example.com"),
                "us-east-1", "bucket", "prefix", false,
                Duration.ofSeconds(1), 1,
                Optional.of("access"), Optional.of("secret"), Optional.empty());
        assertThatThrownBy(() -> new S3CompatibleObjectStoreProvider().create(
                config, new NoopObjectStoreSecretResolver()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unresolved");
    }
}
