/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

class S3CompatibleObjectStoreProviderTest {
    @Test
    void clearsResolverOwnedSecretArrays() {
        char[] access = "access-key".toCharArray();
        char[] secret = "secret-key".toCharArray();
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
        AwsCredentials credentials = S3CompatibleObjectStoreProvider.credentials(
                config, reference -> Optional.of("access".equals(reference) ? access : secret))
                .resolveCredentials();
        assertThat(credentials.accessKeyId()).isEqualTo("access-key");
        assertThat(credentials.secretAccessKey()).isEqualTo("secret-key");
        assertThat(access).containsOnly('\0');
        assertThat(secret).containsOnly('\0');
    }

    @Test
    void missingExplicitSecretFailsClosed() {
        ObjectStoreConfiguration config = new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                URI.create("https://s3.example.com"),
                "us-east-1", "bucket", "prefix", false,
                Duration.ofSeconds(1), 1,
                Optional.of("access"), Optional.of("secret"), Optional.empty());
        assertThatThrownBy(() -> S3CompatibleObjectStoreProvider.credentials(
                config, new NoopObjectStoreSecretResolver()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unresolved");
    }
}
