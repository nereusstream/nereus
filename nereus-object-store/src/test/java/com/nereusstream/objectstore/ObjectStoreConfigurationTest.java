/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ObjectStoreConfigurationTest {
    @Test
    void acceptsCanonicalEndpointPrefixAndCredentialReferences() {
        ObjectStoreConfiguration config = config(
                URI.create("https://s3.example.com"),
                "nereus/cluster-a",
                Optional.of("access-ref"),
                Optional.of("secret-ref"),
                Optional.of("session-ref"));

        assertThat(config.endpoint().getHost()).isEqualTo("s3.example.com");
        assertThat(config.prefix()).isEqualTo("nereus/cluster-a");
        assertThat(config.sessionTokenSecretRef()).contains("session-ref");
    }

    @Test
    void rejectsEndpointPrefixAndCredentialCombinationsThatCouldAliasOrLeak() {
        assertThatThrownBy(() -> config(
                        URI.create("https://user@s3.example.com?secret=x"), "nereus/cluster",
                        Optional.empty(), Optional.empty(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("origin");
        for (String prefix : new String[] {"/nereus", "nereus/", "nereus//cluster", "nereus/../cluster"}) {
            assertThatThrownBy(() -> config(
                            URI.create("https://s3.example.com"), prefix,
                            Optional.empty(), Optional.empty(), Optional.empty()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> config(
                        URI.create("https://s3.example.com"), "nereus/cluster",
                        Optional.of("access"), Optional.empty(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("together");
        assertThatThrownBy(() -> config(
                        URI.create("https://s3.example.com"), "nereus/cluster",
                        Optional.empty(), Optional.empty(), Optional.of("session")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires");
        assertThatThrownBy(() -> config(
                        URI.create("https://s3.example.com"), "nereus/cluster",
                        Optional.of(" "), Optional.of("secret"), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ObjectStoreConfiguration config(
            URI endpoint,
            String prefix,
            Optional<String> access,
            Optional<String> secret,
            Optional<String> session) {
        return new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                endpoint,
                "us-east-1",
                "bucket",
                prefix,
                true,
                Duration.ofSeconds(30),
                64,
                access,
                secret,
                session);
    }
}
