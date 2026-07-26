/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AdminConfigurationTest {

    @Test
    void loadsValidMinimalConfig(@TempDir Path tempDir) throws IOException {
        Path config = tempDir.resolve("admin.properties");
        Files.writeString(config,
                "cluster=test-cluster\n"
                        + "oxia.serviceAddress=localhost:6648\n"
                        + "oxia.namespace=nereus\n"
                        + "oxia.sessionTimeoutSeconds=30\n"
                        + "oxia.maxPendingOperations=1024\n"
                        + "objectStore.providerClassName=com.nereusstream.objectstore.S3CompatibleObjectStoreProvider\n"
                        + "objectStore.endpoint=http://localhost:8333\n"
                        + "objectStore.region=us-east-1\n"
                        + "objectStore.bucket=nereus-benchmark\n"
                        + "objectStore.prefix=v0.1.0\n"
                        + "objectStore.pathStyleAccess=true\n"
                        + "objectStore.requestTimeoutSeconds=30\n"
                        + "objectStore.maxConnections=64\n"
                        + "objectStore.secretResolverClassName=com.nereusstream.objectstore.EnvironmentObjectStoreSecretResolver\n"
                        + "objectStore.accessKeySecretRef=NEREUS_S3_ACCESS_KEY\n"
                        + "objectStore.secretKeySecretRef=NEREUS_S3_SECRET_KEY\n"
                        + "bookkeeper.deploymentId=nereus-v010-benchmark\n"
                        + "bookkeeper.providerScopeSha256=" + "a".repeat(64) + "\n"
                        + "bookkeeper.ledgerIdPrefixBits=12\n"
                        + "bookkeeper.ledgerIdPrefixValue=2049\n"
                        + "bookkeeper.reservationId=test-reservation-id\n"
                        + "bookkeeper.ensembleSize=3\n"
                        + "bookkeeper.writeQuorumSize=3\n"
                        + "bookkeeper.ackQuorumSize=2\n"
                        + "bookkeeper.digestType=CRC32C\n"
                        + "bookkeeper.passwordSecretRef=NEREUS_BK_PASSWORD\n"
                        + "bookkeeper.passwordIdentityVersion=v1\n"
                        + "operatorEvidenceSha256=" + "b".repeat(64) + "\n");

        AdminConfiguration adminConfig = AdminConfiguration.load(config);

        assertThat(adminConfig.cluster()).isEqualTo("test-cluster");
        assertThat(adminConfig.oxia().serviceAddress()).isEqualTo("localhost:6648");
        assertThat(adminConfig.oxia().namespace()).isEqualTo("nereus");
        assertThat(adminConfig.objectStore().bucket()).isEqualTo("nereus-benchmark");
        assertThat(adminConfig.objectStore().prefix()).isEqualTo("v0.1.0");
        assertThat(adminConfig.bookKeeper().deploymentId()).isEqualTo("nereus-v010-benchmark");
        assertThat(adminConfig.operatorEvidenceSha256()).isEqualTo("b".repeat(64));
    }

    @Test
    void rejectsMissingRequiredKey(@TempDir Path tempDir) throws IOException {
        Path config = tempDir.resolve("admin.properties");
        Files.writeString(config, "cluster=test\n");

        assertThatThrownBy(() -> AdminConfiguration.load(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oxia.serviceAddress");
    }

    @Test
    void rejectsUnknownKeysInsteadOfSilentlyIgnoringTypos(
            @TempDir Path tempDir) throws IOException {
        Path config = tempDir.resolve("admin.properties");
        Files.writeString(config,
                "cluster=test\n"
                        + "objectStore.maxConnection=64\n");

        assertThatThrownBy(() -> AdminConfiguration.load(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "unsupported config keys: objectStore.maxConnection");
    }

    @Test
    void usesDefaultsForOptionalFields(@TempDir Path tempDir) throws IOException {
        Path config = tempDir.resolve("admin.properties");
        Files.writeString(config,
                "cluster=test-cluster\n"
                        + "oxia.serviceAddress=localhost:6648\n"
                        + "oxia.namespace=nereus\n"
                        + "objectStore.providerClassName=com.nereusstream.objectstore.S3CompatibleObjectStoreProvider\n"
                        + "objectStore.endpoint=http://localhost:8333\n"
                        + "objectStore.region=us-east-1\n"
                        + "objectStore.bucket=test\n"
                        + "objectStore.prefix=test\n"
                        + "objectStore.secretResolverClassName="
                        + "com.nereusstream.objectstore.EnvironmentObjectStoreSecretResolver\n"
                        + "bookkeeper.deploymentId=test\n"
                        + "bookkeeper.providerScopeSha256=" + "a".repeat(64) + "\n"
                        + "bookkeeper.reservationId=test\n"
                        + "bookkeeper.digestType=CRC32C\n"
                        + "bookkeeper.passwordSecretRef=TEST\n"
                        + "bookkeeper.passwordIdentityVersion=v1\n"
                        + "operatorEvidenceSha256=" + "b".repeat(64) + "\n");

        AdminConfiguration adminConfig = AdminConfiguration.load(config);

        assertThat(adminConfig.objectStore().maxConnections()).isEqualTo(64);
        assertThat(adminConfig.objectStore().requestTimeout().toSeconds()).isEqualTo(30);
        assertThat(adminConfig.objectStoreSecretResolverClassName())
                .isEqualTo(
                        "com.nereusstream.objectstore.EnvironmentObjectStoreSecretResolver");
    }

    @Test
    void rejectsUnknownFile(@TempDir Path tempDir) {
        Path config = tempDir.resolve("nonexistent.properties");

        assertThatThrownBy(() -> AdminConfiguration.load(config))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void rejectsInvalidBooleanInsteadOfSilentlyDisablingIt(
            @TempDir Path tempDir) throws IOException {
        Path config = tempDir.resolve("admin.properties");
        Files.writeString(config,
                "cluster=test-cluster\n"
                        + "oxia.serviceAddress=localhost:6648\n"
                        + "oxia.namespace=nereus\n"
                        + "objectStore.providerClassName="
                        + "com.nereusstream.objectstore.S3CompatibleObjectStoreProvider\n"
                        + "objectStore.endpoint=http://localhost:8333\n"
                        + "objectStore.region=us-east-1\n"
                        + "objectStore.bucket=test\n"
                        + "objectStore.prefix=test\n"
                        + "objectStore.pathStyleAccess=tru\n"
                        + "objectStore.secretResolverClassName="
                        + "com.nereusstream.objectstore.EnvironmentObjectStoreSecretResolver\n"
                        + "bookkeeper.deploymentId=test\n"
                        + "bookkeeper.providerScopeSha256=" + "a".repeat(64) + "\n"
                        + "bookkeeper.reservationId=test\n"
                        + "bookkeeper.digestType=CRC32C\n"
                        + "bookkeeper.passwordSecretRef=TEST\n"
                        + "bookkeeper.passwordIdentityVersion=v1\n"
                        + "operatorEvidenceSha256=" + "b".repeat(64) + "\n");

        assertThatThrownBy(() -> AdminConfiguration.load(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid boolean");
    }

    @Test
    void loadsObjectStoreOnlyConfigWhenBookKeeperAdministrationIsDisabled(
            @TempDir Path tempDir) throws IOException {
        Path config = tempDir.resolve("admin.properties");
        Files.writeString(config,
                "cluster=test-cluster\n"
                        + "oxia.serviceAddress=localhost:6648\n"
                        + "oxia.namespace=nereus\n"
                        + "objectStore.providerClassName="
                        + "com.nereusstream.objectstore.S3CompatibleObjectStoreProvider\n"
                        + "objectStore.endpoint=http://localhost:8333\n"
                        + "objectStore.region=us-east-1\n"
                        + "objectStore.bucket=test\n"
                        + "objectStore.prefix=test\n"
                        + "objectStore.secretResolverClassName="
                        + "com.nereusstream.objectstore.EnvironmentObjectStoreSecretResolver\n"
                        + "bookkeeper.enabled=false\n");

        AdminConfiguration adminConfig = AdminConfiguration.load(config);

        assertThat(adminConfig.optionalBookKeeper()).isEmpty();
        assertThatThrownBy(adminConfig::bookKeeper)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("disabled");
    }
}
