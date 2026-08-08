/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class CommandLineArgumentsTest {

    @Test
    void parsesMinimumArguments() throws IOException {
        Path config = Files.createTempFile("config", ".properties");
        Files.writeString(
                config,
                "cluster=test\noxia.serviceAddress=localhost:6648\n"
                        + "oxia.namespace=test\nobjectStore.providerClassName=com.example.Provider\n"
                        + "objectStore.endpoint=http://localhost:9000\n"
                        + "objectStore.region=us-east-1\nobjectStore.bucket=test\n"
                        + "objectStore.prefix=test\nbookkeeper.deploymentId=test\n"
                        + "bookkeeper.providerScopeSha256=" + "a".repeat(64) + "\n"
                        + "bookkeeper.reservationId=test\nbookkeeper.digestType=CRC32C\n"
                        + "bookkeeper.passwordSecretRef=TEST\nbookkeeper.passwordIdentityVersion=v1\n"
                        + "operatorEvidenceSha256=" + "b".repeat(64) + "\n");

        CommandLineArguments args = CommandLineArguments.parse(
                new String[] {"bookkeeper", "namespace", "ensure", "--config", config.toString()});

        assertThat(args.command()).isEqualTo("bookkeeper");
        assertThat(args.subcommand()).isEqualTo("namespace");
        assertThat(args.action()).contains("ensure");
        assertThat(args.commandPath()).containsExactly("bookkeeper", "namespace", "ensure");
        assertThat(args.config()).isEqualTo(config);
        assertThat(args.timeoutSeconds()).isEqualTo(600);
    }

    @Test
    void parsesAllOptions() throws IOException {
        Path config = Files.createTempFile("config", ".properties");
        Files.writeString(
                config,
                "cluster=test\noxia.serviceAddress=localhost:6648\n"
                        + "oxia.namespace=test\nobjectStore.providerClassName=com.example.Provider\n"
                        + "objectStore.endpoint=http://localhost:9000\n"
                        + "objectStore.region=us-east-1\nobjectStore.bucket=test\n"
                        + "objectStore.prefix=test\nbookkeeper.deploymentId=test\n"
                        + "bookkeeper.providerScopeSha256=" + "a".repeat(64) + "\n"
                        + "bookkeeper.reservationId=test\nbookkeeper.digestType=CRC32C\n"
                        + "bookkeeper.passwordSecretRef=TEST\nbookkeeper.passwordIdentityVersion=v1\n"
                        + "operatorEvidenceSha256=" + "b".repeat(64) + "\n");

        CommandLineArguments args = CommandLineArguments.parse(new String[] {
            "object-store", "contract",
            "--config", config.toString(),
            "--timeout-seconds", "300",
            "--output", "/tmp/evidence.json"
        });

        assertThat(args.command()).isEqualTo("object-store");
        assertThat(args.subcommand()).isEqualTo("contract");
        assertThat(args.timeoutSeconds()).isEqualTo(300);
        assertThat(args.outputFile()).isPresent();
        assertThat(args.outputFile().get()).isEqualTo(Path.of("/tmp/evidence.json"));
    }

    @Test
    void parsesPersistenceCommandWithRequiredRunId() throws IOException {
        Path config = Files.createTempFile("config", ".properties");

        CommandLineArguments args = CommandLineArguments.parse(new String[] {
            "object-store",
            "persistence",
            "verify",
            "--config",
            config.toString(),
            "--run-id",
            "abcdefghijklmnopqrstuvwxyz"
        });

        assertThat(args.commandPath()).containsExactly("object-store", "persistence", "verify");
        assertThat(args.runId()).contains("abcdefghijklmnopqrstuvwxyz");
    }

    @Test
    void rejectsMissingOrMalformedPersistenceRunId() throws IOException {
        Path config = Files.createTempFile("config", ".properties");

        assertThatThrownBy(() -> CommandLineArguments.parse(
                        new String[] {"object-store", "persistence", "create", "--config", config.toString()}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--run-id is required");
        assertThatThrownBy(() -> CommandLineArguments.parse(new String[] {
                    "object-store", "persistence", "cleanup", "--config", config.toString(), "--run-id", "NOT_base32"
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("run-id");
    }

    @Test
    void rejectsRunIdForUnrelatedCommand() throws IOException {
        Path config = Files.createTempFile("config", ".properties");

        assertThatThrownBy(() -> CommandLineArguments.parse(new String[] {
                    "object-store", "contract",
                    "--config", config.toString(),
                    "--run-id", "abcdefghijklmnopqrstuvwxyz"
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only valid");
    }

    @Test
    void rejectsMissingConfig() {
        assertThatThrownBy(() -> CommandLineArguments.parse(new String[] {"bookkeeper", "namespace", "ensure"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--config");
    }

    @Test
    void rejectsUnknownCommand() {
        assertThatThrownBy(() -> CommandLineArguments.parse(
                        new String[] {"unknown", "command", "--config", "/nonexistent.properties"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown command");
    }

    @Test
    void rejectsNegativeTimeout() throws IOException {
        Path config = Files.createTempFile("config", ".properties");
        Files.writeString(
                config,
                "cluster=test\noxia.serviceAddress=localhost:6648\n"
                        + "oxia.namespace=test\nobjectStore.providerClassName=com.example.Provider\n"
                        + "objectStore.endpoint=http://localhost:9000\n"
                        + "objectStore.region=us-east-1\nobjectStore.bucket=test\n"
                        + "objectStore.prefix=test\nbookkeeper.deploymentId=test\n"
                        + "bookkeeper.providerScopeSha256=" + "a".repeat(64) + "\n"
                        + "bookkeeper.reservationId=test\nbookkeeper.digestType=CRC32C\n"
                        + "bookkeeper.passwordSecretRef=TEST\nbookkeeper.passwordIdentityVersion=v1\n"
                        + "operatorEvidenceSha256=" + "b".repeat(64) + "\n");

        assertThatThrownBy(() -> CommandLineArguments.parse(new String[] {
                    "bookkeeper", "namespace", "verify", "--config", config.toString(), "--timeout-seconds", "-1"
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout-seconds must be positive");
    }

    @Test
    void rejectsDuplicateOption() throws IOException {
        Path config = Files.createTempFile("config", ".properties");
        Files.writeString(
                config,
                "cluster=test\noxia.serviceAddress=localhost:6648\n"
                        + "oxia.namespace=test\nobjectStore.providerClassName=com.example.Provider\n"
                        + "objectStore.endpoint=http://localhost:9000\n"
                        + "objectStore.region=us-east-1\nobjectStore.bucket=test\n"
                        + "objectStore.prefix=test\nbookkeeper.deploymentId=test\n"
                        + "bookkeeper.providerScopeSha256=" + "a".repeat(64) + "\n"
                        + "bookkeeper.reservationId=test\nbookkeeper.digestType=CRC32C\n"
                        + "bookkeeper.passwordSecretRef=TEST\nbookkeeper.passwordIdentityVersion=v1\n"
                        + "operatorEvidenceSha256=" + "b".repeat(64) + "\n");

        assertThatThrownBy(() -> CommandLineArguments.parse(new String[] {
                    "bookkeeper", "namespace", "ensure", "--config", config.toString(), "--config", "/other.properties"
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate option");
    }

    @Test
    void rejectsMissingOptionValue() throws IOException {
        Path config = Files.createTempFile("config", ".properties");
        Files.writeString(
                config,
                "cluster=test\noxia.serviceAddress=localhost:6648\n"
                        + "oxia.namespace=test\nobjectStore.providerClassName=com.example.Provider\n"
                        + "objectStore.endpoint=http://localhost:9000\n"
                        + "objectStore.region=us-east-1\nobjectStore.bucket=test\n"
                        + "objectStore.prefix=test\nbookkeeper.deploymentId=test\n"
                        + "bookkeeper.providerScopeSha256=" + "a".repeat(64) + "\n"
                        + "bookkeeper.reservationId=test\nbookkeeper.digestType=CRC32C\n"
                        + "bookkeeper.passwordSecretRef=TEST\nbookkeeper.passwordIdentityVersion=v1\n"
                        + "operatorEvidenceSha256=" + "b".repeat(64) + "\n");

        assertThatThrownBy(() ->
                        CommandLineArguments.parse(new String[] {"bookkeeper", "namespace", "ensure", "--config"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing value");
    }

    @Test
    void rejectsUnknownOption() throws IOException {
        Path config = Files.createTempFile("config", ".properties");

        assertThatThrownBy(() -> CommandLineArguments.parse(new String[] {
                    "bookkeeper", "activation", "read", "--config", config.toString(), "--mutate", "true"
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown option --mutate");
    }

    @Test
    void rejectsMissingNamespaceAction() throws IOException {
        Path config = Files.createTempFile("config", ".properties");

        assertThatThrownBy(() -> CommandLineArguments.parse(
                        new String[] {"bookkeeper", "namespace", "--config", config.toString()}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown command");
    }
}
