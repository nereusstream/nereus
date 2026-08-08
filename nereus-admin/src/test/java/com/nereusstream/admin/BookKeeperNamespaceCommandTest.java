/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BookKeeperNamespaceCommandTest {

    @Test
    void acceptsTheExactReadOnlyVerificationResultAndWritesEvidence(@TempDir Path tempDir) throws Exception {
        Path configPath = writeConfig(tempDir, "b".repeat(64));
        Path output = tempDir.resolve("verify.json");
        AdminConfiguration config = AdminConfiguration.load(configPath);
        CommandLineArguments arguments = CommandLineArguments.parse(new String[] {
            "bookkeeper", "namespace", "verify", "--config", configPath.toString(), "--output", output.toString()
        });

        AdminExitCode result =
                BookKeeperNamespaceCommand.evaluateVerify(config, reservation("b".repeat(64)), arguments);

        assertThat(result).isEqualTo(AdminExitCode.SUCCESS);
        assertThat(Files.readString(output))
                .contains("\"command\": \"bookkeeper namespace verify\"")
                .contains("\"status\": \"ACTIVE\"");
    }

    @Test
    void rejectsOperatorEvidenceDrift(@TempDir Path tempDir) throws Exception {
        Path configPath = writeConfig(tempDir, "b".repeat(64));
        AdminConfiguration config = AdminConfiguration.load(configPath);
        CommandLineArguments arguments = CommandLineArguments.parse(
                new String[] {"bookkeeper", "namespace", "verify", "--config", configPath.toString()});

        AdminExitCode result =
                BookKeeperNamespaceCommand.evaluateVerify(config, reservation("c".repeat(64)), arguments);

        assertThat(result).isEqualTo(AdminExitCode.CONDITION_FAILED);
    }

    private static BookKeeperLedgerIdNamespaceReservation reservation(String operatorEvidence) {
        return new BookKeeperLedgerIdNamespaceReservation(
                1,
                "reservation-1",
                "deployment-1",
                "cluster-1",
                "a".repeat(64),
                12,
                2049,
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                1,
                1,
                0,
                operatorEvidence,
                1,
                new Checksum(ChecksumType.SHA256, "d".repeat(64)),
                "/nereus/bookkeeper/namespace");
    }

    private static Path writeConfig(Path tempDir, String operatorEvidence) throws Exception {
        Path config = tempDir.resolve("admin.properties");
        Files.writeString(
                config,
                "cluster=cluster-1\n"
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
                        + "bookkeeper.deploymentId=deployment-1\n"
                        + "bookkeeper.providerScopeSha256=" + "a".repeat(64) + "\n"
                        + "bookkeeper.reservationId=reservation-1\n"
                        + "bookkeeper.digestType=CRC32C\n"
                        + "bookkeeper.passwordSecretRef=NEREUS_BK_PASSWORD\n"
                        + "bookkeeper.passwordIdentityVersion=v1\n"
                        + "operatorEvidenceSha256=" + operatorEvidence + "\n");
        return config;
    }
}
