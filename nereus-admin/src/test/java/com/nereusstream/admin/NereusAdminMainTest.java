/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusAdminMainTest {

    @Test
    void classifiesUnknownCommandAsInvalidArguments(
            @TempDir Path tempDir) throws Exception {
        Path config = Files.writeString(
                tempDir.resolve("admin.properties"),
                "cluster=test\n");

        assertThat(NereusAdminMain.run(new String[]{
                "bookkeeper", "namespace", "unknown",
                "--config", config.toString()
        })).isEqualTo(AdminExitCode.INVALID_ARGUMENT);
    }

    @Test
    void classifiesMalformedPropertiesAsConfigurationError(
            @TempDir Path tempDir) throws Exception {
        Path config = Files.writeString(
                tempDir.resolve("admin.properties"),
                "cluster=test\n");

        assertThat(NereusAdminMain.run(new String[]{
                "bookkeeper", "namespace", "verify",
                "--config", config.toString()
        })).isEqualTo(AdminExitCode.CONFIGURATION_ERROR);
    }
}
