/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPlanProfileV4;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV4;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3V4AllocatorProtocolMainTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void nativeBaselineRowsPreserveV3IdentityAndUseV4IdentityForTheDrainRuntime() {
        assertThat(M3V3NativeBaselineCanaryTest.rowSchema(false))
                .isEqualTo("NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_V3");
        assertThat(M3V3NativeBaselineCanaryTest.rowSchema(true))
                .isEqualTo("NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_V4");
    }

    @Test
    void diagnosticSealerBindsTheIndependentTwentyThreeTestNadv4Inventory() throws Exception {
        SourceBinding source = source();
        Path junit = diagnosticJUnitDirectory();
        Path output = temporaryDirectory.resolve("diagnostic.nadv4");

        M3V4AllocatorProtocolMain.main(arguments("seal-diagnostic", junit, output, source));
        M3V4AllocatorProtocolMain.main(arguments("validate-diagnostic", output, junit, source));

        var diagnostic = AllocatorCampaignPromotionGateV4.decodeDiagnostic(
                CanonicalBytes.copyOf(Files.readAllBytes(output)));
        assertThat(M3V4AllocatorProtocolMain.DIAGNOSTIC_SUITES).hasSize(9);
        assertThat(M3V4AllocatorProtocolMain.DIAGNOSTIC_TESTS).hasSize(23);
        assertThat(diagnostic.source()).isEqualTo(source);
        assertThat(diagnostic.scenarios())
                .containsExactlyInAnyOrderElementsOf(java.util.EnumSet.allOf(
                        AllocatorCampaignPromotionGateV4.DiagnosticScenario.class));
        assertThatThrownBy(() -> AllocatorCampaignPromotionGateV3.decodeDiagnostic(
                        CanonicalBytes.copyOf(Files.readAllBytes(output))))
                .isInstanceOf(IllegalArgumentException.class);

        Path v4Runner = junit.resolve("TEST-" + M3V4AsyncActorLaneRunnerTest.class.getName() + ".xml");
        Files.writeString(v4Runner, Files.readString(v4Runner).replaceFirst("/>", "><failure/></testcase>"));
        assertThatThrownBy(() -> M3V4AllocatorProtocolMain.main(arguments(
                        "seal-diagnostic",
                        junit,
                        temporaryDirectory.resolve("forged.nadv4"),
                        source)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("summary");
    }

    private Path diagnosticJUnitDirectory() throws Exception {
        Path directory = Files.createDirectory(temporaryDirectory.resolve("diagnostic-junit"));
        Map<String, List<String>> testsBySuite = M3V4AllocatorProtocolMain.DIAGNOSTIC_TESTS.stream()
                .map(identity -> identity.split("#", 2))
                .collect(Collectors.groupingBy(
                        values -> values[0],
                        Collectors.mapping(values -> values[1], Collectors.toList())));
        for (String suite : M3V4AllocatorProtocolMain.DIAGNOSTIC_SUITES.stream().sorted().toList()) {
            List<String> tests = testsBySuite.getOrDefault(suite, List.of()).stream()
                    .sorted(Comparator.naturalOrder())
                    .toList();
            StringBuilder xml = new StringBuilder("<testsuite name=\"")
                    .append(suite)
                    .append("\" tests=\"")
                    .append(tests.size())
                    .append("\" failures=\"0\" errors=\"0\" skipped=\"0\">");
            tests.forEach(test -> xml.append("<testcase classname=\"")
                    .append(suite)
                    .append("\" name=\"")
                    .append(test)
                    .append("\"/>"));
            xml.append("</testsuite>");
            Files.writeString(directory.resolve("TEST-" + suite + ".xml"), xml);
        }
        return directory;
    }

    private static String[] arguments(String command, Path input, Path output, SourceBinding source) {
        List<String> args = new ArrayList<>();
        args.add(command);
        args.add(input.toString());
        args.add(output.toString());
        args.add(source.nereusCommit());
        args.add(source.oxiaImageDigest().toHex());
        args.add(source.dependencyLockDigest().toHex());
        args.add(source.executorDigest().toHex());
        args.add(source.workloadDigest().toHex());
        return args.toArray(String[]::new);
    }

    private static SourceBinding source() {
        return new SourceBinding(
                "d".repeat(40),
                digest("oxia"),
                digest("dependency"),
                digest("executor"),
                AllocatorCampaignPlanProfileV4.zeroDecisionPlanDigest());
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
