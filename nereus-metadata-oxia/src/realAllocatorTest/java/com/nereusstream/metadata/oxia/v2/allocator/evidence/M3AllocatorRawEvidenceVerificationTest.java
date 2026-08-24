/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorSelectionReceiptV1;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class M3AllocatorRawEvidenceVerificationTest {
    @Test
    void recomputesNarsNaeaJunitAndExactSourceArtifacts() throws Exception {
        assertThat(codeSource(M3AllocatorRawEvidenceVerificationTest.class))
                .isEqualTo(Path.of(required("testedEvidenceArtifactPath")).toRealPath());
        assertThat(codeSource(AllocatorSelectionReceiptV1.class))
                .isEqualTo(Path.of(required("runtimeDomainArtifactPath")).toRealPath());
        Path output = Path.of(required("verificationPayload")).toAbsolutePath().normalize();
        M3AllocatorEvidenceVerifyMain.main(new String[] {
            required("outputDirectory"),
            required("oxiaClientJarPath"),
            required("testedEvidenceArtifactPath"),
            required("runtimeDomainArtifactPath"),
            required("runtimeMetadataSpiArtifactPath"),
            required("runtimeMetadataOxiaArtifactPath"),
            required("sourceLocksPath"),
            required("executorManifestPath"),
            output.toString()
        });
        String canonical = Files.readString(output);
        assertThat(canonical)
                .startsWith("{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_RAW_RECOMPUTATION_V1\"")
                .contains("\"status\":\"PASS_RAW_RECOMPUTED\"")
                .contains("\"intervals\":288")
                .contains("\"faultCutKinds\":" + AllocatorSelectionReceiptV1.REQUIRED_FAULT_CUTS)
                .contains("\"tests\":1,\"failures\":0,\"errors\":0,\"skips\":0");
    }

    private static Path codeSource(Class<?> type) throws Exception {
        return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI())
                .toRealPath();
    }

    private static String required(String suffix) {
        String value = System.getProperty("nereus.m3.allocator." + suffix);
        if (value == null || value.isBlank() || "UNSET".equals(value)) {
            throw new IllegalStateException("missing allocator raw verification property " + suffix);
        }
        return value;
    }
}
