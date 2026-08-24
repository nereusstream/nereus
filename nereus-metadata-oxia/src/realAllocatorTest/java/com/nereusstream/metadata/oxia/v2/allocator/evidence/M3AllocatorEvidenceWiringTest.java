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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceAttachmentKindV1;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3AllocatorEvidenceWiringTest {
    private static final Pattern SELF_SHA = Pattern.compile("\\\"selfSha256\\\":\\\"([0-9a-f]{64})\\\"");

    @Test
    void freezesTheExactFiveFileNaeaInventoryAndFailClosedSealEntrypoint() {
        assertThat(Arrays.stream(AllocatorEvidenceAttachmentKindV1.values())
                        .map(AllocatorEvidenceAttachmentKindV1::fileName))
                .containsExactly(
                        "test.naea",
                        "native.naea",
                        "fault.naea",
                        "scale-10000.naea",
                        "scale-100000.naea");
        assertThatThrownBy(() -> M3AllocatorEvidenceSealMain.main(new String[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected output");
    }

    @Test
    void sealsOnlyTheExactVerifierTestcaseAndUsesTheFrozenZeroedSelfHash(@TempDir Path temporary)
            throws Exception {
        Path raw = temporary.resolve("raw-verification-payload.json");
        Files.writeString(raw, rawVerificationJson());
        Path junit = temporary.resolve(M3AllocatorVerificationSealMain.TEST_XML);
        Files.writeString(junit, junitXml("0"));
        Path sealed = temporary.resolve("raw-verification.json");
        M3AllocatorVerificationSealMain.main(
                new String[] {raw.toString(), junit.toString(), sealed.toString()});

        byte[] exact = Files.readAllBytes(sealed);
        Matcher matcher = SELF_SHA.matcher(new String(exact, StandardCharsets.UTF_8));
        assertThat(matcher.find()).isTrue();
        String observed = matcher.group(1);
        byte[] zeroed = new String(exact, StandardCharsets.UTF_8)
                .replaceFirst(observed, "0".repeat(64))
                .getBytes(StandardCharsets.UTF_8);
        assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(zeroed)))
                .isEqualTo(observed);

        Path skippedDirectory = temporary.resolve("skipped");
        Files.createDirectories(skippedDirectory);
        Path skipped = skippedDirectory.resolve(M3AllocatorVerificationSealMain.TEST_XML);
        Files.writeString(skipped, junitXml("1"));
        assertThatThrownBy(() -> M3AllocatorVerificationSealMain.main(new String[] {
                    raw.toString(), skipped.toString(), skippedDirectory.resolve("raw-verification.json").toString()
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1/0/0/0");

        Path tamperedDirectory = temporary.resolve("tampered");
        Files.createDirectories(tamperedDirectory);
        Path tamperedRaw = tamperedDirectory.resolve("raw-verification-payload.json");
        Files.writeString(tamperedRaw, Files.readString(raw).replace("\"note\":\"A\"", "\"note\":\"B\""));
        Path exactJUnit = tamperedDirectory.resolve(M3AllocatorVerificationSealMain.TEST_XML);
        Files.writeString(exactJUnit, junitXml("0"));
        assertThatThrownBy(() -> M3AllocatorVerificationSealMain.main(new String[] {
                    tamperedRaw.toString(),
                    exactJUnit.toString(),
                    tamperedDirectory.resolve("raw-verification.json").toString()
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("self hash differs");
    }

    private static String rawVerificationJson() throws Exception {
        String zeroed = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_RAW_RECOMPUTATION_V1\","
                + "\"selfSha256\":\""
                + "0".repeat(64)
                + "\",\"selfHashRule\":\""
                + M3AllocatorEvidenceVerifyMain.SELF_HASH_RULE
                + "\",\"status\":\"PASS_RAW_RECOMPUTED\",\"note\":\"A\"}\n";
        String digest = HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(zeroed.getBytes(StandardCharsets.UTF_8)));
        return zeroed.replaceFirst("0{64}", digest);
    }

    private static String junitXml(String skipped) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<testsuite name=\"exact\" tests=\"1\" skipped=\""
                + skipped
                + "\" failures=\"0\" errors=\"0\">\n"
                + "  <testcase name=\""
                + M3AllocatorVerificationSealMain.TEST_CASE
                + "\" classname=\""
                + M3AllocatorVerificationSealMain.TEST_CLASS
                + "\"/>\n"
                + "</testsuite>\n";
    }
}
