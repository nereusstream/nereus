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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignFeasibilityV3;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.CampaignResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.CheckpointSink;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The separately authorized ADR-0108 bounded-adaptive V3 formal campaign entry. */
class M3V3BoundedAdaptiveFormalCampaignTest {
    @Test
    void executesOnlyValidatorPlannedPhysicalActions() throws Exception {
        AllocatorCampaignFeasibilityV3.requireFormalFeasible();
        String commit = required("nereus.m3.allocator.v3.formal.authorizedCommit");
        String expectedPlan = required("nereus.m3.allocator.v3.formal.zeroDecisionPlanSha256");
        assertThat(M3V3FormalCampaignPlan.zeroDecisionPlanDigest().toHex()).isEqualTo(expectedPlan);

        Path output = Path.of(required("nereus.m3.allocator.v3.formal.outputDirectory"))
                .toAbsolutePath()
                .normalize();
        assertThat(Files.isDirectory(output)).isTrue();
        try (var files = Files.list(output)) {
            assertThat(files).isEmpty();
        }
        Path checkpoints = Files.createDirectory(output.resolve("checkpoints"));
        long started = System.nanoTime();
        try (M3V3RealFormalActionRuntime runtime = new M3V3RealFormalActionRuntime(
                output,
                required("nereus.m3.allocator.v3.formal.oxiaServiceAddress"),
                commit.substring(0, 16))) {
            SourceBinding source = new SourceBinding(
                    commit,
                    digest(required("nereus.m3.allocator.v3.formal.oxiaImageDigest")),
                    digest(required("nereus.m3.allocator.v3.formal.dependencyLockSha256")),
                    digest(required("nereus.m3.allocator.v3.formal.executorSha256")),
                    M3V3FormalCampaignPlan.zeroDecisionPlanDigest());
            M3V3AdaptiveCampaignExecutor executor = new M3V3AdaptiveCampaignExecutor(
                    source,
                    new RemainingBudgets(900, 5_400, 7_200, 5_400, 13_120, 1_640, 600),
                    new M3V3FormalActionExecutorAdapter(runtime),
                    CheckpointSink.createNewDirectory(checkpoints),
                    M3V3AdaptiveCampaignExecutor.StopSignal.never(),
                    () -> elapsedSeconds(started) >= M3V3FormalCampaignPlan.CAMPAIGN_WALL_CLOCK_CAP_SECONDS);
            CampaignResult result = executor.start();
            AllocatorCampaignCheckpointV3 checkpoint = AllocatorCampaignCheckpointV3.decode(result.checkpointBytes());
            writeResult(output.resolve("campaign-result.json"), result, checkpoint);

            assertThat(result.status())
                    .withFailMessage(
                            "allocator V3 campaign failed: reason=%s detail=%s", result.reason(), result.detail())
                    .isEqualTo(Status.COMPLETED);
            assertThat(result.completed())
                    .withFailMessage(
                            "allocator V3 campaign did not complete: reason=%s detail=%s",
                            result.reason(), result.detail())
                    .isTrue();
        }
    }

    static void writeResult(
            Path output, CampaignResult result, AllocatorCampaignCheckpointV3 checkpoint) throws Exception {
        String json = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_EXECUTION_V3\","
                + "\"status\":\"" + result.status() + "\",\"terminalReason\":\"" + result.reason()
                + "\",\"terminalDetail\":" + jsonString(result.detail())
                + ",\"checkpointSequence\":" + checkpoint.checkpointSequence()
                + ",\"checkpointSha256\":\"" + AllocatorCampaignCheckpointV3.digest(result.checkpointBytes()).toHex()
                + "\",\"evaluationCreated\":false,\"selectionCreated\":false}\n";
        Files.writeString(
                output,
                json,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC);
    }

    static String jsonString(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 2).append('"');
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) character));
                    } else {
                        escaped.append(character);
                    }
                }
            }
        }
        return escaped.append('"').toString();
    }

    private static long elapsedSeconds(long started) {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started);
    }

    private static String required(String name) {
        String value = System.getProperty(name, "UNSET");
        if (value.isBlank() || value.equals("UNSET")) {
            throw new IllegalArgumentException("allocator V3 formal property is absent: " + name);
        }
        return value;
    }

    private static Sha256Digest digest(String hex) {
        return Sha256Digest.copyOf(HexFormat.of().parseHex(hex.replaceFirst("^sha256:", "")));
    }
}
