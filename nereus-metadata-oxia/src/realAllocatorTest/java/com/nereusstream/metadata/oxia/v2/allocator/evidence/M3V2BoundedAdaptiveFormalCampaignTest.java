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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.Status;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AdaptiveCampaignExecutor.CampaignResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AdaptiveCampaignExecutor.CheckpointSink;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The single explicitly authorized ADR-0104 bounded-adaptive formal campaign entry. */
class M3V2BoundedAdaptiveFormalCampaignTest {
    @Test
    void executesOnlyValidatorPlannedPhysicalActions() throws Exception {
        String commit = required("nereus.m3.allocator.v2.formal.authorizedCommit");
        String expectedPlan = required("nereus.m3.allocator.v2.formal.zeroDecisionPlanSha256");
        assertThat(M3V2FormalCampaignPlan.zeroDecisionPlanDigest().toHex()).isEqualTo(expectedPlan);

        Path output = Path.of(required("nereus.m3.allocator.v2.formal.outputDirectory"))
                .toAbsolutePath()
                .normalize();
        assertThat(Files.isDirectory(output)).isTrue();
        try (var files = Files.list(output)) {
            assertThat(files).isEmpty();
        }
        Path checkpoints = Files.createDirectory(output.resolve("checkpoints"));
        long started = System.nanoTime();
        try (M3V2RealFormalActionRuntime runtime = new M3V2RealFormalActionRuntime(
                output,
                required("nereus.m3.allocator.v2.formal.oxiaServiceAddress"),
                commit.substring(0, 16))) {
            SourceBinding source = new SourceBinding(
                    commit,
                    digest(required("nereus.m3.allocator.v2.formal.oxiaImageDigest")),
                    digest(required("nereus.m3.allocator.v2.formal.dependencyLockSha256")),
                    digest(required("nereus.m3.allocator.v2.formal.executorSha256")),
                    M3V2FormalCampaignPlan.zeroDecisionPlanDigest());
            M3V2AdaptiveCampaignExecutor executor = new M3V2AdaptiveCampaignExecutor(
                    source,
                    new RemainingBudgets(900, 5_400, 7_200, 5_400, 11_520, 1_440, 600),
                    new M3V2FormalActionExecutorAdapter(runtime),
                    CheckpointSink.createNewDirectory(checkpoints),
                    M3V2AdaptiveCampaignExecutor.StopSignal.never(),
                    () -> elapsedSeconds(started) >= M3V2FormalCampaignPlan.CAMPAIGN_WALL_CLOCK_CAP_SECONDS);
            CampaignResult result = executor.start();
            AllocatorCampaignCheckpointV2 checkpoint = AllocatorCampaignCheckpointV2.decode(result.checkpointBytes());
            writeResult(output.resolve("campaign-result.json"), result, checkpoint);

            assertThat(result.status()).isEqualTo(Status.COMPLETED);
            assertThat(result.completed()).isTrue();
        }
    }

    private static void writeResult(
            Path output, CampaignResult result, AllocatorCampaignCheckpointV2 checkpoint) throws Exception {
        String json = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_EXECUTION_V2\","
                + "\"status\":\"" + result.status() + "\",\"terminalReason\":\"" + result.reason()
                + "\",\"checkpointSequence\":" + checkpoint.checkpointSequence()
                + ",\"checkpointSha256\":\"" + AllocatorCampaignCheckpointV2.digest(result.checkpointBytes()).toHex()
                + "\",\"evaluationCreated\":false,\"selectionCreated\":false}\n";
        Files.writeString(
                output,
                json,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC);
    }

    private static long elapsedSeconds(long started) {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started);
    }

    private static String required(String name) {
        String value = System.getProperty(name, "UNSET");
        if (value.isBlank() || value.equals("UNSET")) {
            throw new IllegalArgumentException("allocator V2 formal property is absent: " + name);
        }
        return value;
    }

    private static Sha256Digest digest(String hex) {
        return Sha256Digest.copyOf(HexFormat.of().parseHex(hex.replaceFirst("^sha256:", "")));
    }
}
