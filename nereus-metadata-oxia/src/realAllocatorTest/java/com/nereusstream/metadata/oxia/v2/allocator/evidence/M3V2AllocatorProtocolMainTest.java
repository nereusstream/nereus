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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.ExecutionRecord;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationSealV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPlannerV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.DiagnosticAttestation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.DiagnosticScenario;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.FaultEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Plan;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Row;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3V2AllocatorProtocolMainTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void offlineCheckpointValidationAndNoneEvaluationSealingAreCreateNew() throws Exception {
        Fixture fixture = fixture();
        Path checkpoint = temporaryDirectory.resolve("campaign.nacp");
        Path evaluation = temporaryDirectory.resolve("evaluation.naev");
        Files.write(checkpoint, fixture.checkpointBytes().toByteArray());

        M3V2AllocatorProtocolMain.main(arguments("validate-checkpoint", checkpoint, null, fixture.source()));
        M3V2AllocatorProtocolMain.main(arguments("seal-evaluation", checkpoint, evaluation, fixture.source()));

        assertThat(AllocatorCampaignEvaluationSealV2.decode(CanonicalBytes.copyOf(Files.readAllBytes(evaluation)))
                        .status())
                .isEqualTo(
                        com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationV2.Status.NONE_QUALIFIED);
        assertThatThrownBy(() -> M3V2AllocatorProtocolMain.main(
                        arguments("seal-evaluation", checkpoint, evaluation, fixture.source())))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
    }

    @Test
    void diagnosticSealerRequiresTheExactFourTestZeroSkipJUnitInventory() throws Exception {
        SourceBinding source = source();
        Path junit = temporaryDirectory.resolve("diagnostic.xml");
        Path output = temporaryDirectory.resolve("diagnostic.nadv");
        Files.writeString(junit, diagnosticJUnit());

        M3V2AllocatorProtocolMain.main(arguments("seal-diagnostic", junit, output, source));

        DiagnosticAttestation diagnostic = AllocatorCampaignPromotionGateV2.decodeDiagnostic(
                CanonicalBytes.copyOf(Files.readAllBytes(output)));
        assertThat(diagnostic.source()).isEqualTo(source);
        assertThat(diagnostic.scenarios()).containsExactlyInAnyOrderElementsOf(EnumSet.allOf(DiagnosticScenario.class));

        Files.writeString(
                junit,
                diagnosticJUnit().replace(
                        "<testcase name=\"strictWorkflowUsesRealOxia\"/>",
                        "<testcase name=\"strictWorkflowUsesRealOxia\"><failure/></testcase>"));
        assertThatThrownBy(() -> M3V2AllocatorProtocolMain.main(arguments(
                        "seal-diagnostic", junit, temporaryDirectory.resolve("forged.nadv"), source)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("summary");
    }

    @Test
    void promotionCliWritesValidNonPromotableDecisionForNoneWithoutFailing() throws Exception {
        Fixture fixture = fixture();
        Path checkpoint = temporaryDirectory.resolve("campaign.nacp");
        Path evaluation = temporaryDirectory.resolve("evaluation.naev");
        Path diagnostic = temporaryDirectory.resolve("diagnostic.nadv");
        Path diagnosticJunit = temporaryDirectory.resolve("diagnostic.xml");
        Path formalJunit = temporaryDirectory.resolve("formal.xml");
        Path attachments = Files.createDirectory(temporaryDirectory.resolve("attachments"));
        Path decision = temporaryDirectory.resolve("decision.json");
        Files.write(checkpoint, fixture.checkpointBytes().toByteArray());
        Files.write(evaluation, AllocatorCampaignEvaluationSealV2.seal(fixture.checkpointBytes()).toByteArray());
        Files.write(
                diagnostic,
                AllocatorCampaignPromotionGateV2.encodeDiagnostic(new DiagnosticAttestation(
                                fixture.source(),
                                EnumSet.allOf(DiagnosticScenario.class),
                                digest(diagnosticJUnit())))
                        .toByteArray());
        Files.writeString(diagnosticJunit, diagnosticJUnit());
        Files.writeString(
                formalJunit,
                "<testsuite tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\">"
                        + "<testcase name=\"formal\"/></testsuite>");
        for (int index = 0; index < fixture.attachmentBytes().size(); index++) {
            Files.write(attachments.resolve("attachment-" + index + ".bin"), fixture.attachmentBytes().get(index));
        }

        List<String> args = new ArrayList<>();
        args.add("promotion-check");
        args.add(evaluation.toString());
        args.add(checkpoint.toString());
        args.add(diagnostic.toString());
        args.add(diagnosticJunit.toString());
        args.add(formalJunit.toString());
        args.add(attachments.toString());
        args.add(decision.toString());
        args.addAll(sourceArguments(fixture.source()));
        M3V2AllocatorProtocolMain.main(args.toArray(String[]::new));

        assertThat(Files.readString(decision))
                .contains("\"status\":\"NON_PROMOTABLE_EVALUATION\"")
                .contains("\"selectedCandidate\":\"NONE\"")
                .contains("\"diagnosticJUnitSha256\":\"")
                .contains("\"formalJUnitSha256\":\"");

        Files.writeString(diagnosticJunit, Files.readString(formalJunit));
        args.set(7, temporaryDirectory.resolve("tampered-decision.json").toString());
        assertThatThrownBy(() -> M3V2AllocatorProtocolMain.main(args.toArray(String[]::new)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("diagnostic JUnit");
    }

    private static String[] arguments(String command, Path input, Path output, SourceBinding source) {
        List<String> args = new ArrayList<>();
        args.add(command);
        args.add(input.toString());
        if (output != null) {
            args.add(output.toString());
        }
        args.addAll(sourceArguments(source));
        return args.toArray(String[]::new);
    }

    private static List<String> sourceArguments(SourceBinding source) {
        return List.of(
                source.nereusCommit(),
                source.oxiaImageDigest().toHex(),
                source.dependencyLockDigest().toHex(),
                source.executorDigest().toHex(),
                source.workloadDigest().toHex());
    }

    private static Fixture fixture() {
        List<Observation> observations = new ArrayList<>();
        List<ExecutionRecord> records = new ArrayList<>();
        List<byte[]> attachments = new ArrayList<>();
        Plan terminal = null;
        for (int guard = 0; guard < 400; guard++) {
            Plan plan = AllocatorCampaignPlannerV2.plan(observations);
            if (plan.completed()) {
                terminal = plan;
                break;
            }
            Observation observation = plan.nextAction().orElseThrow() instanceof ExecuteCell executeCell
                    ? executeCell.cell().candidate().nativePath()
                            ? passing(executeCell.cell())
                            : relativeFailure(executeCell.cell())
                    : passingFault(((ExecuteFaultRow) plan.nextAction().orElseThrow()).row());
            byte[] attachment = ("attachment-content-" + records.size()).getBytes(StandardCharsets.UTF_8);
            observations.add(observation);
            records.add(new ExecutionRecord(observation, Sha256Digest.hash(CanonicalBytes.copyOf(attachment))));
            attachments.add(attachment);
        }
        if (terminal == null) {
            throw new AssertionError("allocator V2 CLI fixture did not terminate");
        }
        SourceBinding source = source();
        AllocatorCampaignCheckpointV2 checkpoint = AllocatorCampaignCheckpointV2.initial(
                source,
                new RemainingBudgets(900, 5_400, 7_200, 5_400, 11_520, 1_440, 600),
                records,
                terminal.dispositions(),
                Status.COMPLETED);
        return new Fixture(
                source,
                AllocatorCampaignCheckpointV2.encode(checkpoint),
                List.copyOf(attachments));
    }

    private static IntervalEvidence passing(Cell cell) {
        long offered = (long) cell.offeredRolloverRequestsPerSecond() * AllocatorCampaignV2.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell,
                offered,
                offered,
                0,
                offered,
                0,
                0,
                offered,
                0,
                0,
                0,
                0,
                0,
                100_000,
                100_000,
                100_000,
                cell.offeredRolloverRequestsPerSecond(),
                100_000,
                100_000,
                0,
                0,
                0);
    }

    private static IntervalEvidence relativeFailure(Cell cell) {
        IntervalEvidence value = passing(cell);
        return new IntervalEvidence(
                cell,
                value.offered(),
                value.admitted(),
                0,
                value.completed(),
                0,
                0,
                value.terminal(),
                0,
                0,
                0,
                0,
                0,
                value.rolloverP99Micros(),
                value.oxiaOperationP99Micros(),
                value.queueAgeP99Micros(),
                value.queueDepthMaximum(),
                value.starvationMaximumMicros(),
                400_001,
                0,
                0,
                0);
    }

    private static FaultEvidence passingFault(Row row) {
        return new FaultEvidence(row, EnumSet.allOf(AllocatorFaultCutV1.class), 0, 0, 0, 0, 0, 0, 0, 0, 1, 1_000_000);
    }

    private static SourceBinding source() {
        return new SourceBinding(
                "c".repeat(40), digest("oxia"), digest("dependency"), digest("executor"), digest("workload"));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static String diagnosticJUnit() {
        return "<testsuite tests=\"4\" failures=\"0\" errors=\"0\" skipped=\"0\">"
                + "<testcase name=\"strictWorkflowUsesRealOxia\"/>"
                + "<testcase name=\"installedRangeReusesGrant\"/>"
                + "<testcase name=\"rangeRenewalUsesCellCas\"/>"
                + "<testcase name=\"conflictStormUsesFourIndependentCoordinators\"/>"
                + "</testsuite>";
    }

    private record Fixture(SourceBinding source, CanonicalBytes checkpointBytes, List<byte[]> attachmentBytes) {}
}
