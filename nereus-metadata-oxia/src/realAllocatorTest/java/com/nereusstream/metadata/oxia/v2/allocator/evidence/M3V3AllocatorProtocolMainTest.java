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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.ExecutionRecord;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationSealV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPlannerV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.DiagnosticAttestation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.DiagnosticScenario;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.FaultEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Plan;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3V3AllocatorProtocolMainTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void offlineCheckpointValidationAndNoneEvaluationSealingAreCreateNew() throws Exception {
        Fixture fixture = fixture();
        Path checkpoint = temporaryDirectory.resolve("campaign.nacp");
        Path evaluation = temporaryDirectory.resolve("evaluation.naev");
        Files.write(checkpoint, fixture.checkpointBytes().toByteArray());

        M3V3AllocatorProtocolMain.main(arguments("validate-checkpoint", checkpoint, null, fixture.source()));
        M3V3AllocatorProtocolMain.main(arguments("seal-evaluation", checkpoint, evaluation, fixture.source()));

        assertThat(AllocatorCampaignEvaluationSealV3.decode(CanonicalBytes.copyOf(Files.readAllBytes(evaluation)))
                        .status())
                .isEqualTo(
                        com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationV3.Status.NONE_QUALIFIED);
        assertThatThrownBy(() -> M3V3AllocatorProtocolMain.main(
                        arguments("seal-evaluation", checkpoint, evaluation, fixture.source())))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
    }

    @Test
    void diagnosticSealerRequiresTheCompleteZeroSkipJUnitInventory() throws Exception {
        SourceBinding source = source();
        Path junit = diagnosticJUnitDirectory("diagnostic");
        Path output = temporaryDirectory.resolve("diagnostic.nadv");

        M3V3AllocatorProtocolMain.main(arguments("seal-diagnostic", junit, output, source));
        M3V3AllocatorProtocolMain.main(arguments("validate-diagnostic", output, junit, source));

        DiagnosticAttestation diagnostic = AllocatorCampaignPromotionGateV3.decodeDiagnostic(
                CanonicalBytes.copyOf(Files.readAllBytes(output)));
        assertThat(diagnostic.source()).isEqualTo(source);
        assertThat(diagnostic.scenarios()).containsExactlyInAnyOrderElementsOf(EnumSet.allOf(DiagnosticScenario.class));

        SourceBinding scheduleOnlySource = new SourceBinding(
                source.nereusCommit(),
                source.oxiaImageDigest(),
                source.dependencyLockDigest(),
                source.executorDigest(),
                digest("schedule-only"));
        assertThatThrownBy(() -> M3V3AllocatorProtocolMain.main(arguments(
                        "seal-diagnostic",
                        junit,
                        temporaryDirectory.resolve("schedule-only.nadv"),
                        scheduleOnlySource)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("formal plan");

        Path first = junit.resolve("TEST-" + M3V3AsyncActorLaneRunnerTest.class.getName() + ".xml");
        Files.writeString(first, Files.readString(first).replaceFirst("/>", "><failure/></testcase>"));
        assertThatThrownBy(() -> M3V3AllocatorProtocolMain.main(arguments(
                        "seal-diagnostic", junit, temporaryDirectory.resolve("forged.nadv"), source)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("summary");

        Path mismatched = diagnosticJUnitDirectory("identity-mismatch");
        Path strict = mismatched.resolve(
                "TEST-" + M3RealAllocatorStrictIntervalDiagnosticTest.class.getName() + ".xml");
        Files.writeString(
                strict,
                Files.readString(strict)
                        .replace(
                                "replaysTheExactFormalSequenceWithoutUnexpectedWarmupFailure()",
                                "strictFixedThenDerivedFormalSchedulesDrainWithoutUnexpectedWarmupFailure()"));
        assertThatThrownBy(() -> M3V3AllocatorProtocolMain.main(arguments(
                        "seal-diagnostic", mismatched, temporaryDirectory.resolve("wrong-identity.nadv"), source)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inventory");
    }

    @Test
    void promotionCliWritesValidNonPromotableDecisionForNoneWithoutFailing() throws Exception {
        Fixture fixture = fixture();
        Path checkpoint = temporaryDirectory.resolve("campaign.nacp");
        Path evaluation = temporaryDirectory.resolve("evaluation.naev");
        Path diagnostic = temporaryDirectory.resolve("diagnostic.nadv");
        Path diagnosticJunit = diagnosticJUnitDirectory("promotion-diagnostic");
        Path formalJunit = temporaryDirectory.resolve("formal.xml");
        Path attachments = Files.createDirectory(temporaryDirectory.resolve("attachments"));
        Path decision = temporaryDirectory.resolve("decision.json");
        Files.write(checkpoint, fixture.checkpointBytes().toByteArray());
        Files.write(evaluation, AllocatorCampaignEvaluationSealV3.seal(fixture.checkpointBytes()).toByteArray());
        M3V3AllocatorProtocolMain.main(arguments("seal-diagnostic", diagnosticJunit, diagnostic, fixture.source()));
        Files.writeString(
                formalJunit,
                "<testsuite name=\"formal\" tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\">"
                        + "<testcase classname=\"formal\" name=\"formal\"/></testsuite>");
        for (PhysicalAttachment attachment : fixture.attachments()) {
            Files.write(attachments.resolve(attachment.name()), attachment.bytes());
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
        M3V3AllocatorProtocolMain.main(args.toArray(String[]::new));

        assertThat(Files.readString(decision))
                .contains("\"status\":\"NON_PROMOTABLE_EVALUATION\"")
                .contains("\"selectedCandidate\":\"NONE\"")
                .contains("\"diagnosticJUnitSha256\":\"")
                .contains("\"formalJUnitSha256\":\"");

        args.set(0, "seal-selection");
        args.set(7, temporaryDirectory.resolve("forbidden-selection.nars").toString());
        assertThatThrownBy(() -> M3V3AllocatorProtocolMain.main(args.toArray(String[]::new)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-promotable");

        PhysicalAttachment firstAttachment = fixture.attachments().get(0);
        Files.writeString(attachments.resolve(firstAttachment.name()), "tampered");
        args.set(0, "promotion-check");
        args.set(7, temporaryDirectory.resolve("tampered-attachment-decision.json").toString());
        assertThatThrownBy(() -> M3V3AllocatorProtocolMain.main(args.toArray(String[]::new)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("filename digest");
        Files.write(attachments.resolve(firstAttachment.name()), firstAttachment.bytes());

        Path tamperedDiagnostic = Files.createDirectory(temporaryDirectory.resolve("tampered-diagnostic"));
        Files.writeString(tamperedDiagnostic.resolve("TEST-formal.xml"), Files.readString(formalJunit));
        args.set(4, tamperedDiagnostic.toString());
        args.set(0, "promotion-check");
        args.set(7, temporaryDirectory.resolve("tampered-decision.json").toString());
        assertThatThrownBy(() -> M3V3AllocatorProtocolMain.main(args.toArray(String[]::new)))
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
        List<PhysicalAttachment> attachments = new ArrayList<>();
        Plan terminal = null;
        for (int guard = 0; guard < 400; guard++) {
            Plan plan = AllocatorCampaignPlannerV3.plan(observations);
            if (plan.completed()) {
                terminal = plan;
                break;
            }
            var required = plan.nextAction().orElseThrow();
            Observation observation = required instanceof ExecuteCell executeCell
                    ? executeCell.cell().candidate().nativePath()
                            ? passing(executeCell.cell(), executeCell.offeredRate())
                            : relativeFailure(executeCell.cell(), executeCell.offeredRate())
                    : passingFault(((ExecuteFaultRow) required).row());
            List<Sha256Digest> physicalDigests = new ArrayList<>();
            for (M3V3FormalCampaignPlan.PlannedActionV3 action :
                    M3V3FormalCampaignPlan.actionsFor(required, observations)) {
                byte[] bytes = ("physical-attachment-content-" + attachments.size())
                        .getBytes(StandardCharsets.UTF_8);
                Sha256Digest physicalDigest = Sha256Digest.hash(CanonicalBytes.copyOf(bytes));
                physicalDigests.add(physicalDigest);
                attachments.add(new PhysicalAttachment(physicalName(action, physicalDigest), bytes));
            }
            observations.add(observation);
            records.add(new ExecutionRecord(observation, aggregateDigest(physicalDigests)));
        }
        if (terminal == null) {
            throw new AssertionError("allocator V3 CLI fixture did not terminate");
        }
        SourceBinding source = source();
        AllocatorCampaignCheckpointV3 checkpoint = AllocatorCampaignCheckpointV3.initial(
                source,
                new RemainingBudgets(900, 5_400, 7_200, 5_400, 11_520, 1_440, 600),
                records,
                terminal.dispositions(),
                Status.COMPLETED);
        return new Fixture(
                source,
                AllocatorCampaignCheckpointV3.encode(checkpoint),
                List.copyOf(attachments));
    }

    private static String physicalName(
            M3V3FormalCampaignPlan.PlannedActionV3 action, Sha256Digest digest) {
        return switch (action.kind()) {
            case NATIVE_INTERVAL, CANDIDATE_INTERVAL ->
                "interval-" + action.cell().contextId() + '-' + digest.toHex() + ".json";
            case SCALE_ACTION -> "scale-" + action.row().candidate() + '-'
                    + action.row().activeManagedLedgers() + '-' + action.row().metadataLatencyP99Millis() + '-'
                    + digest.toHex() + ".json";
            case FAULT_ACTION -> "fault-" + action.row().candidate() + '-'
                    + action.row().activeManagedLedgers() + '-' + action.row().metadataLatencyP99Millis() + '-'
                    + action.faultCut() + '-' + digest.toHex() + ".nare1";
        };
    }

    private static Sha256Digest aggregateDigest(List<Sha256Digest> physicalDigests) {
        ByteBuffer bytes = ByteBuffer.allocate(Math.multiplyExact(physicalDigests.size(), Sha256Digest.LENGTH));
        physicalDigests.forEach(digest -> bytes.put(digest.bytes().toByteArray()));
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.array()));
    }

    private static IntervalEvidence passing(Cell cell, int offeredRate) {
        long offered = (long) offeredRate * AllocatorCampaignV3.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell,
                offeredRate,
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
                offeredRate,
                100_000,
                100_000,
                0,
                0,
                0);
    }

    private static IntervalEvidence relativeFailure(Cell cell, int offeredRate) {
        IntervalEvidence value = passing(cell, offeredRate);
        return new IntervalEvidence(
                cell,
                offeredRate,
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
                "c".repeat(40),
                digest("oxia"),
                digest("dependency"),
                digest("executor"),
                M3V3FormalCampaignPlan.zeroDecisionPlanDigest());
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private Path diagnosticJUnitDirectory(String name) throws Exception {
        Path directory = Files.createDirectory(temporaryDirectory.resolve(name));
        writeSuite(
                directory,
                M3RealAllocatorStrictIntervalDiagnosticTest.class,
                List.of(
                        "replaysTheExactFormalSequenceWithoutUnexpectedWarmupFailure()",
                        "replaysTheExactRange16ScaleThenFixedIntervalWithoutRetainingReservations()"));
        writeSuite(
                directory,
                M3V3AsyncActorLaneRunnerTest.class,
                List.of(
                        "evidenceAdmissionCapIsDerivedFromFrozenRateLatencyAndActorCount()",
                        "dispatcherReachesEveryFrozenOutstandingLevelWithoutBlockingOnCompletion()",
                        "controlledLatencyFuturesCoverFrozenAndDerivedRatesIncludingTwoHundredFiftyMillis()",
                        "twoHundredFiftyMillisAtOneThousandRpsReachesTheDerivedAsyncCap()",
                        "callbackReorderingStillProducesOneCanonicalTerminalPerOrdinal()",
                        "cutoffKeepsUndispatchedRequestsInThePreAdmissionDropPartition()",
                        "cleanupTimeoutClosesTheWorkflowGuardAndLateCompletionCannotDispatchNextOperation()",
                        "normalIntervalsSingleFlightBindingsWhileConflictProofRetainsSameKeyConcurrency()",
                        "everyFrozenRateRetainsOneOrdinalAuthoritativeMeasurementTransition()",
                        "scheduleRejectsWarmupAfterMeasurementAndRunnerContainsNoCorrectnessLockOrWorkerPool()",
                        "candidateWarmupLoadRejectionAllowsAdaptiveDescentButUnexpectedFailureDoesNot()"));
        writeSuite(
                directory,
                M3V3RealOxiaOperationDiagnosticTest.class,
                List.of("realOxiaOperationsRemainNonzeroAcrossEveryFrozenLatency()"));
        writeSuite(
                directory,
                M3V3AllocatorWorkflowDiagnosticTest.class,
                List.of(
                        "strictAndRangeRowsUseAsyncAdmissionAtTwoHundredAndFiveHundred()",
                        "fourActorSameCellConflictStormPreservesUniqueLedgerIds()"));
        writeSuite(
                directory,
                M3V3NativePathDiagnosticTest.class,
                List.of("formalAndDiagnosticUseOneNonBlockingRuntimeAndFrozenSchedule()"));
        writeSuite(
                directory,
                M3V3NativeBaselineCanaryTest.class,
                List.of("exactFormalScheduleClearsAllNativeBaselinesAndRepresentativeRows()"));
        return directory;
    }

    private static void writeSuite(Path directory, Class<?> suite, List<String> tests) throws Exception {
        StringBuilder xml = new StringBuilder("<testsuite name=\"")
                .append(suite.getName())
                .append("\" tests=\"")
                .append(tests.size())
                .append("\" failures=\"0\" errors=\"0\" skipped=\"0\">");
        tests.forEach(test -> xml.append("<testcase classname=\"")
                .append(suite.getName())
                .append("\" name=\"")
                .append(test)
                .append("\"/>"));
        xml.append("</testsuite>");
        Files.writeString(directory.resolve("TEST-" + suite.getName() + ".xml"), xml);
    }

    private record PhysicalAttachment(String name, byte[] bytes) {
        private PhysicalAttachment {
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }

    private record Fixture(
            SourceBinding source, CanonicalBytes checkpointBytes, List<PhysicalAttachment> attachments) {}
}
