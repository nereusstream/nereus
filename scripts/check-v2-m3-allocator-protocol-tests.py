#!/usr/bin/env python3
"""Offline fail-closed contract tests for the ADR-0104 allocator entrypoints."""

from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parent.parent
RUNNER = ROOT / "scripts" / "run-v2-m3-real-allocator-evidence.sh"
SETTINGS = ROOT / "settings.gradle.kts"
ROOT_BUILD = ROOT / "build.gradle.kts"
MODULE_BUILD = ROOT / "nereus-metadata-oxia" / "build.gradle.kts"
HARD_DEADLINE = ROOT / "scripts" / "run-v2-m3-with-hard-deadline.py"
FORMAL_ARCHIVER = ROOT / "scripts" / "archive-v2-m3-allocator-formal.py"
FAILED_FORMAL_ARCHIVER = ROOT / "scripts" / "archive-v2-m3-allocator-failed-formal.py"
PROMOTION_INVALID_FORMAL_ARCHIVER = (
    ROOT / "scripts" / "archive-v2-m3-allocator-promotion-invalid-formal.py"
)
SELECTED_FORMAL_ARCHIVER = (
    ROOT / "scripts" / "archive-v2-m3-allocator-selected-formal.py"
)
DIAGNOSTIC_ARCHIVER = ROOT / "scripts" / "archive-v2-m3-allocator-diagnostic.py"
V3_PLAN = ROOT / "scripts" / "v2-m3-allocator-plan-v3.py"
V4_PLAN = ROOT / "scripts" / "v2-m3-allocator-plan-v4.py"
V4_LAUNCHER = ROOT / "scripts" / "run-v2-m3-real-allocator-evidence-v4.sh"
V5_PLAN = ROOT / "scripts" / "v2-m3-allocator-plan-v5.py"
V5_LAUNCHER = ROOT / "scripts" / "run-v2-m3-real-allocator-evidence-v5.sh"
V3_FORMAL_RUNTIME = (
    ROOT
    / "nereus-metadata-oxia"
    / "src"
    / "realAllocatorTest"
    / "java"
    / "com"
    / "nereusstream"
    / "metadata"
    / "oxia"
    / "v2"
    / "allocator"
    / "evidence"
    / "M3V3RealFormalActionRuntime.java"
)
V3_NATIVE_RUNTIME = V3_FORMAL_RUNTIME.with_name("M3V3NativeIntervalRuntime.java")
V3_CANDIDATE_POPULATION = V3_FORMAL_RUNTIME.with_name("M3CandidateAllocatorPopulation.java")
V3_ASYNC_RUNNER = V3_FORMAL_RUNTIME.with_name("M3V3AsyncActorLaneRunner.java")
V3_FORMAL_HARNESS = V3_FORMAL_RUNTIME.with_name("M3V3AllocatorFormalHarness.java")
REAL_OXIA_ACTORS = V3_FORMAL_RUNTIME.with_name("M3RealOxiaActors.java")
EVIDENCE_ALLOCATOR_STORE = V3_FORMAL_RUNTIME.with_name("M3EvidenceAllocatorStore.java")
V3_STRICT_SEQUENCE_DIAGNOSTIC = V3_FORMAL_RUNTIME.with_name(
    "M3RealAllocatorStrictIntervalDiagnosticTest.java"
)
V3_CANDIDATE_CUTOFF_DIAGNOSTIC = V3_FORMAL_RUNTIME.with_name(
    "M3V3CandidateCutoffDiagnosticTest.java"
)
V3_PROTOCOL_MAIN = V3_FORMAL_RUNTIME.with_name("M3V3AllocatorProtocolMain.java")
V4_PROTOCOL_MAIN = V3_FORMAL_RUNTIME.with_name("M3V4AllocatorProtocolMain.java")
V5_PROTOCOL_MAIN = V3_FORMAL_RUNTIME.with_name("M3V5AllocatorProtocolMain.java")
V4_RUNNER_TEST = V3_FORMAL_RUNTIME.with_name("M3V4AsyncActorLaneRunnerTest.java")
V5_RUNNER_TEST = V3_FORMAL_RUNTIME.with_name("M3V5AsyncActorLaneRunnerTest.java")
V4_TERMINAL_DIAGNOSTIC = V3_FORMAL_RUNTIME.with_name(
    "M3V4TerminalAdmissionDrainDiagnosticTest.java"
)
V4_RANGE_LATENCY_DIAGNOSTIC = V3_FORMAL_RUNTIME.with_name(
    "M3V4RangeLatencyDiagnosticTest.java"
)
V5_TERMINAL_DIAGNOSTIC = V3_FORMAL_RUNTIME.with_name(
    "M3V5TerminalAdmissionDrainDiagnosticTest.java"
)
V5_RANGE_LATENCY_DIAGNOSTIC = V3_FORMAL_RUNTIME.with_name(
    "M3V5RangeLatencyDiagnosticTest.java"
)
V5_RAW_GATE = V3_FORMAL_RUNTIME.with_name("M3V5DiagnosticRawGate.java")
V3_OPERATION_DIAGNOSTIC = V3_FORMAL_RUNTIME.with_name(
    "M3V3RealOxiaOperationDiagnosticTest.java"
)
ALLOCATOR_WIRING_TEST = V3_FORMAL_RUNTIME.with_name("M3AllocatorEvidenceWiringTest.java")
V4_FORMAL_CAMPAIGN = V3_FORMAL_RUNTIME.with_name("M3V4BoundedAdaptiveFormalCampaignTest.java")
V5_FORMAL_CAMPAIGN = V3_FORMAL_RUNTIME.with_name("M3V5BoundedAdaptiveFormalCampaignTest.java")
PRODUCTION_ALLOCATOR = (
    ROOT
    / "nereus-metadata-spi"
    / "src"
    / "main"
    / "java"
    / "com"
    / "nereusstream"
    / "metadata"
    / "spi"
    / "allocator"
    / "ProductionVirtualLedgerAllocator.java"
)
BOUNDED_WORKFLOW = PRODUCTION_ALLOCATOR.with_name("BoundedVirtualLedgerAllocatorWorkflowV2.java")
BOUNDED_WORKFLOW_TEST = (
    ROOT
    / "nereus-metadata-spi"
    / "src"
    / "test"
    / "java"
    / "com"
    / "nereusstream"
    / "metadata"
    / "spi"
    / "allocator"
    / "BoundedVirtualLedgerAllocatorWorkflowV2Test.java"
)
CONDITIONAL_MUTATION_ENGINE = (
    ROOT
    / "nereus-metadata-oxia"
    / "src"
    / "main"
    / "java"
    / "com"
    / "nereusstream"
    / "metadata"
    / "oxia"
    / "v2"
    / "mutation"
    / "ConditionalMutationEngine.java"
)
OXIA_CONDITIONAL_CLIENT = CONDITIONAL_MUTATION_ENGINE.with_name("AsyncOxiaConditionalClient.java")
OXIA_ALLOCATOR_STORE = CONDITIONAL_MUTATION_ENGINE.parents[1] / "allocator" / "OxiaVirtualLedgerAllocatorStore.java"
ALLOCATOR_STORE = PRODUCTION_ALLOCATOR.with_name("PulsarVirtualLedgerAllocatorStore.java")
FORMAL_CAMPAIGN = (
    ROOT
    / "nereus-metadata-oxia"
    / "src"
    / "realAllocatorTest"
    / "java"
    / "com"
    / "nereusstream"
    / "metadata"
    / "oxia"
    / "v2"
    / "allocator"
    / "evidence"
    / "M3V2BoundedAdaptiveFormalCampaignTest.java"
)


class M3AllocatorProtocolConfigurationTest(unittest.TestCase):
    def test_v5_physical_attachment_cap_is_versioned_without_changing_v3_v4(self) -> None:
        protocol = V3_PROTOCOL_MAIN.read_text()
        v4_protocol = V4_PROTOCOL_MAIN.read_text()
        v5_protocol = V5_PROTOCOL_MAIN.read_text()
        self.assertIn("LEGACY_PHYSICAL_ATTACHMENT_MAX_BYTES = 16 * 1024 * 1024", protocol)
        self.assertIn("V5_PHYSICAL_ATTACHMENT_MAX_BYTES = 32 * 1024 * 1024", protocol)
        self.assertIn("readRegular(path, physicalAttachmentMaxBytes(protocolVersion))", protocol)
        self.assertIn('case "V3", "V4" -> LEGACY_PHYSICAL_ATTACHMENT_MAX_BYTES', protocol)
        self.assertIn('case "V5" -> V5_PHYSICAL_ATTACHMENT_MAX_BYTES', protocol)
        self.assertIn('Path.of(args[6]), "V4"', v4_protocol)
        self.assertIn('Path.of(args[6]), "V5"', v5_protocol)
        self.assertTrue(PROMOTION_INVALID_FORMAL_ARCHIVER.is_file())

    def test_v4_installed_range_fast_path_reuses_only_exact_store_and_mutation_authority(self) -> None:
        allocator = PRODUCTION_ALLOCATOR.read_text()
        workflow = BOUNDED_WORKFLOW.read_text()
        workflow_test = BOUNDED_WORKFLOW_TEST.read_text()
        mutation_engine = CONDITIONAL_MUTATION_ENGINE.read_text()
        real_oxia_actors = REAL_OXIA_ACTORS.read_text()
        evidence_allocator_store = EVIDENCE_ALLOCATOR_STORE.read_text()
        conditional_client = OXIA_CONDITIONAL_CLIENT.read_text()
        oxia_store = OXIA_ALLOCATOR_STORE.read_text()
        allocator_store = ALLOCATOR_STORE.read_text()

        self.assertIn("createCandidateAfterStoreObservedRangeAuthorities", allocator)
        self.assertIn("publishCandidateAfterStoreObservedRangeNode", allocator)
        self.assertIn("installRangeReservedGrantAfterStoreObservedAuthorities", allocator)
        self.assertIn("clearRangeReservationAfterStoreObservedInstalledHead", allocator)
        self.assertIn("store-observed candidate fast path requires RANGE mode", allocator)
        self.assertIn("store-observed publish fast path requires RANGE mode", allocator)
        self.assertIn("canUseIndependentInstalledRangeGrant(authorities, reservation)", workflow)
        self.assertLess(
            workflow.index("canUseIndependentInstalledRangeGrant(authorities, reservation)"),
            workflow.index("if (reservation.isPresent())"),
        )
        self.assertIn(
            "createCandidate(request, state, authorities.cell(), authorities.head(), true)",
            workflow,
        )
        self.assertIn("installRangeReservedGrantAfterStoreObservedAuthorities", workflow)
        self.assertIn("clearRangeReservationAfterStoreObservedInstalledHead", workflow)
        self.assertIn("return createCandidate(request, state, cell, head, true)", workflow)
        self.assertIn("allocator.installRangeReservedGrant(", workflow)
        self.assertIn("allocator.clearReservation(", workflow)
        self.assertIn("case DEFINITIVE_CONFLICT", workflow)
        self.assertIn("reconcilePublishedHead", workflow)
        self.assertIn(
            "installedRangeWorkflowDispatchesInitialAuthoritiesTogetherAndReusesExactMutationResults",
            workflow_test,
        )
        self.assertIn("directAllocatorApiRetainsIndependentCellHeadAndNodeProofReads", workflow_test)
        self.assertIn("installedRangeGrantProceedsPastAnotherHeadsCellReservation", workflow_test)
        self.assertIn("installedRangeGrantStillWaitsForItsOwnUnclearedReservation", workflow_test)
        self.assertIn("rangeWithoutUsableGrantStillWaitsForAnotherHeadsCellReservation", workflow_test)
        self.assertIn("createNodeAfterStoreObservedRangeAuthorities", allocator_store)
        self.assertIn("compareAndSetHeadAfterStoreObservedRangeNode", allocator_store)
        self.assertIn("createUsingAcknowledgedSuccess", oxia_store)
        self.assertIn("compareAndSetUsingAcknowledgedSuccess", oxia_store)
        self.assertIn("createIfAbsentAcknowledged", conditional_client)
        self.assertIn("compareAndSetAcknowledged", conditional_client)
        self.assertIn("Oxia conditional mutation returned a different authority key", conditional_client)
        self.assertIn("createUsingAcknowledgedSuccess", mutation_engine)
        self.assertIn("compareAndSetUsingAcknowledgedSuccess", mutation_engine)
        self.assertIn("exactAcknowledgement", mutation_engine)
        self.assertIn("mutationAttempt(() -> client.createIfAbsent", mutation_engine)
        self.assertIn("mutationAttempt(() -> client.compareAndSet", mutation_engine)
        self.assertGreaterEqual(mutation_engine.count("thenCompose(attempt -> reread(key)"), 2)
        self.assertGreaterEqual(mutation_engine.count("return reread(key).thenApply"), 2)
        self.assertGreaterEqual(real_oxia_actors.count("createIfAbsentAcknowledged"), 4)
        self.assertGreaterEqual(real_oxia_actors.count("compareAndSetAcknowledged"), 4)
        self.assertIn("private <T> CompletionStage<T> mutation", real_oxia_actors)
        self.assertIn("private <T> CompletionStage<T> hold", real_oxia_actors)
        self.assertIn("createNodeAfterStoreObservedRangeAuthorities", evidence_allocator_store)
        self.assertIn("compareAndSetHeadAfterStoreObservedRangeNode", evidence_allocator_store)
        self.assertIn(
            "delegate.createNodeAfterStoreObservedRangeAuthorities",
            evidence_allocator_store,
        )
        self.assertIn(
            "delegate.compareAndSetHeadAfterStoreObservedRangeNode",
            evidence_allocator_store,
        )
        self.assertIn("beginRetryDiagnosticCapture", V3_CANDIDATE_POPULATION.read_text())
        self.assertIn("endRetryDiagnosticCapture", V3_CANDIDATE_POPULATION.read_text())
        self.assertIn('\\"retryReasons\\"', V4_RANGE_LATENCY_DIAGNOSTIC.read_text())
        self.assertIn("CONTROLLED_DELAY_SCHEDULER_THREADS_PER_ACTOR = 4", real_oxia_actors)
        self.assertIn("Executors.newScheduledThreadPool(", real_oxia_actors)
        self.assertNotIn("newSingleThreadScheduledExecutor", real_oxia_actors)
        self.assertIn(
            "controlledLatencySchedulerAdvancesFourDueCompletionsConcurrently",
            ALLOCATOR_WIRING_TEST.read_text(),
        )
        self.assertIn(
            "M3RealOxiaActors.CONTROLLED_DELAY_SCHEDULER_THREADS_PER_ACTOR",
            V4_RANGE_LATENCY_DIAGNOSTIC.read_text(),
        )
        self.assertIn(
            "M3RealOxiaActors.CONTROLLED_DELAY_SCHEDULER_THREADS_PER_ACTOR",
            V3_OPERATION_DIAGNOSTIC.read_text(),
        )

    def test_v4_plan_formal_entry_and_native_rows_are_independently_source_bound(self) -> None:
        first = subprocess.run(
            [sys.executable, str(V4_PLAN)],
            cwd=ROOT,
            check=True,
            capture_output=True,
        ).stdout
        second = subprocess.run(
            [sys.executable, str(V4_PLAN)],
            cwd=ROOT,
            check=True,
            capture_output=True,
        ).stdout
        self.assertEqual(first, second)
        plan = json.loads(first)
        self.assertEqual("NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_PLAN_V4", plan["schema"])
        self.assertEqual(
            "1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975",
            plan["zeroDecisionPlanSha256"],
        )
        self.assertEqual(
            "38a3bbda5b63365bc535a5669469728cfcd0c0189684a30c1d53f75b13b7fb35",
            plan["nativeExecution"]["nativeExecutionProfileSha256"],
        )
        self.assertEqual(42, plan["interval"]["totalBudgetedSeconds"])
        self.assertEqual(13_776, plan["interval"]["maximumSeconds"])
        self.assertEqual(34_916, plan["independentPhaseBudgetsSeconds"]["sum"])
        self.assertEqual(13_084, plan["independentPhaseBudgetsSeconds"]["hardCapHeadroom"])
        self.assertEqual(
            "TERMINAL_CENSORING_INFEASIBLE",
            plan["terminalCensoringFeasibility"]["legacySingleCutoff"],
        )
        self.assertEqual("PLAN_FEASIBLE", plan["terminalCensoringFeasibility"]["status"])

        module = MODULE_BUILD.read_text()
        launcher = V4_LAUNCHER.read_text()
        formal = V4_FORMAL_CAMPAIGN.read_text()
        canary = V3_FORMAL_RUNTIME.with_name("M3V3NativeBaselineCanaryTest.java").read_text()
        for token in (
            "realAllocatorV4BoundedAdaptiveFormalCampaign",
            "validateExistingRealAllocatorV4Diagnostic",
            "validateRealAllocatorV4Checkpoint",
            "sealRealAllocatorV4Evaluation",
            "realAllocatorV4PromotionCheck",
            "sealRealAllocatorV4Selection",
            "realAllocatorV4PreCampaignCheck",
        ):
            self.assertIn(token, module)
        self.assertIn("M3V4AdaptiveCampaignExecutor", formal)
        self.assertIn("AllocatorCampaignCheckpointV4", formal)
        self.assertIn("new M3V3RealFormalActionRuntime(", formal)
        self.assertIn("commit.substring(0, 16),\n                true", formal)
        self.assertIn("NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_EXECUTION_V4", formal)
        self.assertIn("NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_", canary)
        self.assertIn("return rowSchema(terminalDrainV4 ? 4 : 3)", canary)
        for token in (
            "NEREUS_M3_ALLOCATOR_V4_FORMAL_AUTHORIZATION_SHA",
            "NEREUS_M3_ALLOCATOR_V4_NATIVE_EXECUTION_PROFILE_SHA256",
            "NEREUS_M3_ALLOCATOR_V4_DIAGNOSTIC_PATH",
            "NEREUS_M3_ALLOCATOR_V4_DIAGNOSTIC_JUNIT_DIRECTORY",
            ":nereus-metadata-oxia:realAllocatorV4PreCampaignCheck",
            ":nereus-metadata-oxia:validateExistingRealAllocatorV4Diagnostic",
            ":nereus-metadata-oxia:realAllocatorV4BoundedAdaptiveFormalCampaign",
            "--hard-deadline-seconds 48000",
            "--termination-grace-seconds 30",
            "--no-configuration-cache",
        ):
            self.assertIn(token, launcher)

        environment = dict(os.environ)
        for name in tuple(environment):
            if name.startswith("NEREUS_M3_ALLOCATOR_V4_"):
                environment.pop(name)
        output = ROOT / "build" / "never-created-v4-formal"
        result = subprocess.run(
            [str(V4_LAUNCHER), "--bounded-adaptive-formal", str(output)],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(0, result.returncode)
        self.assertIn("separately authorized exact SHA", result.stderr)
        self.assertFalse(output.exists())

    def test_v4_diagnostic_is_independent_exact_inventory_and_uses_the_formal_drain(self) -> None:
        module = MODULE_BUILD.read_text()
        runner = V3_ASYNC_RUNNER.read_text()
        harness = V3_FORMAL_HARNESS.read_text()
        native_runtime = V3_NATIVE_RUNTIME.read_text()
        protocol = V4_PROTOCOL_MAIN.read_text()
        runner_test = V4_RUNNER_TEST.read_text()
        terminal = V4_TERMINAL_DIAGNOSTIC.read_text()
        range_latency = V4_RANGE_LATENCY_DIAGNOSTIC.read_text()

        diagnostic = module.split(
            'val realAllocatorV4DiagnosticTest = tasks.register<Test>("realAllocatorV4DiagnosticTest")',
            1,
        )[1].split("val realAllocatorV4DiagnosticJUnitDirectory", 1)[0]
        for suite in (
            "M3V3AsyncActorLaneRunnerTest",
            "M3V4AsyncActorLaneRunnerTest",
            "M3V3RealOxiaOperationDiagnosticTest",
            "M3V3AllocatorWorkflowDiagnosticTest",
            "M3V3NativePathDiagnosticTest",
            "M3V3NativeBaselineCanaryTest",
            "M3V4TerminalAdmissionDrainDiagnosticTest",
            "M3V4RangeLatencyDiagnosticTest",
            "M3RealAllocatorStrictIntervalDiagnosticTest",
        ):
            self.assertIn(suite, diagnostic)
        self.assertIn("forkEvery = 1", diagnostic)
        self.assertIn('systemProperty("nereus.m3.allocator.protocol", "V4")', diagnostic)
        self.assertIn("sealRealAllocatorV4Diagnostic", module)
        self.assertIn("validateRealAllocatorV4Diagnostic", module)
        self.assertIn("NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_JUNIT_MANIFEST_", V3_PROTOCOL_MAIN.read_text())
        self.assertIn('readDiagnosticSuite(directory, DIAGNOSTIC_SUITES, "V3")', V3_PROTOCOL_MAIN.read_text())
        self.assertIn('readDiagnosticSuite(directory, DIAGNOSTIC_SUITES, "V4")', protocol)
        self.assertIn("M3V4AsyncActorLaneRunnerTest", protocol)
        self.assertIn("M3V4TerminalAdmissionDrainDiagnosticTest", protocol)
        self.assertIn("M3V4RangeLatencyDiagnosticTest", protocol)
        self.assertIn(
            "exactRange1024TwentyFiveMillisSequenceAttributesOperationAndSchedulerCapacity",
            protocol,
        )
        self.assertIn('"v4-range1024-25ms-formal-sequence.json"', range_latency)
        self.assertIn("runSequence(25", range_latency)
        self.assertIn("AllocatorCampaignPromotionGateV4", protocol)
        self.assertIn("AllocatorCampaignSelectionV4", protocol)
        self.assertIn("formalV4()", runner)
        self.assertIn("terminalAdmissionDrain", runner)
        self.assertIn("formalV4()", harness)
        self.assertIn("formalV4()", native_runtime)
        self.assertIn("admitsOnlyAlreadyOfferedWorkDuringTheTerminalDrain", runner_test)
        self.assertIn("dropsAnOnTimeRequestStillBlockedAtTheFinalAdmissionDeadline", runner_test)
        self.assertIn("Duration.ofSeconds(2)", terminal)
        self.assertIn("M3V3RealFormalActionRuntime.candidateSchedule", terminal)
        self.assertIn("assertFormalEquivalent(fixed, 30_000)", terminal)
        self.assertIn("assertFormalEquivalent(derived, 24_000)", terminal)
        self.assertIn(r'\"diagnosticOnly\":true', terminal)
        self.assertIn(r'\"authority\":false', terminal)
        self.assertIn(r'\"selectionEligible\":false', terminal)
        self.assertIn("Cell.fixedRate(Candidate.RANGE_1024, POPULATION, latencyMillis, 1_000)", range_latency)
        self.assertIn("Cell.derived(Candidate.RANGE_1024, POPULATION, latencyMillis)", range_latency)
        self.assertIn("M3V3RealFormalActionRuntime.candidateSchedule", range_latency)
        self.assertIn("beginSharedDiagnosticCapture", range_latency)
        self.assertIn("realOutstandingMaximum", range_latency)
        self.assertIn("delaySchedulerLagP99Micros", range_latency)
        self.assertIn(r'\"diagnosticOnly\":true', range_latency)
        self.assertIn(r'\"authority\":false', range_latency)
        self.assertIn(r'\"selectionEligible\":false', range_latency)

    def test_v5_storm_admission_plan_wire_and_diagnostic_are_independently_source_bound(self) -> None:
        first = subprocess.run(
            [sys.executable, str(V5_PLAN)],
            cwd=ROOT,
            check=True,
            capture_output=True,
        ).stdout
        second = subprocess.run(
            [sys.executable, str(V5_PLAN)],
            cwd=ROOT,
            check=True,
            capture_output=True,
        ).stdout
        self.assertEqual(first, second)
        plan = json.loads(first)
        self.assertEqual("NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_PLAN_V5", plan["schema"])
        self.assertEqual(
            "974857cab839ba9cfd02ad8694a51976cf0279a4f61d11fe767aef5518a72dea",
            plan["zeroDecisionPlanSha256"],
        )
        self.assertEqual(
            "0bfa9670b8e3b1721ab83f03bd34ed368814e914288a5af772d17dec67ee3449",
            plan["nativeExecution"]["nativeExecutionProfileSha256"],
        )
        self.assertEqual("FROZEN_TARGET_OFFSET", plan["nativeExecution"]["scheduledOfferAuthority"])
        self.assertEqual(
            "FINAL_ADMISSION_DEADLINE",
            plan["nativeExecution"]["scheduledOfferDeliveryDeadline"],
        )
        self.assertEqual(
            {
                "actorCount": 4,
                "maxAsyncOutstandingPerActor": 128,
                "maxGlobalOutstanding": 512,
                "maxRolloverOutstandingPerBinding": 1,
                "preAdmissionQueueRateMultiplier": 2,
                "derivedOutstandingPerActor": 125,
            },
            plan["admission"],
        )
        self.assertEqual(1_024, plan["stormAdmissionFeasibility"]["v4"]["optimisticRequestsPerSecond"])
        self.assertEqual(
            "STORM_ADMISSION_INFEASIBLE",
            plan["stormAdmissionFeasibility"]["v4"]["status"],
        )
        self.assertEqual(2_048, plan["stormAdmissionFeasibility"]["v5"]["optimisticRequestsPerSecond"])
        self.assertEqual("PLAN_FEASIBLE", plan["stormAdmissionFeasibility"]["v5"]["status"])
        self.assertEqual(24_576, plan["stormAdmissionFeasibility"]["v5"]["optimisticServiceThroughRequests"])

        module = MODULE_BUILD.read_text()
        formal = V5_FORMAL_CAMPAIGN.read_text()
        protocol = V5_PROTOCOL_MAIN.read_text()
        raw_gate = V5_RAW_GATE.read_text()
        runner_test = V5_RUNNER_TEST.read_text()
        range_latency = V5_RANGE_LATENCY_DIAGNOSTIC.read_text()
        strict_sequence = V3_STRICT_SEQUENCE_DIAGNOSTIC.read_text()
        for token in (
            "realAllocatorV5BoundedAdaptiveFormalCampaign",
            "realAllocatorV5DiagnosticTest",
            "sealRealAllocatorV5Diagnostic",
            "validateRealAllocatorV5Diagnostic",
            "validateExistingRealAllocatorV5Diagnostic",
            "validateRealAllocatorV5Checkpoint",
            "sealRealAllocatorV5Evaluation",
            "realAllocatorV5PromotionCheck",
            "sealRealAllocatorV5Selection",
            "realAllocatorV5PreCampaignCheck",
        ):
            self.assertIn(token, module)
        diagnostic = module.split(
            'val realAllocatorV5DiagnosticTest = tasks.register<Test>("realAllocatorV5DiagnosticTest")',
            1,
        )[1].split("val realAllocatorV5DiagnosticJUnitDirectory", 1)[0]
        for suite in (
            "M3V3AsyncActorLaneRunnerTest",
            "M3V4AsyncActorLaneRunnerTest",
            "M3V5AsyncActorLaneRunnerTest",
            "M3V3NativeBaselineCanaryTest",
            "M3V5TerminalAdmissionDrainDiagnosticTest",
            "M3V5RangeLatencyDiagnosticTest",
        ):
            self.assertIn(suite, diagnostic)
        self.assertIn('systemProperty("nereus.m3.allocator.protocol", "V5")', diagnostic)
        self.assertIn("AllocatorCampaignCheckpointV5", formal)
        self.assertIn("M3V5AdaptiveCampaignExecutor", formal)
        self.assertIn("commit.substring(0, 16),\n                5", formal)
        self.assertIn("NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_EXECUTION_V5", formal)
        self.assertIn("M3V5DiagnosticRawGate.validate", protocol)
        self.assertIn("diagnostic.rawManifestDigest()", protocol)
        self.assertIn("rawManifest", protocol)
        self.assertIn("M3V4AsyncActorLaneRunnerTest", protocol)
        self.assertIn("M3V5AsyncActorLaneRunnerTest", protocol)
        self.assertIn("DIAGNOSTIC_RAW_MANIFEST_V5", raw_gate)
        self.assertIn('"v5-range1024-25ms-formal-sequence.json"', raw_gate)
        self.assertIn("requireLosslessRow(json, \"fixed1000\", 1_000, true)", raw_gate)
        self.assertIn("requireLosslessRow(json, \"derived800\", 800, true)", raw_gate)
        self.assertIn("requireLegacyAuditableRow(json, \"fixed1000\", 1_000)", raw_gate)
        self.assertIn("requireLegacyAuditableRow(json, \"derived800\", 800)", raw_gate)
        self.assertIn("warmupDroppedBeforeAdmission", raw_gate)
        self.assertIn("native-baseline-row-09.json", raw_gate)
        self.assertIn("actorLanesStopped", raw_gate)
        self.assertIn("forContractTestV5", range_latency)
        self.assertIn("forContractTestV5", strict_sequence)
        self.assertIn('Duration.ofSeconds(2)', strict_sequence)
        self.assertIn('"terminalAdmissionDrainSeconds\\\":2"', strict_sequence)
        self.assertIn("MAX_GLOBAL_OUTSTANDING", runner_test)
        self.assertIn("containsExactly(128, 128, 128, 128)", runner_test)

        launcher = V5_LAUNCHER.read_text()
        for token in (
            "NEREUS_M3_ALLOCATOR_V5_FORMAL_AUTHORIZATION_SHA",
            "NEREUS_M3_ALLOCATOR_V5_NATIVE_EXECUTION_PROFILE_SHA256",
            "NEREUS_M3_ALLOCATOR_V5_DIAGNOSTIC_PATH",
            "NEREUS_M3_ALLOCATOR_V5_DIAGNOSTIC_OUTPUT_DIRECTORY",
            ":nereus-metadata-oxia:realAllocatorV5PreCampaignCheck",
            ":nereus-metadata-oxia:validateExistingRealAllocatorV5Diagnostic",
            ":nereus-metadata-oxia:realAllocatorV5BoundedAdaptiveFormalCampaign",
            ".stormAdmissionFeasibility.v4.status",
            ".stormAdmissionFeasibility.v5.status",
            "--hard-deadline-seconds 48000",
            "--no-configuration-cache",
        ):
            self.assertIn(token, launcher)
        environment = dict(os.environ)
        for name in tuple(environment):
            if name.startswith("NEREUS_M3_ALLOCATOR_V5_"):
                environment.pop(name)
        output = ROOT / "build" / "never-created-v5-formal"
        result = subprocess.run(
            [str(V5_LAUNCHER), "--bounded-adaptive-formal", str(output)],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(0, result.returncode)
        self.assertIn("separately authorized exact SHA", result.stderr)
        self.assertFalse(output.exists())

    def test_v3_native_execution_plan_is_stable_source_bound_and_feasible(self) -> None:
        first = subprocess.run(
            [sys.executable, str(V3_PLAN)],
            cwd=ROOT,
            check=True,
            capture_output=True,
        ).stdout
        second = subprocess.run(
            [sys.executable, str(V3_PLAN)],
            cwd=ROOT,
            check=True,
            capture_output=True,
        ).stdout
        self.assertEqual(first, second)
        plan = json.loads(first)
        self.assertEqual(
            "5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283",
            plan["zeroDecisionPlanSha256"],
        )
        self.assertEqual(328, plan["maximumExecutedIntervalCells"])
        self.assertEqual(360, plan["maximumExecutedFaultActions"])
        self.assertEqual(32, plan["maximumExecutedScaleActions"])
        self.assertEqual(720, plan["maximumTotalExecutedActions"])
        self.assertEqual(48_000, plan["campaignWallClockCapSeconds"])
        self.assertEqual("PLAN_FEASIBLE", plan["feasibilityStatus"])
        self.assertEqual(
            {
                "nativeExecutionModel": "PINNED_MANAGED_LEDGER_ASYNC_CHAIN_V1",
                "nativeBridgeWorkers": 0,
                "nativeBridgeQueueCapacity": 0,
                "hiddenDispatchQueue": 0,
                "nativeExecutionProfileSha256": (
                    "4b11530bd3627feba731f3c59026012dce95b35c1434b0e2b71d5effbe18d751"
                ),
                "workloadScheduleSha256": (
                    "b0e923a08ea26a9638f6722698a88a8f20a4d11cbf58126fe4d03b28b4e0e798"
                ),
            },
            plan["nativeExecution"],
        )
        self.assertEqual(
            subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip(),
            plan["exactSourceTuple"]["nereusCommit"],
        )

    def test_v3_formal_and_diagnostic_share_one_nonblocking_native_runtime(self) -> None:
        formal = V3_FORMAL_RUNTIME.read_text()
        shared = V3_NATIVE_RUNTIME.read_text()
        module = MODULE_BUILD.read_text()
        self.assertIn("nativeIntervalRuntime().run(", formal)
        self.assertIn("M3AllocatorWorkloadPlan.v3Requests", formal)
        self.assertIn("M3AllocatorWorkloadPlan.v3Requests", shared)
        for forbidden in (
            "nativeDispatchWorkers",
            "boundedNativeDispatchWorkers",
            "ArrayBlockingQueue",
            "CompletableFuture.runAsync",
        ):
            self.assertNotIn(forbidden, formal)
        self.assertIn("rolloverAsync(", shared)
        self.assertIn("operationContext::allowsNextMetadataOperation", shared)
        self.assertNotIn("ExecutorService", shared)
        self.assertNotIn(".join()", shared)
        self.assertNotIn("toCompletableFuture()", shared)
        self.assertIn("realAllocatorV3NativeCanaryTest", module)
        diagnostic = module.split(
            'val realAllocatorV3DiagnosticTest = tasks.register<Test>("realAllocatorV3DiagnosticTest")',
            1,
        )[1].split("val realAllocatorV3NativeCanaryTest", 1)[0]
        self.assertIn("M3V3NativeBaselineCanaryTest", diagnostic)
        canary = V3_FORMAL_RUNTIME.with_name("M3V3NativeBaselineCanaryTest.java").read_text()
        for diagnostic_field in (
            "firstDroppedOrdinal",
            "firstDroppedSchedulerLagMicros",
            "firstDroppedFailureSummary",
            "queueDepthMaximum",
            "queueWaitMaximumMicros",
            "bindingBusyMaximum",
            "pendingPermitMaximum",
        ):
            self.assertIn(diagnostic_field, canary)
        baseline_assertions = canary.split("private static void assertBaseline", 1)[1].split(
            "private static void assertRepresentative", 1
        )[0]
        representative_assertions = canary.split("private static void assertRepresentative", 1)[1].split(
            "private static void assertCommon", 1
        )[0]
        self.assertNotIn("row.warmupDropped()", baseline_assertions)
        self.assertNotIn("row.warmupDropped()", representative_assertions)
        for measured_assertion in ("row.dropped()", "row.failed()", "row.timedOut()"):
            self.assertIn(measured_assertion, baseline_assertions)
            self.assertIn(measured_assertion, representative_assertions)
        self.assertIn('maxHeapSize = "6144m"', diagnostic)
        self.assertIn("timeout.set(Duration.ofMinutes(60))", diagnostic)
        self.assertIn("validateRealAllocatorV3Diagnostic", module)
        harness = V3_FORMAL_RUNTIME.with_name("M3V3AllocatorFormalHarness.java").read_text()
        protocol_main = V3_FORMAL_RUNTIME.with_name("M3V3AllocatorProtocolMain.java").read_text()
        self.assertIn("candidateInfrastructureValid(interval)", harness)
        self.assertIn("warmupLoadRejectedAfterAdmission", formal)
        self.assertIn("warmupUnexpectedFailedAfterAdmission", formal)
        self.assertIn(
            "candidateWarmupLoadRejectionAllowsAdaptiveDescentButUnexpectedFailureDoesNot()",
            protocol_main,
        )

    def test_v3_async_completion_reconciles_exact_cell_before_following_fault_action(self) -> None:
        population = V3_CANDIDATE_POPULATION.read_text()
        v3_completion = population.split("private CompletionStage<?> boundedAllocateV3(", 1)[1].split(
            "private CompletionStage<?> boundedAllocate(", 1
        )[0]
        self.assertIn(
            "cell.updateAndGet(current -> newestCompletedWorkflowCell(current, exact.exactCell()))",
            v3_completion,
        )
        self.assertIn("heads.compareAndSet(ledgerIndex, predecessor, exact.exactHead())", v3_completion)
        self.assertIn("population Cell proof retained a reservation", population)
        self.assertIn("if (observed.value().reservation().isPresent())", population)
        self.assertIn("return current;", population)
        self.assertIn("workflow Cell cursor/grant ordering diverged", population)

    def test_v3_unexpected_warmup_failure_is_attributed_and_formal_sequence_is_replayed(self) -> None:
        runner = V3_ASYNC_RUNNER.read_text()
        harness = V3_FORMAL_HARNESS.read_text()
        diagnostic = V3_STRICT_SEQUENCE_DIAGNOSTIC.read_text()
        module = MODULE_BUILD.read_text()
        self.assertIn("warmupFirstUnexpectedFailure", runner)
        self.assertIn("warmupFirstUnexpectedFailure", harness)
        self.assertIn("warmupFirstUnexpectedFailure.isEmpty()", runner)
        self.assertIn("startOfferers(", runner)
        self.assertIn('"m3-v3-allocator-offer-actor-" + actorId', runner)
        self.assertIn("CountDownLatch measurementReady", runner)
        self.assertIn("CountDownLatch measurementRelease", runner)
        self.assertIn("FINAL_OFFER_PRECISION_WINDOW_NANOS", runner)
        self.assertIn("waitUntilOffer(targetNanos, cutoffNanos)", runner)
        self.assertIn("queue.size() == 1 && canAdmit(offer)", runner)
        self.assertIn("dispatch(state, admitted, operation)", runner)
        self.assertNotIn("newSingleThreadScheduledExecutor", runner)
        self.assertNotIn("warmupFirstUnexpectedFailure", V3_FORMAL_RUNTIME.read_text())
        self.assertIn("Cell.fixedRate(Candidate.STRICT, 10_000, 1, 1_000)", diagnostic)
        self.assertIn("Cell.derived(Candidate.STRICT, 10_000, 1)", diagnostic)
        self.assertIn("Duration.ofSeconds(10)", diagnostic)
        self.assertIn("Duration.ofSeconds(30)", diagnostic)
        self.assertIn("M3V3RealFormalActionRuntime.candidateSchedule", diagnostic)
        self.assertIn("globalOutstandingMaximum()).isGreaterThan(4)", diagnostic)
        self.assertIn(r'\"diagnosticOnly\":true', diagnostic)
        self.assertIn(r'\"authority\":false', diagnostic)
        self.assertIn(r'\"selectionEligible\":false', diagnostic)
        self.assertIn("M3RealAllocatorStrictIntervalDiagnosticTest", module)
        full_diagnostic = module.split(
            'val realAllocatorV3DiagnosticTest = tasks.register<Test>("realAllocatorV3DiagnosticTest")',
            1,
        )[1].split("val realAllocatorV3NativeCanaryTest", 1)[0]
        self.assertIn("M3RealAllocatorStrictIntervalDiagnosticTest", full_diagnostic)
        self.assertIn("maxParallelForks = 1", full_diagnostic)
        self.assertIn("forkEvery = 1", full_diagnostic)
        protocol_main = V3_PROTOCOL_MAIN.read_text()
        self.assertIn('PACKAGE + "M3RealAllocatorStrictIntervalDiagnosticTest"', protocol_main)
        self.assertIn(
            '"replaysTheExactFormalSequenceWithoutUnexpectedWarmupFailure()"',
            protocol_main,
        )
        self.assertIn(
            '"replaysTheExactRange16ScaleThenFixedIntervalWithoutRetainingReservations()"',
            protocol_main,
        )
        self.assertIn(
            "void replaysTheExactFormalSequenceWithoutUnexpectedWarmupFailure()",
            diagnostic,
        )
        self.assertIn(
            "void replaysTheExactRange16ScaleThenFixedIntervalWithoutRetainingReservations()",
            diagnostic,
        )
        self.assertIn("Cell.fixedRate(Candidate.RANGE_16, 10_000, 1, 1_000)", diagnostic)
        self.assertIn('"range16-formal-sequence.json"', diagnostic)
        self.assertIn(r'\"measuredDroppedBeforeAdmission\"', diagnostic)
        self.assertIn(r'\"firstDroppedBindingOrdinal\"', diagnostic)
        self.assertIn("result.rolloverP99Micros()", diagnostic)
        self.assertLess(
            diagnostic.index('"range16-formal-sequence.json"'),
            diagnostic.index("assertThat(fixed.runnerResult().completed()).isEqualTo(30_000)"),
        )
        cutoff_diagnostic = V3_CANDIDATE_CUTOFF_DIAGNOSTIC.read_text()
        self.assertIn(r'\"firstDroppedBindingOrdinal\"', cutoff_diagnostic)
        self.assertIn("result.rolloverP99Micros()", cutoff_diagnostic)

    def test_formal_archiver_is_create_new_byte_exact_and_collision_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "formal"
            source.mkdir()
            campaign = source / "campaign-result.json"
            campaign.write_text(
                '{"status":"COMPLETED","terminalReason":"COMPLETED"}\n',
                encoding="utf-8",
            )
            evaluation = source / "evaluation.naev"
            evaluation.write_bytes(b"canonical-naev3")
            checkpoints = source / "checkpoints"
            checkpoints.mkdir()
            checkpoint = checkpoints / "final.nacp"
            checkpoint.write_bytes(b"canonical-nacp3")
            attachment = source / "attachments"
            attachment.mkdir()
            (attachment / "one.json").write_bytes(b"{}\n")
            files = sorted(path for path in source.rglob("*") if path.is_file())
            archive = root / "archive"
            command = [
                sys.executable,
                str(FORMAL_ARCHIVER),
                "--source",
                str(source),
                "--archive",
                str(archive),
                "--archived-on",
                "2026-08-28",
                "--source-commit",
                "a" * 40,
                "--plan-sha256",
                "b" * 64,
                "--campaign-result-sha256",
                hashlib.sha256(campaign.read_bytes()).hexdigest(),
                "--final-checkpoint-relative-path",
                checkpoint.relative_to(source).as_posix(),
                "--final-checkpoint-sha256",
                hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
                "--evaluation-sha256",
                hashlib.sha256(evaluation.read_bytes()).hexdigest(),
                "--attachment-root-sha256",
                "c" * 64,
                "--formal-junit-sha256",
                "d" * 64,
                "--expected-file-count",
                str(len(files)),
                "--expected-total-bytes",
                str(sum(path.stat().st_size for path in files)),
                "--evaluation-status",
                "NATIVE_BASELINE_UNAVAILABLE",
            ]
            first = subprocess.run(command, check=True, capture_output=True, text=True)
            identity = json.loads((archive / "archive-identity.json").read_bytes())
            self.assertTrue(identity["sourceAndPayloadByteIdentical"])
            self.assertFalse(identity["promotableInput"])
            self.assertFalse(identity["futureCampaignInput"])
            for source_file in files:
                relative = source_file.relative_to(source)
                self.assertEqual(source_file.read_bytes(), (archive / "payload" / relative).read_bytes())
            manifest_before = (archive / "SHA256SUMS").read_bytes()
            second = subprocess.run(command, check=False, capture_output=True, text=True)
            self.assertNotEqual(0, second.returncode)
            self.assertIn("archive target already exists", second.stderr)
            self.assertEqual(manifest_before, (archive / "SHA256SUMS").read_bytes())
            self.assertIn('"sourceAndPayloadByteIdentical": true', first.stdout)

            none_archive = root / "none-archive"
            none_command = command.copy()
            none_command[none_command.index("--archive") + 1] = str(none_archive)
            none_command[none_command.index("--evaluation-status") + 1] = "NONE_QUALIFIED"
            subprocess.run(none_command, check=True, capture_output=True, text=True)
            none_identity = json.loads((none_archive / "archive-identity.json").read_bytes())
            self.assertEqual("NONE_QUALIFIED", none_identity["evaluationStatus"])
            self.assertFalse(none_identity["selectionEligible"])
            self.assertFalse(none_identity["promotableInput"])

            selected_archive = root / "selected-archive"
            selected_command = command.copy()
            selected_command[selected_command.index("--archive") + 1] = str(selected_archive)
            selected_command[selected_command.index("--evaluation-status") + 1] = "RANGE_SELECTED"
            selected = subprocess.run(selected_command, check=False, capture_output=True, text=True)
            self.assertNotEqual(0, selected.returncode)
            self.assertIn("not a legal non-promotable allocator terminal", selected.stderr)
            self.assertFalse(selected_archive.exists())

            v4_source = root / "v4-formal"
            v4_checkpoints = v4_source / "checkpoints"
            v4_checkpoints.mkdir(parents=True)
            v4_checkpoint = v4_checkpoints / "last.nacp4"
            v4_checkpoint.write_bytes(b"v4-checkpoint")
            (v4_source / "campaign-result.json").write_bytes(campaign.read_bytes())
            v4_evaluation = v4_source / "evaluation.naev4"
            v4_evaluation.write_bytes(b"v4-evaluation")
            v4_archive = root / "v4-none-archive"
            v4_files = sorted(path for path in v4_source.rglob("*") if path.is_file())
            v4_command = [
                sys.executable,
                str(FORMAL_ARCHIVER),
                "--source",
                str(v4_source),
                "--archive",
                str(v4_archive),
                "--protocol-version",
                "4",
                "--archived-on",
                "2026-08-28",
                "--source-commit",
                "e" * 40,
                "--plan-sha256",
                "f" * 64,
                "--campaign-result-sha256",
                hashlib.sha256((v4_source / "campaign-result.json").read_bytes()).hexdigest(),
                "--final-checkpoint-relative-path",
                v4_checkpoint.relative_to(v4_source).as_posix(),
                "--final-checkpoint-sha256",
                hashlib.sha256(v4_checkpoint.read_bytes()).hexdigest(),
                "--evaluation-sha256",
                hashlib.sha256(v4_evaluation.read_bytes()).hexdigest(),
                "--attachment-root-sha256",
                "1" * 64,
                "--formal-junit-sha256",
                "2" * 64,
                "--expected-file-count",
                str(len(v4_files)),
                "--expected-total-bytes",
                str(sum(path.stat().st_size for path in v4_files)),
                "--evaluation-status",
                "NONE_QUALIFIED",
            ]
            subprocess.run(v4_command, check=True, capture_output=True, text=True)
            v4_identity = json.loads((v4_archive / "archive-identity.json").read_bytes())
            self.assertEqual(4, v4_identity["protocolVersion"])
            self.assertEqual("evaluation.naev4", v4_identity["evaluationRelativePath"])
            self.assertFalse(v4_identity["nars4Present"])

            v5_source = root / "v5-formal"
            v5_checkpoints = v5_source / "checkpoints"
            v5_checkpoints.mkdir(parents=True)
            v5_checkpoint = v5_checkpoints / "last.nacp5"
            v5_checkpoint.write_bytes(b"v5-checkpoint")
            (v5_source / "campaign-result.json").write_bytes(campaign.read_bytes())
            v5_evaluation = v5_source / "evaluation.naev5"
            v5_evaluation.write_bytes(b"v5-evaluation")
            v5_archive = root / "v5-none-archive"
            v5_files = sorted(path for path in v5_source.rglob("*") if path.is_file())
            v5_command = v4_command.copy()
            replacements = {
                str(v4_source): str(v5_source),
                str(v4_archive): str(v5_archive),
                "4": "5",
                "e" * 40: "3" * 40,
                "f" * 64: "4" * 64,
                hashlib.sha256((v4_source / "campaign-result.json").read_bytes()).hexdigest(): hashlib.sha256(
                    (v5_source / "campaign-result.json").read_bytes()
                ).hexdigest(),
                v4_checkpoint.relative_to(v4_source).as_posix(): v5_checkpoint.relative_to(v5_source).as_posix(),
                hashlib.sha256(v4_checkpoint.read_bytes()).hexdigest(): hashlib.sha256(
                    v5_checkpoint.read_bytes()
                ).hexdigest(),
                hashlib.sha256(v4_evaluation.read_bytes()).hexdigest(): hashlib.sha256(
                    v5_evaluation.read_bytes()
                ).hexdigest(),
                str(len(v4_files)): str(len(v5_files)),
                str(sum(path.stat().st_size for path in v4_files)): str(
                    sum(path.stat().st_size for path in v5_files)
                ),
            }
            v5_command = [replacements.get(value, value) for value in v5_command]
            subprocess.run(v5_command, check=True, capture_output=True, text=True)
            v5_identity = json.loads((v5_archive / "archive-identity.json").read_bytes())
            self.assertEqual(
                "NEREUS_V2_M3_ALLOCATOR_FORMAL_ARCHIVE_IDENTITY_V4",
                v5_identity["schema"],
            )
            self.assertEqual(5, v5_identity["protocolVersion"])
            self.assertEqual("evaluation.naev5", v5_identity["evaluationRelativePath"])
            self.assertFalse(v5_identity["nars5Present"])

    def test_failed_formal_archiver_preserves_terminal_payload_and_junit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "failed-formal"
            checkpoints = source / "checkpoints"
            checkpoints.mkdir(parents=True)
            checkpoint = checkpoints / "last.nacp"
            checkpoint.write_bytes(b"failed-checkpoint")
            campaign = source / "campaign-result.json"
            campaign.write_text(
                json.dumps(
                    {
                        "status": "INFRASTRUCTURE_FAILED",
                        "terminalReason": "BUDGET_ACCOUNTING_FAILED",
                        "terminalDetail": "derived slot has no baseline-independent rate",
                        "checkpointSequence": 1,
                        "evaluationCreated": False,
                        "selectionCreated": False,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            junit = root / "formal-junit.xml"
            junit.write_text(
                '<testsuite tests="1" failures="1" errors="0" skipped="0"/>\n',
                encoding="utf-8",
            )
            files = sorted(path for path in source.rglob("*") if path.is_file())
            archive = root / "archive"
            command = [
                sys.executable,
                str(FAILED_FORMAL_ARCHIVER),
                "--source",
                str(source),
                "--archive",
                str(archive),
                "--archived-on",
                "2026-08-28",
                "--source-commit",
                "a" * 40,
                "--plan-sha256",
                "b" * 64,
                "--campaign-result-sha256",
                hashlib.sha256(campaign.read_bytes()).hexdigest(),
                "--final-checkpoint-relative-path",
                checkpoint.relative_to(source).as_posix(),
                "--final-checkpoint-sha256",
                hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
                "--formal-junit",
                str(junit),
                "--formal-junit-sha256",
                hashlib.sha256(junit.read_bytes()).hexdigest(),
                "--expected-file-count",
                str(len(files)),
                "--expected-total-bytes",
                str(sum(path.stat().st_size for path in files)),
                "--terminal-status",
                "INFRASTRUCTURE_FAILED",
                "--terminal-reason",
                "BUDGET_ACCOUNTING_FAILED",
            ]

            invalid = command.copy()
            invalid[invalid.index("--terminal-reason") + 1] = "BUDGET_EXHAUSTED"
            rejected = subprocess.run(invalid, check=False, capture_output=True, text=True)
            self.assertNotEqual(0, rejected.returncode)
            self.assertIn("terminal reason does not belong", rejected.stderr)
            self.assertFalse(archive.exists())

            subprocess.run(command, check=True, capture_output=True, text=True)
            identity = json.loads((archive / "archive-identity.json").read_bytes())
            self.assertEqual("INFRASTRUCTURE_FAILED", identity["campaignStatus"])
            self.assertEqual("BUDGET_ACCOUNTING_FAILED", identity["terminalReason"])
            self.assertFalse(identity["evaluationCreated"])
            self.assertFalse(identity["selectionCreated"])
            self.assertFalse(identity["promotableInput"])
            self.assertEqual(junit.read_bytes(), (archive / "formal-junit.xml").read_bytes())
            for source_file in files:
                relative = source_file.relative_to(source)
                self.assertEqual(source_file.read_bytes(), (archive / "payload" / relative).read_bytes())
            repeated = subprocess.run(command, check=False, capture_output=True, text=True)
            self.assertNotEqual(0, repeated.returncode)
            self.assertIn("archive target already exists", repeated.stderr)

            v5_archive = root / "v5-archive"
            v5_command = command.copy()
            v5_command[v5_command.index("--archive") + 1] = str(v5_archive)
            v5_command[2:2] = ["--protocol-version", "5"]
            subprocess.run(v5_command, check=True, capture_output=True, text=True)
            v5_identity = json.loads((v5_archive / "archive-identity.json").read_bytes())
            self.assertEqual(5, v5_identity["protocolVersion"])
            self.assertEqual(
                "NEREUS_V2_M3_ALLOCATOR_FAILED_FORMAL_ARCHIVE_IDENTITY_V2",
                v5_identity["schema"],
            )

    def test_promotion_invalid_archiver_preserves_selected_evaluation_without_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "promotion-invalid-formal"
            checkpoints = source / "checkpoints"
            actions = source / "actions"
            checkpoints.mkdir(parents=True)
            actions.mkdir()
            checkpoint = checkpoints / "last.nacp5"
            checkpoint.write_bytes(b"completed-checkpoint")
            campaign = source / "campaign-result.json"
            campaign.write_text(
                json.dumps(
                    {
                        "status": "COMPLETED",
                        "terminalReason": "COMPLETED",
                        "checkpointSequence": 32,
                        "evaluationCreated": False,
                        "selectionCreated": False,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            evaluation = source / "evaluation.naev5"
            evaluation.write_bytes(b"range-selected-evaluation")
            attachment_bytes = b"x" * (16 * 1024 * 1024 + 1)
            attachment_digest = hashlib.sha256(attachment_bytes).hexdigest()
            attachment = actions / (
                "fault-RANGE_64-100000-1-BROKER_SESSION_CRASH_MASS_TAKEOVER-"
                + attachment_digest
                + ".nare1"
            )
            attachment.write_bytes(attachment_bytes)
            junit = root / "formal-junit.xml"
            junit.write_text(
                '<testsuite tests="1" failures="0" errors="0" skipped="0"/>' + "\n",
                encoding="utf-8",
            )
            files = sorted(path for path in source.rglob("*") if path.is_file())
            archive = root / "archive"
            failure_detail = "allocator V3 input length is outside its cap: actions/fault-RANGE_64.nare1"
            command = [
                sys.executable,
                str(PROMOTION_INVALID_FORMAL_ARCHIVER),
                "--source",
                str(source),
                "--archive",
                str(archive),
                "--archived-on",
                "2026-08-30",
                "--source-commit",
                "a" * 40,
                "--plan-sha256",
                "b" * 64,
                "--campaign-result-sha256",
                hashlib.sha256(campaign.read_bytes()).hexdigest(),
                "--final-checkpoint-relative-path",
                checkpoint.relative_to(source).as_posix(),
                "--final-checkpoint-sha256",
                hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
                "--evaluation-sha256",
                hashlib.sha256(evaluation.read_bytes()).hexdigest(),
                "--attachment-root-sha256",
                "c" * 64,
                "--formal-junit",
                str(junit),
                "--formal-junit-sha256",
                hashlib.sha256(junit.read_bytes()).hexdigest(),
                "--expected-file-count",
                str(len(files)),
                "--expected-total-bytes",
                str(sum(path.stat().st_size for path in files)),
                "--evaluation-status",
                "RANGE_SELECTED",
                "--selected-candidate",
                "RANGE_64",
                "--failed-attachment-relative-path",
                attachment.relative_to(source).as_posix(),
                "--failed-attachment-sha256",
                attachment_digest,
                "--failed-attachment-bytes",
                str(len(attachment_bytes)),
                "--promotion-failure-code",
                "PHYSICAL_ATTACHMENT_SIZE_CAP",
                "--promotion-failure-detail",
                failure_detail,
            ]

            invalid = command.copy()
            invalid[invalid.index("--selected-candidate") + 1] = "STRICT"
            rejected = subprocess.run(invalid, check=False, capture_output=True, text=True)
            self.assertNotEqual(0, rejected.returncode)
            self.assertIn("does not belong", rejected.stderr)
            self.assertFalse(archive.exists())

            subprocess.run(command, check=True, capture_output=True, text=True)
            identity = json.loads((archive / "archive-identity.json").read_bytes())
            self.assertEqual(
                "NEREUS_V2_M3_ALLOCATOR_PROMOTION_INVALID_FORMAL_ARCHIVE_IDENTITY_V1",
                identity["schema"],
            )
            self.assertEqual("COMPLETED", identity["campaignStatus"])
            self.assertEqual("RANGE_SELECTED", identity["evaluationStatus"])
            self.assertEqual("RANGE_64", identity["selectedCandidate"])
            self.assertTrue(identity["selectionEligible"])
            self.assertEqual("UNSELECTED", identity["allocatorMode"])
            self.assertFalse(identity["nars5Present"])
            self.assertFalse(identity["promotionIntegrityValidated"])
            self.assertFalse(identity["promotableInput"])
            self.assertFalse(identity["futureCampaignInput"])
            self.assertEqual(16 * 1024 * 1024, identity["legacyAttachmentCapBytes"])
            self.assertEqual(32 * 1024 * 1024, identity["correctedV5AttachmentCapBytes"])
            self.assertEqual(junit.read_bytes(), (archive / "formal-junit.xml").read_bytes())
            for source_file in files:
                relative = source_file.relative_to(source)
                self.assertEqual(source_file.read_bytes(), (archive / "payload" / relative).read_bytes())
            repeated = subprocess.run(command, check=False, capture_output=True, text=True)
            self.assertNotEqual(0, repeated.returncode)
            self.assertIn("archive target already exists", repeated.stderr)

    def test_selected_formal_archiver_preserves_promotion_validated_nars5(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "selected-formal"
            checkpoints = source / "checkpoints"
            checkpoints.mkdir(parents=True)
            checkpoint = checkpoints / "last.nacp5"
            checkpoint.write_bytes(b"completed-checkpoint")
            checkpoint_sha = hashlib.sha256(checkpoint.read_bytes()).hexdigest()
            campaign = source / "campaign-result.json"
            campaign.write_text(
                json.dumps({"status": "COMPLETED", "terminalReason": "COMPLETED"}) + "\n",
                encoding="utf-8",
            )
            evaluation = source / "evaluation.naev5"
            evaluation.write_bytes(b"range-selected-evaluation")
            evaluation_sha = hashlib.sha256(evaluation.read_bytes()).hexdigest()
            junit = root / "formal-junit.xml"
            junit.write_text(
                '<testsuite tests="1" failures="0" errors="0" skipped="0"/>\n',
                encoding="utf-8",
            )
            junit_sha = hashlib.sha256(junit.read_bytes()).hexdigest()
            promotion = source / "promotion-decision.json"
            diagnostic_sha = "d" * 64
            diagnostic_junit_sha = "e" * 64
            diagnostic_raw_sha = "f" * 64
            promotion.write_text(
                json.dumps(
                    {
                        "schema": "NEREUS_V2_M3_ALLOCATOR_PROMOTION_DECISION_V5",
                        "status": "PROMOTABLE",
                        "selectedCandidate": "RANGE_64",
                        "checkpointSha256": checkpoint_sha,
                        "evaluationSha256": evaluation_sha,
                        "diagnosticSha256": diagnostic_sha,
                        "diagnosticJUnitSha256": diagnostic_junit_sha,
                        "diagnosticRawManifestSha256": diagnostic_raw_sha,
                        "formalJUnitSha256": junit_sha,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            source_commit = "a" * 40
            attachment_root = "c" * 64
            selection_bytes = bytearray(500)
            selection_bytes[:10] = b"NARS5\0\0\0\0\x05"
            selection_bytes[10] = 3
            selection_bytes[12:52] = source_commit.encode("ascii")
            selection_bytes[148:180] = bytes.fromhex("b" * 64)
            selection_bytes[180:212] = bytes.fromhex("1" * 64)
            selection_bytes[212:244] = bytes.fromhex("b" * 64)
            selection_bytes[244:276] = bytes.fromhex("2" * 64)
            selection_bytes[276:308] = bytes.fromhex(checkpoint_sha)
            selection_bytes[308:340] = bytes.fromhex(evaluation_sha)
            selection_bytes[340:372] = bytes.fromhex(attachment_root)
            selection_bytes[372:404] = bytes.fromhex(diagnostic_sha)
            selection_bytes[404:436] = bytes.fromhex(diagnostic_junit_sha)
            selection_bytes[436:468] = bytes.fromhex(diagnostic_raw_sha)
            selection_bytes[468:500] = bytes.fromhex("3" * 64)
            selection = source / "selection.nars5"
            selection.write_bytes(selection_bytes)
            files = sorted(path for path in source.rglob("*") if path.is_file())
            archive = root / "archive"
            command = [
                sys.executable,
                str(SELECTED_FORMAL_ARCHIVER),
                "--source",
                str(source),
                "--archive",
                str(archive),
                "--archived-on",
                "2026-08-30",
                "--source-commit",
                source_commit,
                "--plan-sha256",
                "b" * 64,
                "--campaign-result-sha256",
                hashlib.sha256(campaign.read_bytes()).hexdigest(),
                "--final-checkpoint-relative-path",
                checkpoint.relative_to(source).as_posix(),
                "--final-checkpoint-sha256",
                checkpoint_sha,
                "--evaluation-sha256",
                evaluation_sha,
                "--attachment-root-sha256",
                attachment_root,
                "--promotion-decision-sha256",
                hashlib.sha256(promotion.read_bytes()).hexdigest(),
                "--selection-sha256",
                hashlib.sha256(selection.read_bytes()).hexdigest(),
                "--formal-junit",
                str(junit),
                "--formal-junit-sha256",
                junit_sha,
                "--expected-file-count",
                str(len(files)),
                "--expected-total-bytes",
                str(sum(path.stat().st_size for path in files)),
                "--evaluation-status",
                "RANGE_SELECTED",
                "--selected-candidate",
                "RANGE_64",
            ]

            subprocess.run(command, check=True, capture_output=True, text=True)
            identity = json.loads((archive / "archive-identity.json").read_bytes())
            self.assertEqual(
                "NEREUS_V2_M3_ALLOCATOR_SELECTED_FORMAL_ARCHIVE_IDENTITY_V1",
                identity["schema"],
            )
            self.assertEqual("RANGE_64", identity["selectedCandidate"])
            self.assertEqual("RANGE", identity["allocatorMode"])
            self.assertTrue(identity["nars5Present"])
            self.assertTrue(identity["promotionIntegrityValidated"])
            self.assertTrue(identity["promotableInput"])
            self.assertFalse(identity["futureCampaignInput"])
            self.assertEqual(junit.read_bytes(), (archive / "formal-junit.xml").read_bytes())
            for source_file in files:
                relative = source_file.relative_to(source)
                self.assertEqual(source_file.read_bytes(), (archive / "payload" / relative).read_bytes())

            invalid = command.copy()
            invalid[invalid.index("--selected-candidate") + 1] = "STRICT"
            second_archive = root / "invalid-archive"
            invalid[invalid.index("--archive") + 1] = str(second_archive)
            rejected = subprocess.run(invalid, check=False, capture_output=True, text=True)
            self.assertNotEqual(0, rejected.returncode)
            self.assertIn("does not belong", rejected.stderr)
            self.assertFalse(second_archive.exists())

            wrong_wire = command.copy()
            wire_archive = root / "wrong-wire-archive"
            wrong_wire[wrong_wire.index("--archive") + 1] = str(wire_archive)
            selection_bytes[372] ^= 1
            selection.write_bytes(selection_bytes)
            wrong_wire[wrong_wire.index("--selection-sha256") + 1] = hashlib.sha256(
                selection.read_bytes()
            ).hexdigest()
            rejected_wire = subprocess.run(
                wrong_wire, check=False, capture_output=True, text=True
            )
            self.assertNotEqual(0, rejected_wire.returncode)
            self.assertIn("fixed source/digest wire differs", rejected_wire.stderr)
            self.assertFalse(wire_archive.exists())
            selection_bytes[372] ^= 1
            selection.write_bytes(selection_bytes)

            repeated = subprocess.run(command, check=False, capture_output=True, text=True)
            self.assertNotEqual(0, repeated.returncode)
            self.assertIn("archive target already exists", repeated.stderr)

    def test_diagnostic_archiver_preserves_output_and_exact_junit_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            diagnostic = root / "diagnostic"
            diagnostic.mkdir()
            receipt = diagnostic / "v4-range1024-25ms-formal-sequence.json"
            receipt.write_text(
                '{"diagnosticOnly":true,"authority":false,"selectionEligible":false}\n',
                encoding="utf-8",
            )
            junit = root / "junit"
            junit.mkdir()
            xml = junit / "TEST-example.xml"
            xml.write_text(
                '<testsuite tests="2" failures="1" errors="0" skipped="0">'
                '<testcase name="passes"/><testcase name="fails"><failure/></testcase></testsuite>\n',
                encoding="utf-8",
            )
            (junit / "results.bin").write_bytes(b"exact-junit-binary")
            archive = root / "archive"
            command = [
                sys.executable,
                str(DIAGNOSTIC_ARCHIVER),
                "--diagnostic-output",
                str(diagnostic),
                "--junit-directory",
                str(junit),
                "--archive",
                str(archive),
                "--archived-on",
                "2026-08-28",
                "--source-commit",
                "a" * 40,
                "--plan-sha256",
                "b" * 64,
                "--executor-sha256",
                "c" * 64,
                "--run-id",
                "diagnostic-r1",
                "--expected-tests",
                "2",
                "--expected-failures",
                "1",
                "--expected-errors",
                "0",
                "--expected-skipped",
                "0",
                "--expected-suites",
                "1",
            ]

            subprocess.run(command, check=True, capture_output=True, text=True)
            identity = json.loads((archive / "archive-identity.json").read_bytes())
            self.assertEqual("FAILED", identity["diagnosticStatus"])
            self.assertEqual({"tests": 2, "failures": 1, "errors": 0, "skipped": 0, "suites": 1}, identity["junit"])
            self.assertFalse(identity["nadvPresent"])
            self.assertFalse(identity["promotableInput"])
            self.assertEqual(receipt.name, identity["rangeAttributionRelativePath"])
            self.assertEqual(hashlib.sha256(receipt.read_bytes()).hexdigest(), identity["rangeAttributionSha256"])
            self.assertEqual(
                [{"relativePath": receipt.name, "sha256": hashlib.sha256(receipt.read_bytes()).hexdigest()}],
                identity["rangeAttributions"],
            )
            self.assertEqual(receipt.read_bytes(), (archive / "payload" / "diagnostic-output" / receipt.name).read_bytes())
            self.assertEqual(xml.read_bytes(), (archive / "payload" / "junit" / xml.name).read_bytes())
            repeated = subprocess.run(command, check=False, capture_output=True, text=True)
            self.assertNotEqual(0, repeated.returncode)
            self.assertIn("archive target already exists", repeated.stderr)

    def test_diagnostic_archiver_preserves_multiple_range_attributions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            diagnostic = root / "diagnostic"
            diagnostic.mkdir()
            receipts = [
                diagnostic / "v5-range1024-10ms-formal-sequence.json",
                diagnostic / "v5-range1024-25ms-formal-sequence.json",
            ]
            for receipt in receipts:
                receipt.write_text(
                    '{"diagnosticOnly":true,"authority":false,"selectionEligible":false}\n',
                    encoding="utf-8",
                )
            (diagnostic / "receipt.nadv5").write_bytes(b"canonical-nadv5")
            junit = root / "junit"
            junit.mkdir()
            (junit / "TEST-example.xml").write_text(
                '<testsuite tests="1" failures="0" errors="0" skipped="0">'
                '<testcase name="passes"/></testsuite>\n',
                encoding="utf-8",
            )
            archive = root / "archive"
            subprocess.run(
                [
                    sys.executable,
                    str(DIAGNOSTIC_ARCHIVER),
                    "--diagnostic-output",
                    str(diagnostic),
                    "--junit-directory",
                    str(junit),
                    "--archive",
                    str(archive),
                    "--protocol-version",
                    "5",
                    "--diagnostic-status",
                    "PASSED",
                    "--archived-on",
                    "2026-08-29",
                    "--source-commit",
                    "a" * 40,
                    "--plan-sha256",
                    "b" * 64,
                    "--executor-sha256",
                    "c" * 64,
                    "--run-id",
                    "diagnostic-r1",
                    "--expected-tests",
                    "1",
                    "--expected-failures",
                    "0",
                    "--expected-errors",
                    "0",
                    "--expected-skipped",
                    "0",
                    "--expected-suites",
                    "1",
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            identity = json.loads((archive / "archive-identity.json").read_bytes())
            self.assertEqual(5, identity["protocolVersion"])
            self.assertEqual("PASSED", identity["diagnosticStatus"])
            self.assertTrue(identity["nadvPresent"])
            self.assertIsNone(identity["rangeAttributionRelativePath"])
            self.assertIsNone(identity["rangeAttributionSha256"])
            self.assertEqual(
                [
                    {
                        "relativePath": receipt.name,
                        "sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
                    }
                    for receipt in receipts
                ],
                identity["rangeAttributions"],
            )

            interrupted_diagnostic = root / "interrupted-diagnostic"
            interrupted_diagnostic.mkdir()
            (interrupted_diagnostic / "partial.json").write_text(
                '{"diagnosticOnly":true,"authority":false,"selectionEligible":false}\n',
                encoding="utf-8",
            )
            interrupted_junit = root / "interrupted-junit"
            interrupted_junit.mkdir()
            (interrupted_junit / "results-generic.bin").write_bytes(b"partial-junit-events")
            interrupted_archive = root / "interrupted-archive"
            subprocess.run(
                [
                    sys.executable,
                    str(DIAGNOSTIC_ARCHIVER),
                    "--diagnostic-output",
                    str(interrupted_diagnostic),
                    "--junit-directory",
                    str(interrupted_junit),
                    "--archive",
                    str(interrupted_archive),
                    "--protocol-version",
                    "5",
                    "--diagnostic-status",
                    "INTERRUPTED",
                    "--archived-on",
                    "2026-08-29",
                    "--source-commit",
                    "d" * 40,
                    "--plan-sha256",
                    "e" * 64,
                    "--executor-sha256",
                    "f" * 64,
                    "--run-id",
                    "interrupted-r1",
                    "--expected-tests",
                    "0",
                    "--expected-failures",
                    "0",
                    "--expected-errors",
                    "0",
                    "--expected-skipped",
                    "0",
                    "--expected-suites",
                    "0",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            interrupted_identity = json.loads(
                (interrupted_archive / "archive-identity.json").read_bytes()
            )
            self.assertEqual("INTERRUPTED", interrupted_identity["diagnosticStatus"])
            self.assertEqual(
                {"tests": 0, "failures": 0, "errors": 0, "skipped": 0, "suites": 0},
                interrupted_identity["junit"],
            )
            self.assertFalse(interrupted_identity["nadvPresent"])

    def test_plan_only_is_byte_stable_and_freezes_all_independent_budgets(self) -> None:
        first = subprocess.run(
            [str(RUNNER), "--plan-only"],
            cwd=ROOT,
            check=True,
            capture_output=True,
        ).stdout
        second = subprocess.run(
            [str(RUNNER), "--plan-only"],
            cwd=ROOT,
            check=True,
            capture_output=True,
        ).stdout
        self.assertEqual(first, second)

        plan = json.loads(first)
        self.assertEqual("NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_PLAN_V2", plan["schema"])
        self.assertEqual(288, plan["logicalIntervalCells"])
        self.assertEqual(13, plan["minimumValidEvaluationCells"])
        self.assertEqual(17, plan["minimumPromotableCells"])
        self.assertEqual(288, plan["maximumExecutedIntervalCells"])
        self.assertEqual(360, plan["maximumExecutedFaultActions"])
        self.assertEqual(32, plan["maximumExecutedScaleActions"])
        self.assertEqual(680, plan["maximumTotalExecutedActions"])
        self.assertEqual(48_000, plan["campaignWallClockCapSeconds"])
        self.assertEqual(
            "4fbeb2d43bd5865cb6139277a5021ed1b0762223f4983fc8fa50f8edc975ff08",
            plan["zeroDecisionPlanSha256"],
        )
        self.assertEqual(
            {
                "warmupSeconds": 10,
                "measuredSeconds": 30,
                "totalSeconds": 40,
                "minimumValidEvaluationSeconds": 520,
                "minimumPromotableSeconds": 680,
                "maximumSeconds": 11520,
            },
            plan["interval"],
        )
        self.assertEqual(
            {
                "setup": 900,
                "population": 5400,
                "fault": 7200,
                "scale": 5400,
                "interval": 11520,
                "cleanup": 1440,
                "checkpointResumeEvaluationSeal": 600,
            },
            plan["independentPhaseBudgetsSeconds"],
        )
        source = plan["exactSourceTuple"]
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
        ).stdout.strip()
        self.assertEqual(head, source["nereusCommit"])
        self.assertEqual(
            hashlib.sha256((ROOT / "docs/v2/source-locks.json").read_bytes()).hexdigest(),
            source["sourceLocksSha256"],
        )
        self.assertRegex(source["dependencyLockSha256"], r"^[0-9a-f]{64}$")
        self.assertEqual("7ff908330809f2e9bc5c69ead87bb85c566bc0a9", source["pulsarCommit"])
        self.assertEqual("37a17bef17202d5fd6e23282da5fd26d94865484", source["oxiaServerCommit"])
        self.assertEqual("091a42c2780d92da56e9ec1f02ce1c3d988adc16", source["oxiaClientCommit"])
        self.assertFalse(plan["budgetExhaustionMayCreateDisposition"])
        self.assertTrue(plan["evaluationRequiresCompletedCampaign"])
        self.assertTrue(plan["promotionRequiresUniqueQualifiedMode"])
        self.assertFalse(plan["accessesOxia"])
        self.assertFalse(plan["accessesPulsar"])
        self.assertFalse(plan["createsPopulation"])
        self.assertFalse(plan["writesEvidence"])

    def test_pre_campaign_forces_fresh_execution_on_the_dedicated_pulsar_composite(self) -> None:
        source = RUNNER.read_text()
        block = source.split('if [[ "${1:-}" == "--pre-campaign-check" ]]', 1)[1]
        block = block.split('if [[ "${1:-}" == "--validate-checkpoint" ]]', 1)[0]
        self.assertIn(":nereus-metadata-oxia:realAllocatorV2PreCampaignCheck", block)
        self.assertIn('"-PpulsarCheckout=$protocol_pulsar_checkout"', block)
        self.assertIn("--rerun-tasks", block)
        self.assertIn("--no-configuration-cache", block)

    def test_direct_allocator_tasks_default_to_the_dedicated_m3_pulsar_worktree(self) -> None:
        settings = SETTINGS.read_text()
        for token in (
            'task.startsWith("realAllocator")',
            'task.startsWith("v2M3Allocator")',
            'task.contains("RealAllocator")',
            'task.startsWith("validateRealAllocatorV2")',
            'task.startsWith("sealRealAllocatorV2")',
            '"The M3 module/API and allocator evidence gates require the dedicated Pulsar evidence worktree via "',
        ):
            self.assertIn(token, settings)
        self.assertIn(
            "val pulsarCheckout = if (m3DedicatedPulsarRequired)",
            settings,
        )

    def test_default_formal_path_remains_disabled_without_touching_oxia(self) -> None:
        result = subprocess.run(
            [str(RUNNER)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(64, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("full allocator execution is disabled", result.stderr)
        self.assertIn("immutable V1 runner", result.stderr)

    def test_legacy_gradle_full_execution_is_also_fail_closed(self) -> None:
        source = MODULE_BUILD.read_text()
        block = source.split(
            'val realAllocatorEvidenceTest = tasks.register<Test>("realAllocatorEvidenceTest")',
            1,
        )[1]
        block = block.split("val realAllocatorDomainJar", 1)[0]
        self.assertIn("legacy full allocator execution is disabled", block)
        self.assertIn("realAllocatorV2BoundedAdaptiveFormalCampaign entry", block)

    def test_bounded_adaptive_formal_path_requires_separate_authorization_before_preflight(self) -> None:
        environment = dict(os.environ)
        for name in tuple(environment):
            if name.startswith("NEREUS_M3_ALLOCATOR_V2_") or name.startswith(
                "NEREUS_M3_ALLOCATOR_OXIA_"
            ):
                environment.pop(name)
        result = subprocess.run(
            [str(RUNNER), "--bounded-adaptive-formal", str(ROOT / "build/never-created-formal")],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(0, result.returncode)
        self.assertIn("separately authorized exact SHA", result.stderr)
        self.assertFalse((ROOT / "build/never-created-formal").exists())

    def test_formal_entry_has_process_and_task_hard_deadlines(self) -> None:
        script = RUNNER.read_text()
        block = script.split('if [[ "${1:-}" == "--bounded-adaptive-formal" ]]', 1)[1]
        block = block.split('protocol_pulsar_checkout=', 1)[0]
        self.assertIn('scripts/run-v2-m3-with-hard-deadline.py', block)
        self.assertIn('--hard-deadline-seconds 48000', block)
        self.assertIn('--termination-grace-seconds 30', block)

        module = MODULE_BUILD.read_text()
        task = module.split(
            'val realAllocatorV2BoundedAdaptiveFormalCampaign = tasks.register<Test>(', 1
        )[1]
        task = task.split('val realAllocatorV2ShortDiagnosticTest', 1)[0]
        self.assertIn('timeout.set(Duration.ofSeconds(48_000))', task)

        started = time.monotonic()
        result = subprocess.run(
            [
                sys.executable,
                str(HARD_DEADLINE),
                "--hard-deadline-seconds",
                "0.4",
                "--termination-grace-seconds",
                "0.2",
                "--",
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(124, result.returncode)
        self.assertIn("hard-deadline supervisor", result.stderr)
        self.assertLess(time.monotonic() - started, 2.0)

    def test_formal_entry_passes_a_literal_docker_label_template(self) -> None:
        script = RUNNER.read_text()
        block = script.split('if [[ "${1:-}" == "--bounded-adaptive-formal" ]]', 1)[1]
        block = block.split('protocol_pulsar_checkout=', 1)[0]
        self.assertIn(
            "--format '{{index .Config.Labels \"org.opencontainers.image.revision\"}}'",
            block,
        )
        self.assertNotIn(r'\"org.opencontainers.image.revision\"', block)

    def test_formal_failure_result_and_junit_preserve_campaign_detail(self) -> None:
        source = FORMAL_CAMPAIGN.read_text()
        self.assertIn(r'\"terminalDetail\"', source)
        self.assertIn("jsonString(result.detail())", source)
        self.assertIn(".withFailMessage(", source)
        self.assertGreaterEqual(source.count("result.detail()"), 3)

    def test_formal_task_is_unique_and_not_reachable_from_ordinary_or_m3_gates(self) -> None:
        module = MODULE_BUILD.read_text()
        task = "realAllocatorV2BoundedAdaptiveFormalCampaign"
        self.assertEqual(1, module.count(f'val {task} = tasks.register<Test>('))
        self.assertNotIn(f"dependsOn({task}", module)
        self.assertNotIn(f'dependsOn("{task}"', module)
        root = ROOT_BUILD.read_text()
        self.assertNotIn(task, root)
        script = RUNNER.read_text()
        block = script.split('if [[ "${1:-}" == "--bounded-adaptive-formal" ]]', 1)[1]
        block = block.split('protocol_pulsar_checkout=', 1)[0]
        self.assertIn("NEREUS_M3_ALLOCATOR_V2_FORMAL_AUTHORIZATION_SHA", block)
        self.assertIn(":nereus-metadata-oxia:realAllocatorV2BoundedAdaptiveFormalCampaign", block)
        self.assertIn("v2M3AllocatorV2ZeroDecisionPlanSha256", block)
        self.assertIn("output directory is not empty", module)
        self.assertIn("V2InputAttachment", module)
        self.assertIn(
            'planOutput.contains("\\\"oxiaClientJarSha256\\\": \\\"$expectedOxiaClientJar\\\"")',
            module,
        )
        self.assertIn(
            'planOutput.contains("\\\"oxiaServerImageDigest\\\": \\\"$expectedOxiaImage\\\"")',
            module,
        )
        self.assertIn('required("v2M3AllocatorV2OxiaContainerName")', module)
        self.assertIn("com.nereusstream.evidence", module)
        self.assertIn("v2-m3-bounded-adaptive-formal", module)
        self.assertIn('"127.0.0.1:$oxiaBoundPort"', module)
        self.assertIn("v2M3AllocatorV2OxiaContainerName=$formal_container", block)

    def test_allocator_child_gate_defaults_to_v5_and_keeps_v1_v2_compatibility_separate(self) -> None:
        source = ROOT_BUILD.read_text()
        v2_block = source.split('tasks.register<Exec>("v2M3AllocatorV2VerificationSeal")', 1)[1]
        v2_block = v2_block.split('tasks.register("v2M3AllocatorV1CompatibilityCheck")', 1)[0]
        self.assertIn('dependsOn(":nereus-metadata-oxia:realAllocatorV2PromotionCheck")', v2_block)
        self.assertIn('"--seal-allocator-v2-verification"', v2_block)
        self.assertIn('"--allocator-v2-execution-attachment"', v2_block)
        v5_block = source.split('tasks.register<Exec>("v2M3AllocatorV5VerificationSeal")', 1)[1]
        v5_block = v5_block.split('tasks.register("v2M3AllocatorV1CompatibilityCheck")', 1)[0]
        self.assertIn('"--seal-allocator-v5-verification"', v5_block)
        self.assertIn('"--allocator-v5-diagnostic-junit"', v5_block)
        self.assertIn('"--allocator-v5-diagnostic-raw"', v5_block)
        self.assertIn('"--allocator-v5-execution-attachment"', v5_block)
        self.assertIn("32L * 1024L * 1024L", v5_block)
        default_block = source.split('tasks.register("v2M3AllocatorCheck")', 1)[1]
        default_block = default_block.split('val v2M3LocalCapEvidenceOutputDirectory', 1)[0]
        self.assertIn('dependsOn("v2M3AllocatorV5VerificationSeal")', default_block)
        self.assertNotIn('dependsOn("v2M3AllocatorV2VerificationSeal")', default_block)
        self.assertNotIn('dependsOn("v2M3AllocatorVerificationSeal")', default_block)


if __name__ == "__main__":
    unittest.main(verbosity=2)
