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
V3_PLAN = ROOT / "scripts" / "v2-m3-allocator-plan-v3.py"
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

    def test_allocator_child_gate_defaults_to_v2_and_keeps_v1_compatibility_separate(self) -> None:
        source = ROOT_BUILD.read_text()
        v2_block = source.split('tasks.register<Exec>("v2M3AllocatorV2VerificationSeal")', 1)[1]
        v2_block = v2_block.split('tasks.register("v2M3AllocatorV1CompatibilityCheck")', 1)[0]
        self.assertIn('dependsOn(":nereus-metadata-oxia:realAllocatorV2PromotionCheck")', v2_block)
        self.assertIn('"--seal-allocator-v2-verification"', v2_block)
        self.assertIn('"--allocator-v2-execution-attachment"', v2_block)
        default_block = source.split('tasks.register("v2M3AllocatorCheck")', 1)[1]
        default_block = default_block.split('val v2M3LocalCapEvidenceOutputDirectory', 1)[0]
        self.assertIn('dependsOn("v2M3AllocatorV2VerificationSeal")', default_block)
        self.assertNotIn('dependsOn("v2M3AllocatorVerificationSeal")', default_block)


if __name__ == "__main__":
    unittest.main(verbosity=2)
