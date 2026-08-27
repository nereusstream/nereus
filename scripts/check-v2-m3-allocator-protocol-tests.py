#!/usr/bin/env python3
"""Offline fail-closed contract tests for the ADR-0104 allocator entrypoints."""

from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
RUNNER = ROOT / "scripts" / "run-v2-m3-real-allocator-evidence.sh"
SETTINGS = ROOT / "settings.gradle.kts"
ROOT_BUILD = ROOT / "build.gradle.kts"
MODULE_BUILD = ROOT / "nereus-metadata-oxia" / "build.gradle.kts"


class M3AllocatorProtocolConfigurationTest(unittest.TestCase):
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
