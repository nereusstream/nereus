#!/usr/bin/env python3
"""Offline fail-closed contract tests for the ADR-0104 allocator entrypoints."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
RUNNER = ROOT / "scripts" / "run-v2-m3-real-allocator-evidence.sh"
SETTINGS = ROOT / "settings.gradle.kts"


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
        self.assertEqual(288, plan["logicalPerformanceCells"])
        self.assertEqual(13, plan["executedPerformanceCellsMin"])
        self.assertEqual(17, plan["executedPerformanceCellsMinPromotable"])
        self.assertEqual(288, plan["executedPerformanceCellsMax"])
        self.assertEqual(
            {
                "warmupSeconds": 10,
                "measuredSeconds": 30,
                "totalSeconds": 40,
                "executedMinSeconds": 520,
                "executedMinDuration": "PT8M40S",
                "executedMinPromotableSeconds": 680,
                "executedMinPromotableDuration": "PT11M20S",
                "executedMaxSeconds": 11520,
                "executedMaxDuration": "PT3H12M",
            },
            plan["interval"],
        )
        self.assertEqual(
            {
                "setupSeconds": 900,
                "populationPathsMax": 6,
                "populationPerPathSeconds": 900,
                "populationTotalSeconds": 5400,
                "faultRowsMax": 40,
                "faultPerRowSeconds": 180,
                "faultTotalSeconds": 7200,
                "scalePathsMax": 6,
                "scalePerPathSeconds": 900,
                "scaleTotalSeconds": 5400,
                "cleanupPerExecutedCellSeconds": 5,
                "cleanupTotalMaxSeconds": 1440,
                "checkpointResumeSealSeconds": 600,
                "formalCampaignTotalMaxSeconds": 32460,
                "formalCampaignTotalMaxDuration": "PT9H1M",
            },
            plan["hardUpperBounds"],
        )
        self.assertFalse(plan["budgetExhaustionMayCreateDisposition"])
        self.assertTrue(plan["evaluationRequiresCompletedCampaign"])
        self.assertTrue(plan["promotionRequiresUniqueQualifiedMode"])
        self.assertFalse(plan["accessesOxia"])

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
