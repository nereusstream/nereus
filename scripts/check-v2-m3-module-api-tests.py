#!/usr/bin/env python3
"""Fail-closed configuration contract for the M3 module/API source gate."""

from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build.gradle.kts"
SETTINGS = ROOT / "settings.gradle.kts"
TASK_START = 'tasks.register<Exec>("v2M3ModuleApiSourceCheck") {'
TASK_END = 'tasks.register("v2M3ModuleApiCheck") {'


class M3ModuleApiConfigurationTest(unittest.TestCase):
    def test_authoritative_source_gate_defaults_to_the_dedicated_m3_pulsar_worktree(self) -> None:
        source = BUILD.read_text()
        self.assertEqual(1, source.count(TASK_START))
        self.assertEqual(1, source.count(TASK_END))
        task = source.split(TASK_START, 1)[1].split(TASK_END, 1)[0]

        self.assertIn(
            'commandLine("bash", "scripts/check-v2-m3-module-api.sh", '
            'v2M3PulsarEvidenceWorktree.get())',
            task,
        )
        self.assertNotIn("pulsarCheckoutPath.get()", task)
        self.assertIn(
            'providers.gradleProperty("v2M3PulsarEvidenceWorktree")',
            source,
        )
        self.assertIn(
            'providers.environmentVariable("NEREUS_M3_PULSAR_EVIDENCE_WORKTREE")',
            source,
        )
        self.assertIn(
            '"../../nereusstream/pulsar-worktrees/nereus-v2-m3"',
            source,
        )

        settings = SETTINGS.read_text()
        self.assertIn("val configuredM3PulsarEvidenceWorktree =", settings)
        self.assertIn("val conventionalM3PulsarEvidenceWorktree =", settings)
        self.assertIn("val m3PulsarEvidenceWorktree =", settings)
        self.assertIn(
            "val pulsarCheckout = if (m3DedicatedPulsarRequired)",
            settings,
        )
        self.assertIn(
            "m3DedicatedPulsarRequired && it.resolve(\"settings.gradle.kts\").isFile",
            settings,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
