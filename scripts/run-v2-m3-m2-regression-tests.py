#!/usr/bin/env python3
"""Preflight and closed-plan tests for the current-source M2 regression runner."""

from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import socket
import shutil
import subprocess
import tempfile
import unittest


RUNNER = Path(__file__).with_name("run-v2-m3-m2-regression.sh")
CONTRACT = Path(__file__).with_name("check-v2-m3-m2-regression.py")
PROJECTION = Path(__file__).with_name("check-v2-m3-m2-source-lock-projection.py")


def run(*args: str, cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*args], cwd=cwd, env=env, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, check=False,
    )


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


class Fixture:
    def __init__(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-m3-m2-runner-")
        self.base = Path(self.temporary.name)
        self.repo = self.base / "nereus"
        self.repo.mkdir()
        git(self.repo, "init", "-b", "main")
        git(self.repo, "config", "user.name", "M3 M2 Runner Test")
        git(self.repo, "config", "user.email", "m3-m2-runner@example.invalid")
        scripts = self.repo / "scripts"
        scripts.mkdir()
        shutil.copy2(CONTRACT, scripts / CONTRACT.name)
        shutil.copy2(PROJECTION, scripts / PROJECTION.name)
        self.kafka_repo, self.kafka_worktree, kafka = self.source("kafka", "m2-kafka")
        self.pulsar_repo, self.pulsar_worktree, pulsar = self.source("pulsar", "m2-pulsar")
        locks = {
            "m2KafkaK0InputSourceBinding": {
                "kafkaInput": {
                    "repository": self.kafka_repo.as_uri(), "branch": "m2-kafka",
                    "forkCommit": kafka, "implementationBaseCommit": kafka,
                },
                "bookKeeperInput": {
                    "serverImageReference":
                        "apache/bookkeeper@sha256:" + "1" * 64,
                    "serverImageConfigDigest": "sha256:" + "2" * 64,
                },
            },
            "m2PulsarNativeBinding": {
                "repository": self.pulsar_repo.as_uri(), "branch": "m2-pulsar",
                "finalForkCommit": pulsar, "implementationBaseCommit": pulsar,
            },
            "historicalUnrelated": {"fixture": "immutable"},
        }
        lock_path = self.repo / "docs/v2/source-locks.json"
        lock_path.parent.mkdir(parents=True)
        lock_path.write_text(json.dumps(locks) + "\n")
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-m", "historical M2 source locks")
        historical = git(self.repo, "rev-parse", "HEAD")
        historical_sha = hashlib.sha256(lock_path.read_bytes()).hexdigest()
        receipt_path = self.repo / "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json"
        receipt_path.parent.mkdir(parents=True)
        receipt_path.write_text(json.dumps({
            "promotionEligible": False,
            "result": "PASS_KAFKA_M2_INPUTS_ONLY",
            "schema": "NEREUS_V2_M2_KAFKA_INPUTS_RECEIPT_V1",
            "sourceTuple": {
                "nereusCommit": historical,
                "sourceLocksSha256": historical_sha,
            },
        }) + "\n")
        locks["m3AllocatorEvidenceBinding"] = {"fixture": "current-only"}
        lock_path.write_text(json.dumps(locks) + "\n")
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-m", "fixture current M3 source")
        self.tested = git(self.repo, "rev-parse", "HEAD")
        self.output = self.base / "external-output"
        self.docker_socket_path = self.base / "docker.sock"
        self.docker_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.docker_socket.bind(str(self.docker_socket_path))
        self.fake_docker = self.base / "docker"
        self.fake_docker.write_text(
            "#!/usr/bin/env bash\n"
            "set -eu\n"
            "if [[ \"$1 $2\" == 'compose version' ]]; then echo 'Docker Compose version v2-test'; exit 0; fi\n"
            "if [[ \"$1 $2\" == 'context show' ]]; then echo 'fixture'; exit 0; fi\n"
            "if [[ \"$1 $2\" == 'context inspect' ]]; then echo 'unix://" + str(self.docker_socket_path) + "'; exit 0; fi\n"
            "if [[ \"$1 $2\" == 'version --format' ]]; then echo '1.55 1.40'; exit 0; fi\n"
            "if [[ \"$1 $2 $3\" == 'image inspect --format' ]]; then\n"
            "  format=$4; ref=$5\n"
            "  case \"$ref\" in\n"
            "    apache/bookkeeper@sha256:*) id='sha256:" + "2" * 64 + "'; digest=\"$ref\" ;;\n"
            "    localstack/localstack:4.14.0) id='sha256:ad76d8f93de9cb765653983d32f4b2994ca981b8f6ccfcf7b52b2d1800b18581'; digest='localstack/localstack@sha256:3ebc37595918b8accb852f8048fef2aff047d465167edd655528065b07bc364a' ;;\n"
            "    quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z) id='sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253'; digest='quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e' ;;\n"
            "    *) exit 2 ;;\n"
            "  esac\n"
            "  if [[ \"$format\" == '{{.Id}}' ]]; then echo \"$id\"; else echo \"$digest\"; fi\n"
            "  exit 0\n"
            "fi\n"
            "exit 2\n"
        )
        self.fake_docker.chmod(0o755)

    def source(self, name: str, branch: str) -> tuple[Path, Path, str]:
        repository = self.base / f"{name}.git"
        subprocess.check_call(["git", "init", "--bare", str(repository)], stdout=subprocess.DEVNULL)
        seed = self.base / f"{name}-seed"
        seed.mkdir()
        git(seed, "init", "-b", branch)
        git(seed, "config", "user.name", "M3 M2 Runner Test")
        git(seed, "config", "user.email", "m3-m2-runner@example.invalid")
        (seed / "source.txt").write_text(name + "\n")
        git(seed, "add", ".")
        git(seed, "commit", "-m", "source")
        git(seed, "remote", "add", "origin", repository.as_uri())
        git(seed, "push", "-u", "origin", branch)
        commit = git(seed, "rev-parse", "HEAD")
        git(seed, "checkout", "--detach", commit)
        worktree = self.base / f"{name}-worktree"
        git(seed, "worktree", "add", str(worktree), branch)
        return repository, worktree, commit

    def cleanup(self) -> None:
        self.docker_socket.close()
        self.temporary.cleanup()

    def command(self, *extra: str) -> list[str]:
        return [
            "bash", str(RUNNER), "--dry-run", "--repo-root", str(self.repo),
            "--tested-commit", self.tested, "--kafka-worktree", str(self.kafka_worktree),
            "--pulsar-worktree", str(self.pulsar_worktree), "--output-dir", str(self.output),
            *extra,
        ]

    def environment(self) -> dict[str, str]:
        return {**os.environ, "NEREUS_M3_DOCKER_BIN": str(self.fake_docker)}

    def use_fixed_evidence_branches(self) -> None:
        evidence_branch = "nereus/v2-m3-m2-regression-evidence"
        for worktree in (self.kafka_worktree, self.pulsar_worktree):
            git(worktree, "checkout", "-b", evidence_branch)
            git(worktree, "push", "-u", "origin", evidence_branch)


class RunnerPreflightTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = Fixture()

    def tearDown(self) -> None:
        self.fixture.cleanup()

    def test_dry_run_closes_all_25_gates_without_writing(self) -> None:
        result = run(*self.fixture.command(), env=self.fixture.environment())
        self.assertEqual(0, result.returncode, result.stdout)
        rows = [line for line in result.stdout.splitlines() if line.startswith(("KAFKA_", "PULSAR_"))]
        self.assertEqual(25, len(rows))
        self.assertEqual(25, len({line.split("|", 1)[0] for line in rows}))
        self.assertIn("formalRuns=0", result.stdout)
        self.assertFalse(self.fixture.output.exists())

    def test_dry_run_accepts_only_pushed_fixed_evidence_branches(self) -> None:
        self.fixture.use_fixed_evidence_branches()
        result = run(*self.fixture.command(), env=self.fixture.environment())
        self.assertEqual(0, result.returncode, result.stdout)

        git(self.fixture.kafka_worktree, "checkout", "-b", "unapproved-evidence")
        result = run(*self.fixture.command(), env=self.fixture.environment())
        self.assertNotEqual(0, result.returncode)
        self.assertIn("neither the source lock nor the fixed M3 evidence branch", result.stdout)

    def test_rejects_shared_checkout_dirty_source_and_existing_output(self) -> None:
        shared = self.fixture.base / "kafka-seed"
        command = self.fixture.command()
        command[command.index(str(self.fixture.kafka_worktree))] = str(shared)
        result = run(*command, env=self.fixture.environment())
        self.assertNotEqual(0, result.returncode)
        self.assertIn("dedicated linked Git worktree", result.stdout)

        (self.fixture.repo / "dirty.txt").write_text("dirty\n")
        result = run(*self.fixture.command(), env=self.fixture.environment())
        self.assertNotEqual(0, result.returncode)
        self.assertIn("must be clean", result.stdout)
        (self.fixture.repo / "dirty.txt").unlink()

        self.fixture.output.mkdir()
        result = run(*self.fixture.command(), env=self.fixture.environment())
        self.assertNotEqual(0, result.returncode)
        self.assertIn("must not already exist", result.stdout)

    def test_rejects_source_and_image_identity_drift(self) -> None:
        (self.fixture.kafka_worktree / "dirty.txt").write_text("dirty\n")
        result = run(*self.fixture.command(), env=self.fixture.environment())
        self.assertNotEqual(0, result.returncode)
        self.assertIn("dedicated worktree is not clean", result.stdout)
        (self.fixture.kafka_worktree / "dirty.txt").unlink()

        bad_docker = self.fixture.base / "bad-docker"
        bad_docker.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ \"$1 $2\" == 'compose version' ]]; then exit 0; fi\n"
            "if [[ \"$1 $2\" == 'context show' ]]; then echo 'fixture'; exit 0; fi\n"
            "if [[ \"$1 $2\" == 'context inspect' ]]; then echo 'unix://" + str(self.fixture.docker_socket_path) + "'; exit 0; fi\n"
            "if [[ \"$1 $2\" == 'version --format' ]]; then echo '1.55 1.40'; exit 0; fi\n"
            "if [[ \"$1 $2 $3\" == 'image inspect --format' ]]; then echo 'sha256:bad'; exit 0; fi\n"
            "exit 2\n"
        )
        bad_docker.chmod(0o755)
        environment = {**os.environ, "NEREUS_M3_DOCKER_BIN": str(bad_docker)}
        result = run(*self.fixture.command(), env=environment)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Docker image ID differs", result.stdout)

    def test_rejects_docker_server_that_excludes_fixed_api(self) -> None:
        incompatible_docker = self.fixture.base / "incompatible-docker"
        incompatible_docker.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ \"$1 $2\" == 'version --format' ]]; then echo '1.55 1.45'; exit 0; fi\n"
            "exec \"$NEREUS_M3_FIXTURE_DOCKER\" \"$@\"\n"
        )
        incompatible_docker.chmod(0o755)
        environment = {
            **os.environ,
            "NEREUS_M3_DOCKER_BIN": str(incompatible_docker),
            "NEREUS_M3_FIXTURE_DOCKER": str(self.fixture.fake_docker),
        }
        result = run(*self.fixture.command(), env=environment)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("does not admit the fixed Testcontainers API 1.44", result.stdout)

    def test_native_pulsar_execution_is_rooted_in_dedicated_worktree(self) -> None:
        runner = RUNNER.read_text()
        self.assertIn(
            '"$pulsar_worktree/gradlew" --project-dir "$pulsar_worktree"',
            runner,
        )
        self.assertNotIn(
            '"$pulsar_worktree/gradlew" :tiered-storage:jcloud:test',
            runner,
        )
        self.assertIn(
            ':tiered-storage:tiered-storage-jcloud:test',
            runner,
        )
        self.assertNotIn(
            ':tiered-storage:jcloud:test',
            runner,
        )

    def test_rejects_unapproved_current_only_source_lock_member(self) -> None:
        lock_path = self.fixture.repo / "docs/v2/source-locks.json"
        locks = json.loads(lock_path.read_text())
        locks["unapprovedM3Binding"] = {"fixture": "drift"}
        lock_path.write_text(json.dumps(locks) + "\n")
        git(self.fixture.repo, "add", str(lock_path))
        git(self.fixture.repo, "commit", "-m", "add unapproved source lock")
        self.fixture.tested = git(self.fixture.repo, "rev-parse", "HEAD")
        result = run(*self.fixture.command(), env=self.fixture.environment())
        self.assertNotEqual(0, result.returncode)
        self.assertIn("exact M3-only allowlist", result.stdout)

    def test_rejects_historical_source_lock_member_drift(self) -> None:
        lock_path = self.fixture.repo / "docs/v2/source-locks.json"
        locks = json.loads(lock_path.read_text())
        locks["historicalUnrelated"]["fixture"] = "changed"
        lock_path.write_text(json.dumps(locks) + "\n")
        git(self.fixture.repo, "add", str(lock_path))
        git(self.fixture.repo, "commit", "-m", "change historical source lock")
        self.fixture.tested = git(self.fixture.repo, "rev-parse", "HEAD")
        result = run(*self.fixture.command(), env=self.fixture.environment())
        self.assertNotEqual(0, result.returncode)
        self.assertIn("exact M3-only allowlist", result.stdout)


if __name__ == "__main__":
    unittest.main()
