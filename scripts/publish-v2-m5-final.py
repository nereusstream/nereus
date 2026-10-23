#!/usr/bin/env python3
"""Bind an already-validated complete M5 execution to equivalent commits; never execute tests or push Git."""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile


SPEC = importlib.util.spec_from_file_location("m5_final_contract", Path(__file__).with_name("check-v2-m5-evidence.py"))
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)


def reference(root, path):
    raw = C.read(root, path)
    return {"path": path, "bytes": len(raw), "sha256": C.INPUTS.digest(raw)}


def write_new(root, path, raw):
    destination = root / C.relative(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as output:
        output.write(raw)


def proposed_scenarios(raw, final_path):
    value = json.loads(raw, object_pairs_hook=C.INPUTS.pairs)
    rows = value.get("scenarios")
    C.require(isinstance(rows, list), "scenario registry is missing")
    by_id = {row["id"]: row for row in rows}
    C.require(len(by_id) == len(rows), "duplicate scenario row")
    for identity in C.PROMOTED:
        C.require(identity in by_id, "missing M5 scenario")
        by_id[identity]["status"] = "PASSED_CURRENT_SOURCE"
        by_id[identity]["evidenceReceipt"] = str(final_path)
    for identity in C.DEFERRED:
        C.require(by_id[identity]["status"] == "PLANNED" and by_id[identity]["evidenceReceipt"] is None,
                  "M6-deferred predicate was already promoted")
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()


def assemble(candidate, prepared_inputs, roots, commits, output):
    source_workspace = prepared_inputs / "workspace" / C.INPUTS.LAYOUT["nereus"]
    C.validate_candidate(candidate, prepared_inputs, source_workspace)
    inputs = C.INPUTS.verify(prepared_inputs)
    binding = C.INPUTS.bind(prepared_inputs, roots, commits)
    original = C.load(C.read(candidate, "candidate.json", C.MAX_JSON))
    final = copy.deepcopy(original)
    final.update(schema="NEREUS_V2_M5_FINAL_V1", kind="V2_M5_FINAL", result="PASS_V2_M5_FINAL",
                 promotionEligible=True, sourceTuple={"commits": commits, "sourceLocksSha256": original["sourceLocksSha256"]})
    for path in sorted(C.inventory(candidate, "")):
        if path == "candidate.json" or path.startswith("children/") and path.endswith("/receipt.json"):
            continue
        write_new(output, path, C.read(candidate, path))
    for row in final["childReceipts"]:
        child = C.load(C.read(candidate, row["path"], C.MAX_JSON))
        child["sourceTuple"] = final["sourceTuple"]
        write_new(output, row["path"], C.canonical(child))
        row.update(reference(output, row["path"]))
    write_new(output, "commit-binding.json", C.canonical(binding))
    # Committed source objects remain independently readable in Git; archive every other actual input once.
    versioned = {str(Path(repo["sourceRoot"]) / path): record for repo in inputs["repositories"].values()
                 for path, record in repo["files"].items() if record.get("mode") in ("100644", "100755")}
    objects = set()
    for source, record in inputs["extraInputs"].items():
        tracked = versioned.get(source)
        if tracked is not None and {key: tracked[key] for key in ("bytes", "sha256")} == record:
            continue
        if record["sha256"] not in objects:
            write_new(output, "input-objects/" + record["sha256"],
                      C.INPUTS.regular(prepared_inputs / "objects" / record["sha256"]))
            objects.add(record["sha256"])
    write_new(output, "m5-final.json", C.canonical(final))
    C.validate_committed_inputs(output, inputs, roots, commits)
    C.validate_m4(roots["nereus"])
    C.validate_package(output, final, inputs, commits)
    return final


def publish(candidate, prepared_inputs, roots, commits, promote_scenarios):
    repository = roots["nereus"]
    final_path = C.EVIDENCE_PREFIX / "final" / f"final-source-{commits['nereus']}" / "m5-final.json"
    destination = repository / final_path.parent
    C.require(not destination.exists() and not destination.is_symlink(), "Final publisher refuses to overwrite an artifact")
    C.require(promote_scenarios, "Final publication requires the explicit complete scenario-group update")
    original_scenarios = C.read(repository, str(C.SCENARIOS), C.MAX_JSON)
    inputs = C.INPUTS.verify(prepared_inputs)
    C.require(C.INPUTS.digest(original_scenarios) == C.source_node(inputs, "nereus", str(C.SCENARIOS))["sha256"],
              "scenario source changed since the complete candidate")
    scenario_bytes = proposed_scenarios(original_scenarios, final_path)
    build = repository / "build"
    build.mkdir(exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix="m5-final-build-", dir=build))
    try:
        final = assemble(candidate, prepared_inputs, roots, commits, temporary)
        scenario_check = temporary / "scenario-group-check"
        write_new(scenario_check, str(C.SCENARIOS), scenario_bytes)
        C.validate_scenarios(scenario_check, final_path)
        shutil.rmtree(scenario_check)
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Claim the destination exclusively. Publish Final last, so a partial archive cannot be
        # selected as a complete Final and an existing empty directory is never replaced.
        destination.mkdir()
        for path in sorted(C.inventory(temporary, "") - {"m5-final.json"}):
            write_new(destination, path, C.read(temporary, path))
        write_new(destination, "m5-final.json", C.read(temporary, "m5-final.json"))
        # This is the separately authorized publication action, never a side effect of evidence execution.
        C.require(C.read(repository, str(C.SCENARIOS), C.MAX_JSON) == original_scenarios,
                  "scenario registry changed during Final preparation")
        (repository / C.SCENARIOS).write_bytes(scenario_bytes)
        return final_path, final
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--input-closure", type=Path, required=True)
    for name in C.INPUTS.REPOSITORIES:
        parser.add_argument(f"--{name}-root", type=Path, required=True)
        parser.add_argument(f"--{name}-commit", required=True)
    parser.add_argument("--promote-scenarios", action="store_true")
    args = parser.parse_args(argv)
    try:
        roots = {name: getattr(args, name + "_root").resolve(strict=True) for name in C.INPUTS.REPOSITORIES}
        commits = {name: getattr(args, name + "_commit") for name in C.INPUTS.REPOSITORIES}
        path, final = publish(args.candidate.resolve(strict=True), args.input_closure.resolve(strict=True),
                              roots, commits, args.promote_scenarios)
    except (C.EvidenceError, C.INPUTS.ClosureError, C.JUNIT.JUnitEvidenceError, C.M4.EvidenceError,
            C.HISTORICAL_M4.DependencyError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"M5 Final publication: {error}", file=sys.stderr)
        return 1
    print(f"M5 Final prepared: tested={commits['nereus']} path={path} children=5 tests={final['summary']['tests']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
