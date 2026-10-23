#!/usr/bin/env python3
"""Validate raw M5 JUnit attachments without granting source, child, or Final authority."""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import stat
import sys
import xml.etree.ElementTree as ET


MAX_XML_BYTES = 16_777_216
MAX_TOTAL_BYTES = 134_217_728
MAX_SUITES = 2048
MAX_CASES = 100_000
TASK = re.compile(r":[A-Za-z0-9_-]+:[A-Za-z0-9_-]+")
COUNT = re.compile(r"0|[1-9][0-9]{0,6}")
SHA = re.compile(r"[0-9a-f]{64}")
COUNTERS = ("tests", "failures", "errors", "skipped")


class JUnitEvidenceError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise JUnitEvidenceError(message)


def duration(value: str) -> int:
    require(isinstance(value, str) and len(value) <= 32, "invalid JUnit duration")
    try:
        seconds = Decimal(value)
        require(seconds.is_finite() and 0 <= seconds <= 86_400, "JUnit duration outside finite bound")
        return int(seconds * 1_000_000)
    except (InvalidOperation, ValueError, OverflowError) as error:
        raise JUnitEvidenceError("invalid JUnit duration") from error


def parse_suite(raw: bytes, task: str) -> dict:
    require(isinstance(task, str) and TASK.fullmatch(task) is not None, "invalid Gradle task identity")
    require(0 < len(raw) <= MAX_XML_BYTES, "JUnit XML outside finite byte bound")
    try:
        source = raw.decode("utf-8")
        require("\x00" not in source and not re.search(r"<!\s*(DOCTYPE|ENTITY)\b", source, re.I),
                "JUnit XML declarations or binary encoding are unsupported")
        declaration = re.match(r'\ufeff?\s*<\?xml\b(.*?)\?>', source, re.S)
        if declaration:
            encoding = re.search(r'''encoding\s*=\s*["']([^"']+)["']''', declaration.group(1), re.I)
            require(encoding is None or encoding.group(1).lower() in {"utf-8", "utf8"},
                    "JUnit XML must use UTF-8")
        root = ET.fromstring(source)
    except (UnicodeError, ET.ParseError) as error:
        raise JUnitEvidenceError("invalid JUnit XML") from error
    require(root.tag == "testsuite" and bool(root.get("name")), "expected one named Gradle testsuite")
    require(all(child.tag in {"properties", "testcase", "system-out", "system-err"} for child in root),
            "unsupported or nested JUnit suite member")
    declared = {}
    for key in COUNTERS:
        value = root.get(key)
        require(isinstance(value, str) and COUNT.fullmatch(value) is not None,
                f"invalid declared JUnit {key}")
        declared[key] = int(value)
    cases = root.findall("testcase")
    require(0 < len(cases) <= MAX_CASES, "JUnit suite has zero or excessive cases")
    actual = dict.fromkeys(COUNTERS, 0)
    identities = []
    duration_micros = 0
    seen = set()
    for case in cases:
        name, classname = case.get("name"), case.get("classname")
        require(isinstance(name, str) and bool(name) and isinstance(classname, str) and bool(classname),
                "JUnit testcase lacks exact class/name identity")
        identity = (task, root.get("name"), classname, name)
        require(identity not in seen, "duplicate JUnit testcase identity")
        seen.add(identity)
        require(all(child.tag in {"failure", "error", "skipped", "system-out", "system-err"} for child in case),
                "unsupported JUnit testcase member")
        terminal = [child.tag for child in case if child.tag in {"failure", "error", "skipped"}]
        require(len(terminal) <= 1, "ambiguous JUnit testcase terminal status")
        status = terminal[0] if terminal else "passed"
        actual["tests"] += 1
        if terminal:
            actual[{"failure": "failures", "error": "errors", "skipped": "skipped"}[status]] += 1
        elapsed = duration(case.get("time", "0"))
        duration_micros += elapsed
        identities.append({"task": task, "suite": root.get("name"), "class": classname,
                           "name": name, "status": status, "durationMicros": elapsed})
    require(actual == declared, "declared JUnit counters differ from actual testcase statuses")
    if root.get("time") is not None:
        duration(root.get("time"))
    return {"summary": actual, "durationMicros": duration_micros, "cases": identities}


def validate_suites(rows: list[dict], read) -> dict:
    """Read every exact raw attachment; callers still must prove execution, semantics and source closure."""
    require(isinstance(rows, list) and 0 < len(rows) <= MAX_SUITES, "JUnit suite inventory outside bound")
    totals = dict.fromkeys(COUNTERS, 0)
    cases = []
    paths, identities = set(), set()
    total_bytes = 0
    elapsed = 0
    for row in rows:
        require(isinstance(row, dict) and set(row) == {"path", "task", "bytes", "sha256"},
                "JUnit attachment members differ")
        require(isinstance(row["path"], str) and bool(row["path"]) and row["path"] not in paths,
                "missing or duplicate JUnit attachment path")
        paths.add(row["path"])
        require(type(row["bytes"]) is int and 0 < row["bytes"] <= MAX_XML_BYTES
                and isinstance(row["sha256"], str) and SHA.fullmatch(row["sha256"]) is not None,
                "JUnit attachment length or digest invalid")
        total_bytes += row["bytes"]
        require(total_bytes <= MAX_TOTAL_BYTES, "JUnit attachment inventory exceeds byte bound")
        raw = read(row["path"])
        require(isinstance(raw, bytes) and len(raw) == row["bytes"]
                and hashlib.sha256(raw).hexdigest() == row["sha256"], "JUnit attachment bytes differ")
        suite = parse_suite(raw, row["task"])
        require(all(suite["summary"][key] == 0 for key in ("failures", "errors", "skipped")),
                "failed, errored or skipped JUnit cases cannot qualify an attachment")
        for case in suite["cases"]:
            identity = tuple(case[key] for key in ("task", "class", "name"))
            require(identity not in identities, "JUnit testcase was attached more than once")
            identities.add(identity)
        cases.extend(suite["cases"])
        require(len(cases) <= MAX_CASES, "JUnit total case inventory exceeds bound")
        elapsed += suite["durationMicros"]
        for key in COUNTERS:
            totals[key] += suite["summary"][key]
    return {"schema": "NEREUS_V2_M5_JUNIT_ATTACHMENT_CHECK_V1", "result": "STRUCTURE_AND_COUNTS_VERIFIED_ONLY",
            "executionEvidence": False, "promotionEligible": False, "sourceQualified": False,
            "testSummary": totals, "durationMicros": elapsed, "suites": rows, "cases": cases}


def regular(path: Path) -> bytes:
    path = path.absolute()
    require(all(not ancestor.is_symlink() for ancestor in (path, *path.parents)),
            "JUnit attachment path contains a symlink")
    require(stat.S_ISREG(path.lstat().st_mode) and path.stat().st_size <= MAX_XML_BYTES,
            "JUnit attachment is not a bounded regular file")
    raw = path.read_bytes()
    require(len(raw) <= MAX_XML_BYTES, "JUnit attachment grew beyond bound")
    return raw


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", action="append", required=True, metavar="GRADLE_TASK=XML_PATH")
    args = parser.parse_args()
    rows = []
    for supplied in args.suite:
        task, separator, name = supplied.partition("=")
        require(bool(separator) and bool(name), "--suite requires GRADLE_TASK=XML_PATH")
        path = Path(name).absolute()
        raw = regular(path)
        rows.append({"path": str(path), "task": task, "bytes": len(raw),
                     "sha256": hashlib.sha256(raw).hexdigest()})
    value = validate_suites(rows, lambda name: regular(Path(name)))
    print(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except (JUnitEvidenceError, OSError, ValueError) as error:
        print(f"M5 JUnit attachment rejected: {error}", file=sys.stderr)
        raise SystemExit(1)
