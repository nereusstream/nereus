#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
v2_dir="$repo_root/docs/v2"

fail() {
    echo "V2 documentation check: $*" >&2
    exit 1
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        fail "missing '$literal' in $path"
    fi
}

required_v2_docs=(
    README.md
    01-correctness-and-append.md
    02-storage-profiles-and-topic-binding.md
    03-object-wal.md
    04-bookkeeper-and-pulsar.md
    05-manifest-read-retention-gc.md
    06-metadata-backends-and-handoff.md
    07-protocol-integrations.md
    08-implementation-plan-and-gates.md
    09-scenario-evidence-matrix.md
    open-questions.md
    tradeoffs.md
)

[[ -f "$v2_dir/source-locks.json" ]] || fail "missing docs/v2/source-locks.json"
[[ -f "$v2_dir/v2-scenarios.json" ]] || fail "missing docs/v2/v2-scenarios.json"
source_tuple="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["sourceTupleId"])' "$v2_dir/source-locks.json")"
[[ "$source_tuple" =~ ^v2-m[0-9]+$ ]] || fail "invalid current source tuple $source_tuple"

for name in "${required_v2_docs[@]}"; do
    path="$v2_dir/$name"
    [[ -f "$path" ]] || fail "missing docs/v2/$name"
    [[ "$(head -n 1 "$path")" == "---" ]] || fail "docs/v2/$name has no front matter"
    rg -q '^productLine: V2$' "$path" || fail "docs/v2/$name has the wrong product line"
    rg -q '^designStatus: (Accepted|Proposed)$' "$path" || fail "docs/v2/$name has an invalid design status"
    rg -q '^implementationStatus: (NotStarted|InProgress|Verified)$' "$path" || fail "docs/v2/$name has an invalid implementation status"
    rg -q '^evidenceStatus: (NotRun|DocumentationOnly|CurrentSourceReceipt)$' "$path" || fail "docs/v2/$name has an invalid evidence status"
    rg -q '^authority: [A-Za-z][A-Za-z]+$' "$path" || fail "docs/v2/$name has no authority"
    rg -q "^sourceTuple: ${source_tuple}$" "$path" || fail "docs/v2/$name does not use source tuple $source_tuple"

    if rg -q '^implementationStatus: Verified$' "$path"; then
        rg -q '^evidenceStatus: CurrentSourceReceipt$' "$path" || fail "docs/v2/$name is Verified without current-source evidence"
        rg -q '^receipt: docs/v2/evidence/[^[:space:]]+$' "$path" || fail "docs/v2/$name is Verified without an append-only receipt path"
    fi
done

for path in "$repo_root/docs/design/nereus-design-index.md" "$repo_root/docs/design/nereus-overall-architecture.md"; do
    rg -q "^sourceTuple: ${source_tuple}$" "$path" || fail "${path#"$repo_root/"} does not use source tuple $source_tuple"
done

required_domain_docs=(
    "$repo_root/CONTEXT-MAP.md"
    "$repo_root/docs/domain/shared-storage/CONTEXT.md"
    "$repo_root/docs/domain/kafka/CONTEXT.md"
    "$repo_root/docs/domain/pulsar/CONTEXT.md"
    "$repo_root/docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
    "$repo_root/docs/decisions/0012-v2-storage-epochs-and-profile-evolution.md"
    "$repo_root/docs/decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md"
)
for path in "${required_domain_docs[@]}"; do
    [[ -f "$path" ]] || fail "missing ${path#"$repo_root/"}"
done

[[ -f "$repo_root/docs/design/nereus-future5-kop-compatibility.md" ]] || fail "KoP design document was removed"

require_literal "nereusVersion=0.2.0-SNAPSHOT" "gradle.properties"
require_literal "Designed / deferred from the 0.2 runtime and release gates" "docs/design/nereus-future5-kop-compatibility.md"
require_literal "v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b" "docs/design/nereus-design-index.md"
require_literal "191fbbe5a0430cc4c88b9a2be61cb5a492ec3494" "docs/v2/source-locks.json"
require_literal "v2DocumentationCheck" "build.gradle.kts"
require_literal "v2M0Check" "build.gradle.kts"
require_literal "V2 documentation baseline" ".github/workflows/build.yml"
require_literal "Superseded by ADR 0012." "docs/decisions/0010-v2-topic-profile-binding.md"
require_literal "Kafka and Pulsar do not share a universal numeric position" "docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
require_literal 'V2 does not persist `ledgerBase + entryId`' "docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
require_literal "At most one epoch admits new positions at a time" "docs/decisions/0012-v2-storage-epochs-and-profile-evolution.md"
require_literal "simultaneous native writers for one Topic Incarnation" "docs/decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md"
require_literal "NonNormativeQuestionLog" "docs/v2/open-questions.md"
require_literal "NonNormativeSessionRecord" "docs/v2/grill-notes/01-protocol-position-fabric-and-migration.md"

active_contracts=(
    "$repo_root/docs/v2"
    "$repo_root/docs/design/nereus-design-index.md"
    "$repo_root/docs/design/nereus-overall-architecture.md"
    "$repo_root/docs/decisions/0007-v2-wal-linearized-append.md"
    "$repo_root/docs/decisions/0008-v2-storage-profiles-and-ack-boundaries.md"
    "$repo_root/docs/decisions/0009-v2-protocol-native-data-paths.md"
    "$repo_root/docs/decisions/0010-v2-topic-profile-binding.md"
    "$repo_root/docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
    "$repo_root/docs/decisions/0012-v2-storage-epochs-and-profile-evolution.md"
    "$repo_root/docs/decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md"
    "$repo_root/CONTEXT-MAP.md"
    "$repo_root/docs/domain"
)

for stale in OBJECT_WAL_SYNC_OBJECT OBJECT_WAL_ASYNC_OBJECT BOOKKEEPER_WAL_SYNC_OBJECT; do
    if rg -Fn -- "$stale" "${active_contracts[@]}"; then
        fail "active V2 contracts contain removed profile $stale"
    fi
done

if rg -Fn -- "release/0.1" "${active_contracts[@]}"; then
    fail "active V2 contracts contain the obsolete V1 release branch name"
fi

if rg -Fn -- "V2-OPEN-PUL-01" "${active_contracts[@]}"; then
    fail "active V2 contracts still treat native Pulsar position mapping as open"
fi

python3 - "$repo_root" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
source_path = root / "docs/v2/source-locks.json"
scenario_path = root / "docs/v2/v2-scenarios.json"
matrix_path = root / "docs/v2/09-scenario-evidence-matrix.md"
tradeoff_path = root / "docs/v2/tradeoffs.md"

def fail(message: str) -> None:
    raise SystemExit(f"V2 documentation check: {message}")

try:
    source = json.loads(source_path.read_text())
    scenarios = json.loads(scenario_path.read_text())
except (OSError, json.JSONDecodeError) as error:
    fail(f"invalid structured document: {error}")

source_tuple = source.get("sourceTupleId", "")
if source.get("schemaVersion") != 1 or not re.fullmatch(r"v2-m[0-9]+", source_tuple):
    fail("source-locks.json has the wrong schema or tuple ID")
if source.get("productLine") != "V2" or source.get("developmentVersion") != "0.2.0-SNAPSHOT":
    fail("source-locks.json has the wrong product line or version")

sha = re.compile(r"^[0-9a-f]{40}$")
if not sha.fullmatch(source["v2M0Base"]["commit"]):
    fail("V2 M0 base is not a full commit")
if source["v2M0Base"].get("evidenceStatus") != "BASE_ONLY":
    fail("V2 M0 base must not claim runtime evidence")

archive = source.get("v1Archive", {})
if archive.get("branch") != "v0.1" or not sha.fullmatch(str(archive.get("commit", ""))):
    fail("V1 archive identity is invalid")
if archive.get("tag") is not None or archive.get("evidenceStatus") != "HISTORICAL_ONLY":
    fail("V1 archive must record the still-pending tag and historical-only evidence")

release_record = (root / "docs/releases/v0.1.0.md").read_text()
for value in (archive["branch"], archive["commit"]):
    if value not in release_record:
        fail(f"V1 release record does not contain {value}")

legacy_build = source.get("legacyMainImplementationBaselines", [])
if len(legacy_build) != 1:
    fail("legacy main implementation baseline must have one explicit entry at M0")
if (
    legacy_build[0].get("id") != "ordinary-build-pulsar-api"
    or legacy_build[0].get("role") != "V1_RESIDUE_BUILD_ONLY"
    or legacy_build[0].get("evidenceStatus") != "HISTORICAL_ONLY"
    or not sha.fullmatch(str(legacy_build[0].get("commit", "")))
):
    fail("legacy ordinary-build baseline is invalid or improperly claims V2 evidence")
workflow = (root / ".github/workflows/build.yml").read_text()
if legacy_build[0]["commit"] not in workflow:
    fail("legacy ordinary-build baseline differs from the CI checkout")

fork_ids = set()
for item in source.get("forkDevelopmentBases", []):
    if item.get("id") in fork_ids or not sha.fullmatch(str(item.get("commit", ""))):
        fail("fork development bases contain a duplicate ID or invalid commit")
    fork_ids.add(item["id"])
    if item.get("evidenceStatus") != "DEVELOPMENT_BASE_ONLY":
        fail(f"{item['id']} improperly claims V2 evidence")
if fork_ids != {"pulsar-v2-development-base", "kafka-v2-development-base"}:
    fail("fork development bases are incomplete")

research_ids = set()
for item in source.get("researchBaselines", []):
    if item.get("id") in research_ids or not sha.fullmatch(str(item.get("commit", ""))):
        fail("research baselines contain a duplicate ID or invalid commit")
    research_ids.add(item["id"])
    if item.get("evidenceStatus") != "RESEARCH_ONLY":
        fail(f"{item['id']} is not marked research-only")

for name in ("automqComparison", "pulsarNativeParity"):
    baseline = source.get("acceptanceBaselines", {}).get(name, {})
    if source_tuple == "v2-m0":
        if baseline != {"status": "NOT_PINNED", "commit": None, "receipt": None}:
            fail(f"{name} must remain explicitly unpinned at M0")
    else:
        status = baseline.get("status")
        if status not in {"NOT_PINNED", "PINNED", "VERIFIED"}:
            fail(f"{name} has an invalid acceptance status")
        if status in {"PINNED", "VERIFIED"} and not sha.fullmatch(str(baseline.get("commit", ""))):
            fail(f"{name} is {status} without an exact commit")
        if status == "VERIFIED":
            receipt = baseline.get("receipt")
            if not receipt or not (root / receipt).is_file():
                fail(f"{name} is VERIFIED without an existing receipt")

if scenarios.get("schemaVersion") != 1 or scenarios.get("sourceTuple") != source_tuple:
    fail("scenario manifest has the wrong schema or source tuple")
if scenarios.get("generatedFrom") != "docs/v2/09-scenario-evidence-matrix.md":
    fail("scenario manifest has the wrong Markdown owner")

allowed_statuses = {
    "PLANNED",
    "IMPLEMENTED_NOT_RUN",
    "PASSED_CURRENT_SOURCE",
    "FAILED",
    "BLOCKED_ENVIRONMENT",
}
scenario_ids = []
for item in scenarios.get("scenarios", []):
    scenario_id = item.get("id", "")
    if not re.fullmatch(r"V2-[A-Z]+-[0-9]{3}", scenario_id):
        fail(f"invalid scenario ID {scenario_id!r}")
    scenario_ids.append(scenario_id)
    if item.get("status") not in allowed_statuses:
        fail(f"{scenario_id} has an invalid status")
    owner = root / str(item.get("ownerDoc", ""))
    if not owner.is_file():
        fail(f"{scenario_id} owner document does not exist")
    if item.get("status") == "PASSED_CURRENT_SOURCE":
        receipt = item.get("evidenceReceipt")
        if not receipt or not (root / receipt).is_file():
            fail(f"{scenario_id} is PASSED_CURRENT_SOURCE without a receipt")
    elif item.get("evidenceReceipt") is not None:
        fail(f"{scenario_id} has a receipt without current-source PASS")

if len(scenario_ids) != len(set(scenario_ids)):
    fail("scenario IDs must be unique")

required_scenarios = {
    "V2-APP-001", "V2-APP-002", "V2-APP-003", "V2-PROFILE-001",
    "V2-POSITION-001", "V2-MULTIPROTOCOL-001", "V2-MIGRATION-001",
    "V2-PROJECTION-001",
    "V2-OBJ-001", "V2-OBJ-002", "V2-OBJ-003",
    "V2-BK-001", "V2-BK-002", "V2-BK-003",
    "V2-READ-001", "V2-READ-002", "V2-META-001", "V2-HO-001",
    "V2-KAF-001", "V2-PUL-001", "V2-KOP-001",
}
missing_scenarios = required_scenarios - set(scenario_ids)
if missing_scenarios:
    fail(f"required M0 scenarios were removed: {sorted(missing_scenarios)}")

matrix_ids = set(re.findall(r"V2-[A-Z]+-[0-9]{3}", matrix_path.read_text()))
if matrix_ids != set(scenario_ids):
    fail("Markdown and JSON scenario ID sets differ")

tradeoff_text = tradeoff_path.read_text()
tradeoff_rows = re.findall(r"^\| (T-[A-Z]+-[0-9]{2}) \| (Accepted|Provisional) \|", tradeoff_text, re.M)
tradeoff_ids = [row[0] for row in tradeoff_rows]
if len(set(tradeoff_ids)) != len(tradeoff_ids):
    fail("tradeoff register IDs must be unique")
required_tradeoffs = {
    "T-APPEND-01", "T-PROTOCOL-01", "T-POSITION-01",
    "T-MULTIPROTOCOL-01", "T-PROFILE-01", "T-MIGRATION-01",
    "T-PROJECTION-01", "T-OBJECT-01",
    "T-BK-01", "T-LEDGER-01", "T-META-01", "T-MANIFEST-01",
    "T-HANDOFF-01", "T-COMPAT-01", "T-BENCH-01", "T-KOP-01",
}
missing_tradeoffs = required_tradeoffs - set(tradeoff_ids)
if missing_tradeoffs:
    fail(f"required M0 tradeoffs were removed: {sorted(missing_tradeoffs)}")

contract_paths = list((root / "docs/v2").glob("*.md"))
contract_paths += list((root / "docs/decisions").glob("000[7-9]-*.md"))
contract_paths += list((root / "docs/decisions").glob("001[0-3]-*.md"))
contract_text = "\n".join(
    path.read_text() for path in contract_paths if path != tradeoff_path
)
for tradeoff_id in tradeoff_ids:
    if tradeoff_id not in contract_text:
        fail(f"{tradeoff_id} is not referenced by a normative contract")

profile_text = "\n".join(path.read_text() for path in contract_paths)
profile_tokens = set(
    re.findall(r"(?<![A-Z_])(OBJECT_WAL(?:_[A-Z_]+)?|BOOKKEEPER_WAL_[A-Z_]+)", profile_text)
)
expected_profiles = {
    "OBJECT_WAL",
    "BOOKKEEPER_WAL_ONLY",
    "BOOKKEEPER_WAL_ASYNC_OBJECT",
}
if profile_tokens != expected_profiles:
    fail(f"active profile vocabulary differs: {sorted(profile_tokens)}")
PY

link_docs=(
    "$repo_root/README.md"
    "$repo_root/CONTEXT-MAP.md"
    "$repo_root/docs/domain"
    "$repo_root/docs/v2"
    "$repo_root/docs/design/README.md"
    "$repo_root/docs/design/nereus-design-index.md"
    "$repo_root/docs/design/nereus-overall-architecture.md"
    "$repo_root/docs/design/nereus-future5-kop-compatibility.md"
    "$repo_root/docs/decisions/0006-v0.2-clean-break-from-v0.1.md"
    "$repo_root/docs/decisions/0007-v2-wal-linearized-append.md"
    "$repo_root/docs/decisions/0008-v2-storage-profiles-and-ack-boundaries.md"
    "$repo_root/docs/decisions/0009-v2-protocol-native-data-paths.md"
    "$repo_root/docs/decisions/0010-v2-topic-profile-binding.md"
    "$repo_root/docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
    "$repo_root/docs/decisions/0012-v2-storage-epochs-and-profile-evolution.md"
    "$repo_root/docs/decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md"
)

while IFS=: read -r source match; do
    target="${match#*](}"
    target="${target%)}"
    target="${target#<}"
    target="${target%>}"
    case "$target" in
        ""|\#*|*://*|mailto:*|app://*) continue ;;
    esac
    target="${target%%#*}"
    target="${target//%20/ }"
    if [[ "$target" = /* ]]; then
        resolved="$target"
    else
        resolved="$(dirname "$source")/$target"
    fi
    [[ -e "$resolved" ]] || fail "broken local Markdown link in ${source#"$repo_root/"}: $target"
done < <(rg --with-filename --no-heading -o --glob '*.md' '\]\(([^)]+)\)' "${link_docs[@]}")

echo "V2 documentation baseline: contexts, positions, epochs, profiles, authority, source locks, scenarios, tradeoffs, receipts, and links verified."
