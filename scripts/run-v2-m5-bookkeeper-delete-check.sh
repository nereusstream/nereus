#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C

m5_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
m5_compose="$m5_repo_root/config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"
m5_project="nereus-v2-m5-bookkeeper-delete-$$"
m5_image="apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"
m5_expected_image_id="sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605"

m5_cleanup() {
  docker compose -p "$m5_project" -f "$m5_compose" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap m5_cleanup EXIT INT TERM
m5_cleanup

test "$(docker image inspect "$m5_image" --format '{{.Id}}')" = "$m5_expected_image_id"
test "$(docker image inspect "$m5_image" --format '{{.Os}}/{{.Architecture}}')" = "linux/amd64"
docker compose -p "$m5_project" -f "$m5_compose" up -d --wait

for m5_service in metadata-service bookie-0 bookie-1 bookie-2; do
  m5_container_id="$(docker compose -p "$m5_project" -f "$m5_compose" ps -q "$m5_service")"
  test -n "$m5_container_id"
  test "$(docker inspect --format '{{.Config.Image}}' "$m5_container_id")" = "$m5_image"
  test "$(docker inspect --format '{{.Image}}' "$m5_container_id")" = "$m5_expected_image_id"
done

"$m5_repo_root/gradlew" \
  --no-daemon \
  --no-configuration-cache \
  --rerun-tasks \
  "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers" \
  v2M5BookKeeperDeleteCheck \
  --console=plain

python3 - "$m5_repo_root" <<'PY'
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

root = Path(sys.argv[1])
path = root / "nereus-storage-bookkeeper/build/test-results/realBookKeeperTest/TEST-com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1RealTest.xml"
suite = ET.parse(path).getroot()
if {key: suite.attrib.get(key) for key in ("tests", "failures", "errors", "skipped")} != {
    "tests": "7", "failures": "0", "errors": "0", "skipped": "0"
}:
    raise SystemExit("real BookKeeper JUnit summary differs")
names = {case.attrib.get("name") for case in suite.findall("testcase")}
if "m5DeleteAdapterDeletesOnlyTheExactSealedLedgerAndReconcilesAbsence()" not in names:
    raise SystemExit("real BookKeeper M5 deletion testcase is absent")
PY

printf 'PASS_V2_M5_BOOKKEEPER_DELETE_REAL implementation-only image=%s id=%s\n' \
  "$m5_image" \
  "$m5_expected_image_id"
