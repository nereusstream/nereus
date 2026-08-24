#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:-}"
cd "$repo_root"

fail() {
    echo "V2 M3 module/API gate: $*" >&2
    exit 1
}

[[ -n "$pulsar_checkout" && -f "$pulsar_checkout/settings.gradle.kts" ]] ||
    fail "the explicit dedicated Pulsar source worktree argument is missing or invalid"
[[ -f "$pulsar_checkout/.git" ]] ||
    fail "Pulsar checkout must be a linked dedicated worktree, not a shared checkout"
[[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] || fail "Nereus source tree must be clean"

expected_pulsar_commit="$(jq -er '.m3AllocatorEvidenceBinding.pulsarSourceCommit' docs/v2/source-locks.json)"
expected_pulsar_remote="$(jq -er '.m2PulsarNativeBinding.repository' docs/v2/source-locks.json)"
expected_pulsar_branch="nereus/v2-m3-object-wal-evidence"
[[ "$(git -C "$pulsar_checkout" rev-parse HEAD)" == "$expected_pulsar_commit" ]] ||
    fail "Pulsar source worktree does not match the exact source lock"
[[ "$(git -C "$pulsar_checkout" branch --show-current)" == "$expected_pulsar_branch" ]] ||
    fail "Pulsar source worktree is not the dedicated M3 evidence branch"
[[ "$(git -C "$pulsar_checkout" remote get-url origin)" == "$expected_pulsar_remote" ]] ||
    fail "Pulsar origin does not match the exact source-lock repository"
[[ -z "$(git -C "$pulsar_checkout" status --porcelain=v1 --untracked-files=all)" ]] ||
    fail "Pulsar source worktree must be clean"
remote_head="$(git -C "$pulsar_checkout" ls-remote --heads origin "refs/heads/$expected_pulsar_branch" | awk '{print $1}')"
[[ "$remote_head" == "$expected_pulsar_commit" ]] ||
    fail "Pulsar origin M3 evidence branch is not the exact source-locked commit"
[[ "$(git -C "$pulsar_checkout" rev-parse "refs/remotes/origin/$expected_pulsar_branch")" == "$expected_pulsar_commit" ]] ||
    fail "Pulsar tracking branch is not the exact remote M3 evidence commit"

tested_commit="$(git rev-parse HEAD)"
[[ "$tested_commit" == "$(git rev-parse origin/main)" ]] ||
    fail "HEAD must equal origin/main before source-qualified publication"
version="0.2.0-m3.$tested_commit"

modules=(
    nereus-bom
    nereus-domain
    nereus-metadata-spi
    nereus-metadata-oxia
    nereus-storage-api
    nereus-storage-bookkeeper
    nereus-storage-object
    nereus-storage-object-s3
    nereus-storage-object-vault
    nereus-kafka-bookkeeper
    nereus-pulsar-offload
)
test_modules=("${modules[@]:1}")

# A single nested Gradle invocation owns the ordinary JUnit and style phase. The outer task has no
# Gradle dependencies, so publication cannot race classes/test-results produced by an unrelated task.
ordinary_tasks=()
for module in "${test_modules[@]}"; do
    ordinary_tasks+=(":$module:cleanTest" ":$module:test" ":$module:checkstyleMain" ":$module:checkstyleTest")
done
ordinary_tasks+=(":nereus-domain:spotlessCheck")
for module in "${test_modules[@]:1}"; do
    ordinary_tasks+=(":$module:spotlessCheck")
done
./gradlew --no-daemon --no-configuration-cache --no-parallel --console=plain \
    -PpulsarCheckout="$pulsar_checkout" \
    "${ordinary_tasks[@]}"

python3 - "$repo_root" "${test_modules[@]}" <<'PY'
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

root = Path(sys.argv[1])
modules = sys.argv[2:]
for module in modules:
    reports = sorted((root / module / "build" / "test-results" / "test").glob("TEST-*.xml"))
    if not reports:
        raise SystemExit(f"V2 M3 module/API gate: no JUnit XML reports for {module}")
    executed = failures = errors = skipped = 0
    for report in reports:
        suite = ET.parse(report).getroot()
        executed += int(suite.attrib.get("tests", "0"))
        failures += int(suite.attrib.get("failures", "0"))
        errors += int(suite.attrib.get("errors", "0"))
        skipped += int(suite.attrib.get("skipped", "0"))
    if executed == 0 or failures != 0 or errors != 0 or skipped != 0:
        raise SystemExit(
            "V2 M3 module/API gate: invalid JUnit result for "
            f"{module}: tests={executed} failures={failures} errors={errors} skipped={skipped}"
        )
print(f"V2 M3 ordinary module JUnit/style phase: modules={len(modules)} zeroSkip=PASS")
PY

[[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] ||
    fail "ordinary module verification changed the Nereus source tree"
[[ "$tested_commit" == "$(git rev-parse HEAD)" && "$tested_commit" == "$(git rev-parse origin/main)" ]] ||
    fail "source changed after the ordinary module verification phase"

repository="$(mktemp -d "${TMPDIR:-/tmp}/nereus-m3-development-repository.XXXXXX")"
consumer_dir="$(mktemp -d "${TMPDIR:-/tmp}/nereus-m3-consumer.XXXXXX")"
cleanup() {
    rm -rf -- "$repository" "$consumer_dir"
}
trap cleanup EXIT
[[ -z "$(find "$repository" -mindepth 1 -print -quit)" ]] || fail "fresh M3 development repository is not empty"

publish_tasks=()
for module in "${modules[@]}"; do
    publish_tasks+=(":$module:publishMavenJavaPublicationToDevelopmentRepository")
done
./gradlew --no-daemon --no-configuration-cache --no-parallel --console=plain \
    -PnereusVersion="$version" \
    -PpulsarCheckout="$pulsar_checkout" \
    -PdevelopmentRepository="$repository" \
    "${publish_tasks[@]}"

python3 - "$repository" "$version" "${modules[@]}" <<'PY'
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

repository = Path(sys.argv[1])
version = sys.argv[2]
modules = tuple(sys.argv[3:])
expected = set(modules)
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
expected_direct_internal = {
    "nereus-domain": set(),
    "nereus-metadata-spi": {"nereus-domain"},
    "nereus-metadata-oxia": {"nereus-metadata-spi", "nereus-storage-object"},
    "nereus-storage-api": {"nereus-domain"},
    "nereus-storage-bookkeeper": {"nereus-storage-api"},
    "nereus-storage-object": {"nereus-storage-api", "nereus-domain"},
    "nereus-storage-object-s3": {"nereus-storage-object"},
    "nereus-storage-object-vault": {"nereus-storage-object"},
    "nereus-kafka-bookkeeper": {
        "nereus-storage-api", "nereus-storage-bookkeeper", "nereus-storage-object"
    },
    "nereus-pulsar-offload": {"nereus-metadata-spi", "nereus-storage-object"},
}

for module in modules:
    base = repository / "com" / "nereusstream" / module / version
    pom = base / f"{module}-{version}.pom"
    metadata = base / f"{module}-{version}.module"
    if not pom.is_file() or not metadata.is_file():
        raise SystemExit(f"V2 M3 module/API gate: missing POM/metadata for {module}")
    if module != "nereus-bom":
        for suffix in (".jar", "-sources.jar"):
            artifact = base / f"{module}-{version}{suffix}"
            if not artifact.is_file() or artifact.stat().st_size <= 0:
                raise SystemExit(f"V2 M3 module/API gate: missing artifact {artifact.name}")

    root = ET.parse(pom).getroot()
    pom_internal = set()
    for dependency in root.findall("m:dependencies/m:dependency", namespace):
        group = dependency.findtext("m:groupId", namespaces=namespace)
        artifact = dependency.findtext("m:artifactId", namespaces=namespace)
        dependency_version = dependency.findtext("m:version", namespaces=namespace)
        if group == "com.nereusstream":
            if artifact not in expected or dependency_version != version:
                raise SystemExit(
                    "V2 M3 module/API gate: dangling internal POM dependency "
                    f"{module} -> {group}:{artifact}:{dependency_version}"
                )
            pom_internal.add(artifact)
    if module in expected_direct_internal and pom_internal != expected_direct_internal[module]:
        raise SystemExit(
            "V2 M3 module/API gate: unexpected internal POM closure for "
            f"{module}: actual={sorted(pom_internal)} expected={sorted(expected_direct_internal[module])}"
        )

    module_json = json.loads(metadata.read_text())
    metadata_internal = set()
    for variant in module_json.get("variants", []):
        for dependency in variant.get("dependencies", []):
            if dependency.get("group") != "com.nereusstream":
                continue
            artifact = dependency.get("module")
            required = dependency.get("version", {}).get("requires")
            if artifact not in expected or required != version:
                raise SystemExit(
                    "V2 M3 module/API gate: dangling internal Gradle dependency "
                    f"{module} -> com.nereusstream:{artifact}:{required}"
                )
            metadata_internal.add(artifact)
    if module in expected_direct_internal and metadata_internal != expected_direct_internal[module]:
        raise SystemExit(
            "V2 M3 module/API gate: unexpected internal Gradle closure for "
            f"{module}: actual={sorted(metadata_internal)} expected={sorted(expected_direct_internal[module])}"
        )

storage_api_pom = repository / "com" / "nereusstream" / "nereus-storage-api" / version / f"nereus-storage-api-{version}.pom"
storage_api_root = ET.parse(storage_api_pom).getroot()
if not any(
    dependency.findtext("m:groupId", namespaces=namespace) == "com.nereusstream"
    and dependency.findtext("m:artifactId", namespaces=namespace) == "nereus-domain"
    and dependency.findtext("m:scope", default="compile", namespaces=namespace) == "compile"
    for dependency in storage_api_root.findall("m:dependencies/m:dependency", namespace)
):
    raise SystemExit("V2 M3 module/API gate: storage-api does not publish current domain as compile API")

bookkeeper_pom = repository / "com" / "nereusstream" / "nereus-storage-bookkeeper" / version / f"nereus-storage-bookkeeper-{version}.pom"
bookkeeper_root = ET.parse(bookkeeper_pom).getroot()
if not any(
    dependency.findtext("m:groupId", namespaces=namespace) == "org.apache.bookkeeper"
    and dependency.findtext("m:artifactId", namespaces=namespace) == "bookkeeper-server"
    and dependency.findtext("m:scope", default="compile", namespaces=namespace) == "compile"
    for dependency in bookkeeper_root.findall("m:dependencies/m:dependency", namespace)
):
    raise SystemExit("V2 M3 module/API gate: storage-bookkeeper does not publish BookKeeper types as compile API")

metadata_oxia_pom = repository / "com" / "nereusstream" / "nereus-metadata-oxia" / version / f"nereus-metadata-oxia-{version}.pom"
metadata_oxia_root = ET.parse(metadata_oxia_pom).getroot()
if not any(
    dependency.findtext("m:groupId", namespaces=namespace) == "io.opentelemetry"
    and dependency.findtext("m:artifactId", namespaces=namespace) == "opentelemetry-bom"
    and dependency.findtext("m:version", namespaces=namespace) == "1.63.0"
    and dependency.findtext("m:type", default="jar", namespaces=namespace) == "pom"
    and dependency.findtext("m:scope", default="compile", namespaces=namespace) == "import"
    for dependency in metadata_oxia_root.findall(
        "m:dependencyManagement/m:dependencies/m:dependency", namespace
    )
):
    raise SystemExit("V2 M3 module/API gate: metadata-oxia does not publish the Oxia API OpenTelemetry BOM")

bom = repository / "com" / "nereusstream" / "nereus-bom" / version / f"nereus-bom-{version}.pom"
bom_root = ET.parse(bom).getroot()
constraints = {
    dependency.findtext("m:artifactId", namespaces=namespace)
    for dependency in bom_root.findall("m:dependencyManagement/m:dependencies/m:dependency", namespace)
    if dependency.findtext("m:groupId", namespaces=namespace) == "com.nereusstream"
}
if constraints != expected - {"nereus-bom"}:
    raise SystemExit(
        f"V2 M3 module/API gate: BOM closure differs: actual={sorted(constraints)} "
        f"expected={sorted(expected - {'nereus-bom'})}"
    )
print(f"V2 M3 source-qualified artifact closure: modules={len(modules)} version={version}")
PY

mkdir -p "$consumer_dir/src/main/java/m3/consumer"
cat >"$consumer_dir/settings.gradle.kts" <<EOF
pluginManagement { repositories { gradlePluginPortal(); mavenCentral() } }
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        maven { url = uri("$repository") }
        exclusiveContent {
            forRepository {
                maven { url = uri("$repo_root/gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2") }
            }
            filter {
                includeModule("io.github.oxia-db", "oxia-client")
                includeModule("io.github.oxia-db", "oxia-client-api")
            }
        }
        mavenCentral()
    }
}
includeBuild("$pulsar_checkout")
rootProject.name = "nereus-m3-api-consumer"
EOF

cat >"$consumer_dir/build.gradle.kts" <<EOF
plugins { java }
dependencies {
    implementation(platform("com.nereusstream:nereus-bom:$version"))
    implementation("com.nereusstream:nereus-metadata-oxia")
    implementation("com.nereusstream:nereus-storage-bookkeeper")
    implementation("com.nereusstream:nereus-storage-object-s3")
    implementation("com.nereusstream:nereus-storage-object-vault")
    implementation("com.nereusstream:nereus-kafka-bookkeeper")
    implementation("com.nereusstream:nereus-pulsar-offload")
}
java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
}
tasks.withType<JavaCompile>().configureEach { options.release.set(17) }
EOF

cat >"$consumer_dir/src/main/java/m3/consumer/M3ApiConsumer.java" <<'EOF'
package m3.consumer;

import com.nereusstream.kafka.bookkeeper.object.publication.KafkaNwg1ObjectPipelineV1;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.pulsar.offload.S3PulsarOffloadObjectStoreV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import com.nereusstream.storage.object.vault.VaultTransitRunKeyKms;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader;
import software.amazon.awssdk.services.s3.S3Client;

public final class M3ApiConsumer {
    WalRunObjectSession objectSession;
    KafkaNwg1ObjectPipelineV1 kafka;
    PulsarObjectWalBridgeV1 pulsar;
    SourceSafeLedgerOffloader nativePulsarContract;
    VaultTransitRunKeyKms vault;
    RealBookKeeperCellSessionV1 bookKeeperSession;
    BookKeeper bookKeeperApi;

    OxiaCanonicalControlMetadataStore oxia(AsyncOxiaClient client) {
        return new OxiaCanonicalControlMetadataStore(client, "/cells/test", 0);
    }

    S3C1ObjectProviderTransport s3(S3Client client) {
        return new S3C1ObjectProviderTransport(client, "bucket", "provider", false, 1);
    }

    S3PulsarOffloadObjectStoreV1 pulsarS3(S3Client client, ExecutorService executor) {
        return new S3PulsarOffloadObjectStoreV1(client, "bucket", executor, false);
    }
}
EOF

"$repo_root/gradlew" --no-daemon --no-configuration-cache --no-parallel --console=plain \
    -p "$consumer_dir" compileJava

report_dir="$repo_root/build/v2-m3/module-api"
mkdir -p "$report_dir"
python3 - "$repository" "$report_dir/artifacts.sha256" <<'PY'
from hashlib import sha256
from pathlib import Path
import sys

repository = Path(sys.argv[1])
report = Path(sys.argv[2])
entries = []
for path in sorted(path for path in repository.rglob("*") if path.is_file()):
    digest = sha256(path.read_bytes()).hexdigest()
    entries.append(f"{digest}  {path.relative_to(repository).as_posix()}")
if not entries:
    raise SystemExit("V2 M3 module/API gate: artifact SHA inventory is empty")
temporary = report.with_suffix(".tmp")
temporary.write_text("\n".join(entries) + "\n")
temporary.replace(report)
PY

echo "V2 M3 module/API verified: source=$tested_commit modules=${#modules[@]} consumerCompile=PASS"
