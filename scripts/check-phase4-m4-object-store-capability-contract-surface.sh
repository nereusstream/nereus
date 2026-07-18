#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_file() {
    local path="$1"
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 object-store capability artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 object-store capability contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 object-store capability contract '$literal' in $path" >&2
        exit 1
    fi
}

probe_interface="nereus-object-store/src/main/java/com/nereusstream/objectstore/ObjectStoreDeleteCapabilityProbe.java"
request="nereus-object-store/src/main/java/com/nereusstream/objectstore/ObjectStoreDeleteCapabilityRequest.java"
proof="nereus-object-store/src/main/java/com/nereusstream/objectstore/ObjectStoreDeleteCapabilityProof.java"
implementation="nereus-object-store/src/main/java/com/nereusstream/objectstore/DefaultObjectStoreDeleteCapabilityProbe.java"
test="nereus-object-store/src/test/java/com/nereusstream/objectstore/ObjectStoreDeleteCapabilityProbeTest.java"

for path in "$probe_interface" "$request" "$proof" "$implementation" "$test"; do
    require_file "$path"
done

require_literal "String expectedCapabilitySha256();" "$probe_interface"
require_literal "CompletableFuture<ObjectStoreDeleteCapabilityProof> probe(" "$probe_interface"

require_literal "record ObjectStoreDeleteCapabilityRequest(String runId, Duration timeout)" "$request"
require_literal "runId must be lowercase base32 without padding" "$request"
require_literal "timeout must be positive and millisecond-representable" "$request"

require_literal "public static final int PROTOCOL_VERSION = 1;" "$proof"
require_literal "String capabilitySha256" "$proof"
require_literal "String probeObjectKeySha256" "$proof"
require_literal "only capabilitySha256 is persisted" "$proof"

require_literal '"nereus-object-store-delete-capability-v1"' "$implementation"
require_literal '"__nereus_capability__/delete-v1/"' "$implementation"
require_literal "configuration.providerClassName()" "$implementation"
require_literal "configuration.endpoint().normalize()" "$implementation"
require_literal "configuration.region()" "$implementation"
require_literal "configuration.bucket()" "$implementation"
require_literal "configuration.prefix()" "$implementation"
require_literal "configuration.pathStyleAccess()" "$implementation"
for semantic in \
    "guarded-put-if-absent" \
    "exact-head-crc32c-etag" \
    "complete-prefix-list-with-last-modified" \
    "exact-identity-delete" \
    "delete-response-loss-absence-recovery" \
    "idempotent-delete-and-post-delete-list-absence"; do
    require_literal "$semantic" "$implementation"
done

require_literal "objectStore.putObject(" "$implementation"
require_literal "true," "$implementation"
require_literal "objectStore.headObject(" "$implementation"
require_literal "objectStore.listObjects(" "$implementation"
require_literal "objectStore.deleteObject(" "$implementation"
require_literal "head.etag()" "$implementation"
require_literal "DeleteObjectResult.Status.ALREADY_ABSENT" "$implementation"
require_literal "verifyAbsent(context, deadline)" "$implementation"
require_literal "verifyAbsentList(context, deadline)" "$implementation"
require_literal "cleanup(context, exact.timeout())" "$implementation"
require_literal "Crc32cChecksums.checksum(payload)" "$implementation"
require_literal "MessageDigest.getInstance(\"SHA-256\")" "$implementation"
require_literal "Math.addExact" "$implementation"
require_literal ".orTimeout(" "$implementation"

for secret in \
    "accessKey" \
    "secretKey" \
    "sessionToken" \
    "credentialsProvider"; do
    reject_literal "$secret" "$implementation"
done

require_literal "provesExactLifecycleAndProducesStableScopeIdentity" "$test"
require_literal "capabilityIdentityChangesWithThePhysicalObjectScope" "$test"
require_literal "recoversLostPutAndDeleteResponsesThroughExactFacts" "$test"
require_literal "incompleteListingFailsClosedAndCleansTheExactCanary" "$test"
require_literal "rejectsAmbiguousRequestsAndProofs" "$test"

require_literal "phase4M4ObjectStoreCapabilityCheck" "build.gradle.kts"
require_literal "Checkpoint AP" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4ObjectStoreCapabilityCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 configured-scope object-store delete capability contract surface: PASS"
