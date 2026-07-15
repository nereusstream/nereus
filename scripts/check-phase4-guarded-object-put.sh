#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
object_store="$repo_root/nereus-object-store/src/main/java/com/nereusstream/objectstore/ObjectStore.java"
s3_store="$repo_root/nereus-object-store/src/main/java/com/nereusstream/objectstore/S3CompatibleObjectStore.java"
guard_test="$repo_root/nereus-object-store/src/test/java/com/nereusstream/objectstore/GuardedPutObjectAttemptContractTest.java"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing guarded object PUT contract '$literal' in ${path#"$repo_root/"}" >&2
        exit 1
    fi
}

require_literal "PutObjectAttemptGuard attemptGuard" "$object_store"
require_literal "PutObjectAttemptGuard attemptGuard" "$s3_store"
require_literal "attemptGuard.authorize(key, attemptNumber)" "$s3_store"
require_literal "transmitPut(key, source, options, remaining)" "$s3_store"
require_literal "guardRunsImmediatelyBeforeEveryOwnedProviderRetry" "$guard_test"
require_literal "failedRetryGuardSendsNoSecondAttemptAndPreservesFenceFailure" "$guard_test"
require_literal "failedInitialGuardNeverOpensUploadOrCallsProvider" "$guard_test"

echo "Phase 4 guarded provider PUT ordering and negative tests verified."
