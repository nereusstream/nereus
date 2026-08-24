# ADR 0098: V2 M3 current-source M2 regression prerequisite projection

- Status: Accepted
- Date: 2026-08-24
- Refines: ADR 0088's current-source M2 regression execution profile
- Preserves: the historical M2 Final, every historical M2 receipt, all M2 wire/provider/scenario semantics, and an
  empty M2 Amendment lineage

## Context

ADR 0088 requires M3 Final to bind a trusted full M2 regression whose tested Nereus commit exactly equals the M3
tested commit. It separately requires the historical M2 Final and its source tuple to remain byte-for-byte immutable.

The historical Kafka K1 through K8 shell gates use the SHA-256 of the entire historical
`docs/v2/source-locks.json` as their K0 prerequisite. That was correct for the original M2 aggregate, but it is not a
projection of M2 semantics: adding the disjoint `m3AllocatorEvidenceBinding` top-level member changes the whole-file
SHA while leaving every historical member and every M2 input unchanged. The first trusted M3 regression diagnostic at
`eb2db10d2d5d41834d67d2c03f4a427f4432ec69` therefore stopped at K1 after the fresh local module tests and K0 gates
passed. Rewriting the K0 receipt, weakening its default check, or labelling the diagnostic as evidence would be
incorrect.

## Decision

The M3 trusted runner owns an explicit prerequisite projection before it invokes any M2 gate:

1. Load the immutable K0 input receipt and obtain its historical Nereus commit and source-lock SHA.
2. Read `docs/v2/source-locks.json` from that exact Git commit and require its SHA to equal the receipt.
3. Require the working file to equal the blob at the exact M3 tested commit.
4. Require every historical top-level member and value to remain exactly equal.
5. Require the exact current-only member set to be `{m3AllocatorEvidenceBinding}`. Missing, changed, removed, or any
   other added member fails closed.
6. Materialize the verified historical blob only in the external raw-run directory and pass that path to the legacy
   K1 through K8 K0-prerequisite comparison. Their ordinary/default invocation continues to hash the working
   `source-locks.json` exactly as before.
7. Compile, execute, source-scan, and parse all M2 production code and JUnit XML from the exact current M3 source. The
   historical blob supplies only the immutable K0 prerequisite identity; it cannot substitute historical code or
   test results.
8. In this profile K2 requires its Kafka checkout to expose a non-symlink `.git` file, proving that the already
   source-locked checkout is a linked worktree. The ordinary K2 entry point continues to accept either a normal Git
   checkout or a linked worktree. A `.git` directory cannot satisfy the M3 profile.
9. P5 applies the same linked-worktree rule to Pulsar and requires the fixed
   `nereus/v2-m3-m2-regression-evidence` branch plus both that branch and the historical source-lock branch on
   `origin` to resolve to the same locked commit. Its ordinary entry point continues to require the historical branch.
10. Before P6, the runner resolves the active Docker context, requires a non-symlink Unix socket, and verifies that
    the server's declared minimum/maximum API interval contains fixed API `1.44`. P6 Testcontainers processes receive
    only that exact `DOCKER_HOST`, the environment/system-property strategy, and `api.version=1.44`. This avoids
    Testcontainers 1.20.4's API-1.32 fallback against Docker Engine 29 while leaving user-global Testcontainers
    configuration untouched.
11. The P6 native LocalStack command invokes the Gradle wrapper from the dedicated Pulsar worktree and also passes that
    same absolute path as `--project-dir`; its exact task path is the locked Pulsar build's
    `:tiered-storage:tiered-storage-jcloud:test`. The Nereus process working directory and a stale Maven module name are
    never allowed to select or abbreviate the Gradle project for an exact-source native Pulsar test.

The current-source receipt remains schema `NEREUS_V2_M3_CURRENT_SOURCE_M2_REGRESSION_V1`, non-promotable, and records
`m2AmendmentLineage: []`. This is an M3 execution-profile correction, not an M2 semantic or correctness amendment.

Any change to a historical source-lock member, M2 wire/frontier/checkpoint authority, mandatory M2 scenario,
Provider/source contract, or test expectation still requires an explicit M2 Amendment or new M2 Final lineage. The v1
runner rejects such a change rather than expanding the current-only allowlist by convention.

## Consequences

- The historical M2 Final and K0 input receipt remain unchanged and continue to validate against their original
  whole-file source-lock SHA.
- A future M3-only source-lock member requires an explicit runner/ADR revision and a fresh complete regression; it is
  never admitted by a prefix or wildcard.
- Failed or partial raw-run directories remain diagnostics outside the repository and cannot be published as trusted
  children.
- The K2 Git-layout check no longer misclassifies linked worktrees as non-repositories; the M3 profile is stricter
  than the historical entry point and still rejects a shared checkout.
- P5 cannot substitute the shared Pulsar checkout merely to satisfy a historical branch-name assertion; the M3
  profile proves both remote branch identities at one exact commit.
- P6 uses the same already-verified Docker daemon as the runner's exact image checks. A different socket, symbolic
  socket, non-Unix context, or server that excludes API 1.44 fails before Testcontainers starts.
- P6 native LocalStack cannot resolve `:tiered-storage:jcloud` against the Nereus build merely because the trusted
  runner itself was launched from the Nereus root.
- Receipt publication still requires all 25 non-empty child gates with zero failure, error, and skip at one exact
  current source commit.
