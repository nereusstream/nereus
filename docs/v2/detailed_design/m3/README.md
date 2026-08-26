---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeExecutionIndex
sourceTuple: v2-m1
---

# M3 detailed-design index

M3 implements the one-Cell Object WAL carrier over the accepted M1 identities/metadata authorities and M2 protocol
state machines. The M3-I0 review closes the NWG1 implementation inputs; it does not make the whole milestone
implementation-ready by assertion and does not convert any `PLANNED` scenario into evidence.

## Working implementation snapshot (2026-08-24, non-promotable)

This snapshot records the completed handoff from parallel slice development to one serial integration owner. All
slice workers are quiescent; the main repository owner alone now integrates, reruns, commits, pushes, and publishes
evidence. It is an implementation ledger only: uncommitted source, compilation, static governance tests, focused
tests, and external fork commits are not M3 evidence and do not promote a scenario.

- The serial-integration handoff baseline is `768a97faaef1d87cd914cf7ec840f637c1bdc4c3`. The latest source-tested
  implementation checkpoint is `98a6384a9a0cf0a4579212f2df9b9a760a3b1f59`; `main`, `origin/main`, and the
  working tree were exactly aligned for its ordinary module/API run. Ten reviewable implementation commits since the
  handoff baseline contain the common Object-WAL foundation, Kafka and Pulsar paths plus their documentation
  checkpoints, real Provider/KMS adapters, allocator/Oxia production and evidence paths, aggregate governance, and
  the source-qualified module/API closure.
- All former slice workers are stopped. Former worker test summaries are inputs for serial review only; any source
  change invalidates their freshness and requires a main-owner rerun. The remaining work is execution and publication
  of exact-source M2 regression, Provider/KMS and allocator evidence, child receipts, scenarios, and Final. Prior local
  or diagnostic XML/JSON files are not receipts.
- The current common-module tree passes `:nereus-storage-object:check`: 147 tests execute with zero failure, error, or
  skip, and Checkstyle plus Spotless pass. The non-promotable local gates also pass with 17 wire-source tests over the
  six positive vectors and 114-row TSV, 3 mutation tests over all 84 authored records and 240 paths, and 7 state-trace
  tests over the closed 50 traces and 21 outcomes. These results establish a reviewable source checkpoint only; no
  exact-source child receipt has been published.
- ADR 0096 records the explicit pre-closure-to-closed NWR1 golden lineage and the conservative different-CAS-winner
  rollover rule. Wire versions, field order and widths, rejection codes, validation stages, strict-decoder behavior,
  and the M3-I0 exclusions are unchanged.
- The current Kafka module tree passes `:nereus-kafka-bookkeeper:check`: 260 tests execute with zero failure, error, or
  skip, and all module Checkstyle/Spotless gates pass. The Object path now uses compact streaming publication
  verification, Root-bound NWKCP1 keys and strict Head state, whole-suffix rollback, physical/binding frontiers, and a
  common-coordinator `ProtocolRecoveryHandler` that stages the three secure lane spools exactly once under the durable
  owner fence. This is local current-tree conformance, not the dedicated-fork/native source receipt.
- The dedicated Kafka fork branch `nereus/v2-m3-object-wal-evidence` is clean and pushed at
  `323e035145d203f7e74e969341cb610f33e71b7d`. Its four M3 commits guard native owner callbacks, add deterministic
  rollback/takeover-cut tests, and split the legacy F9 product repository from the V2 M3 repository. A dedicated
  dual-repository run built the F9 product closure from Nereus `884a2cb6521c56885fa42d18138d138bb4fefb35`
  and the V2 M3 closure from `d3ad9274ce0509af95626ef9cc6f27c7cd835f36`; the five owner-fence tests and one
  whole-suffix rollback/takeover test pass with zero failure/error/skip, together with core main/test Checkstyle and
  SpotBugs. The V2 coordinate in this checkpoint remains a temporary exact-worktree `0.2.0-SNAPSHOT`, so this is a
  non-promotable compile checkpoint rather than the final source-qualified native receipt.
- Nereus `bc8691a636456cef48119ded637ea027679b0903` commits the local Pulsar fixed-slice Object-WAL path. The
  `:nereus-pulsar-offload:check` gate passes 140 tests across 12 suites with zero failure/error/skip, plus Spotless and
  main/test/P6-provider Checkstyle. Publication, recovery, and routine reads use the compact streaming NWG1 API;
  routine payload bytes are copied only inside the borrowed-frame callback. The exact Root/Protocol-Cell verification
  context is carried through the all-Binding monotonic owner fence, and append-unit verification binds the one-frame
  ledger/entry, commit-set, storage-attempt, and assigned-payload SHA fields. The common
  `:nereus-storage-object:check` also remains green after this API integration. This is local current-tree conformance,
  not native Pulsar or allocator evidence.
- The dedicated Pulsar fork branch `nereus/v2-m3-object-wal-evidence` is clean and pushed at
  `7ff908330809f2e9bc5c69ead87bb85c566bc0a9`. It adds the non-activating native Cell-owner maintenance bridge and
  resolves the Nereus M3 artifacts from a separate explicit repository. The exact input artifacts were published from
  the clean detached Nereus source `bc8691a636456cef48119ded637ea027679b0903`; the temporary
  `nereus-pulsar-offload` SNAPSHOT JAR SHA-256 is
  `f30542d41bb860ab93bc08f56e9ef88f04d9d2493a3b60fcc24a245b97290158`. Five native-boundary tests pass with zero
  failure/error/skip, main/test Checkstyle passes, the M3-enabled broker main/test sources compile, and the same branch
  compiles the stock broker main/test sources when the optional M3 input is disabled. The bridge blocks Cell-owner
  replacement across the complete synchronous Provider callback and proves stock Position/MessageId retention at the
  fixed 2^40 slice endpoint. Because the coordinate is a temporary SNAPSHOT and this does not activate broker/topic
  lifecycle wiring, it is a non-promotable compile checkpoint; native broker/controller activation remains M6.
- Kafka and Pulsar Nereus adapters now use the bounded owner-open recovery coordinator and both have dedicated
  external-fork compile checkpoints. The production S3 C1 and Vault Transit adapters, their independently classified
  failure surfaces, and their formal exact-source test tasks are implemented. Their ordinary module checks execute
  nine tests with zero failure/error/skip and pass Spotless/Checkstyle; fixed-digest MinIO and Vault diagnostic runs
  execute two additional tests with zero failure/error/skip, including the 64-MiB admitted C1 transfer/fault cuts and
  Vault v1-to-v2 rotation gated by a production `WalRunObjectSession` terminal-closure proof. Diagnostic runs are
  explicitly non-evidence: the clean-pushed exact-source formal Provider/KMS receipts and allocator evidence remain
  open.
- Nereus `a95e492e2597a48c77bcbdc2354ca189d93fc552` commits the allocator/Oxia production path and formal evidence
  runner. Fresh ordinary execution covers 48 allocator tests, eight Object-WAL Oxia adapter tests, five frozen
  allocator-contract tests, and two pinned native-path tests with zero failure/error/skip; one separate test passes
  against the exact source-locked real Oxia image. ADR 0097 independently versions that reproducible local M3 evidence
  image as `nereus/oxia-m3-allocator:37a17bef1720` with config digest
  `sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da`, without changing the historical M1
  image lock. The real-Oxia run and dirty-tree preflight are diagnostic only; 10k/100k formal execution and selection
  remain open.
- At exact clean source `98a6384a9a0cf0a4579212f2df9b9a760a3b1f59`, `v2M3ModuleApiSourceCheck` verifies ten
  production modules with non-empty zero-failure/error/skip JUnit reports and style gates, publishes all eleven M3
  modules at the source-qualified coordinate, verifies their POM/Gradle metadata/BOM closure, and compiles an
  independent consumer. This is ordinary current-source implementation evidence, not a child receipt or scenario
  promotion.
- W1 now has a complete non-promotable current-source M2 regression receipt. Fresh Provider/KMS/allocator and the
  remaining M3 child receipts, aggregate Final, and Markdown/JSON scenario promotions do not yet exist.
  `implementationStatus: InProgress` therefore remains authoritative.

The first full M2 regression diagnostic at exact source `eb2db10d2d5d41834d67d2c03f4a427f4432ec69` correctly
stopped at K1 because the historical gate hashed the whole source-lock document after the disjoint M3 allocator member
was added. [ADR 0098](../../../decisions/0098-v2-m3-current-source-m2-regression-prerequisite-projection.md) now
requires an exact historical-member/current-only-member projection while leaving the default M2 gates and historical
receipts unchanged; the failed external directory is diagnostic and non-promotable.
The next diagnostic advanced through the current-source local K0/K1/K3-K10 and Pulsar P0-P4 gates before K2 exposed
an old `.git`-directory-only assumption. ADR 0098 now also requires K2's M3 input to be a linked-worktree `.git` file;
no shared Kafka checkout was substituted.
The following diagnostic completed K2's local/exact-source 30 tests and P5's native 26 tests before P5 rejected the
fixed evidence branch name; ADR 0098 now makes P5 prove the linked worktree and both published branch refs at the same
locked commit instead of falling back to the shared checkout.
The next diagnostic completed real BookKeeper plus Kafka 10k/100k before P6 exposed Testcontainers 1.20.4's API-1.32
fallback against Docker Engine 29.7.2 (minimum API 1.40). A dedicated-worktree diagnostic passed both P6 suites with
the active Docker-context socket and fixed API 1.44; ADR 0098 now makes those values fail-closed runner inputs. During
diagnosis one command omitted `-PpulsarCheckout`, was interrupted during compilation, and updated only pre-existing
ignored build state in the shared Pulsar checkout; its Git worktree remained clean, no product source changed, and no
cleanup was performed.
A later local documentation-gate invocation also omitted the dedicated Pulsar property and was interrupted while
configuring Gradle build logic. It likewise left the shared Pulsar Git worktree clean, may have refreshed only ignored
Gradle build state, and was not cleaned or used as evidence; every replacement invocation names both dedicated
worktrees explicitly.
The first complete formal attempt at the fixed Docker profile then passed local M2, exact Kafka/Pulsar native, real
BookKeeper, Kafka 10k/100k, and both local P6 Testcontainers suites before the native LocalStack command resolved its
Gradle project against the Nereus working directory. ADR 0098 now pins both the Pulsar wrapper and `--project-dir` to
the dedicated exact-source Pulsar worktree and uses the exact current Gradle task
`:tiered-storage:tiered-storage-jcloud:test`; that failed output remains diagnostic and non-promotable.
The replacement formal profile completed all 25 trusted children at exact source
`89a766124c9ecd1ae407eb76024acddffbe19f69`: 687 tests, zero failure/error/skip, exact Kafka and Pulsar native
sources, real BookKeeper, Kafka 10k/100k, P6 candidate/native/MinIO, and the immutable historical M2 prerequisite.
The [published W1 receipt](../../evidence/v2-m3/w1/m2-regression/receipt.json) is non-promotable, has SHA-256
`4a7e25060ff2e2700f1d4373b5ecb8123b5a51f587f0f80b0209ce4810dfd7bd`, leaves `m2AmendmentLineage` empty, and
promotes no scenario. It remains an intermediate checkpoint: any subsequent non-evidence source change requires a
fresh complete profile before M3 Final can bind it.
The first formal Provider/KMS aggregate invocation at source `3d88ebd50a5dd805bcfeb76f8dd9cf018240e9f5`
completed both one-test fixed-digest real executions and sealed their outputs, but the Gradle build then rejected
configuration-cache serialization of the live Git/exclusive-output task closures. That entire invocation remains
diagnostic and non-promotable. The formal tasks now declare this intentional incompatibility so the repository's
default configuration-cache setting cannot turn successful execution into a failed aggregate after publication.

The integration owner switched the remaining work to a single-agent serial continuation before the allocator
selection run. No parallel worker may edit, build, publish, or promote M3 state; the one owner audits and integrates
each boundary in order. The exact-source Provider/KMS rerun at `99ab89d5dcea04c93369a9dbd66b9f75d632e2f8`
executed one Provider and one KMS test with zero failure/error/skip, but it is an intermediate result that must be
rerun after the final source is frozen. It proves no production KMS deployment and does not promote C2.

The first complete allocator raw-matrix attempt at the same source is retained only as diagnostic evidence. It
completed the native workload before the first fault transition exposed a production instrumentation defect:
concurrent reservations for one exact Oxia authority key were rejected as an overlapping binding. Its JUnit result is
exactly one test, one failure, zero errors, and zero skips; no allocator mode or RANGE size was selected. The preserved
external diagnostic directory is
`/Users/liusinan/Documents/Codex/2026-08-24/nereus-v2-m3-allocator/diagnostic-overlap-99ab89d5`.
The repair removes the key-global trace map and gives each top-level production-store invocation one explicit bound
client that carries its mutation and same-key reread as a single request association. A deterministic test completes
two same-key chains in reverse order and verifies their independent operation sequences plus cross-key rejection; the
fresh six-test contract gate and two-test native-path gate pass with zero failure/error/skip. The repair does not
serialize the real workload or relax raw-event validation. A new raw matrix must still select at most one qualified
mode before source locks and the formal allocator receipt can be frozen. The M2 regression and Provider/KMS results
must subsequently be rerun for freshness at the eventual exact Final source.

The next full diagnostic at exact source `60ae2d8ee52c14af7e4411313baae2e6c10dde9a` completed the native matrix and
entered the STRICT 10k fault batch, but its reported five-minute worker-termination failure masked the first task
failure during `BROKER_SESSION_CRASH_MASS_TAKEOVER`. It is preserved outside the repository as
`diagnostic-worker-drain-60ae2d8e`; its checksum manifest SHA-256 is
`7a877c1e02bfca4021c44fc37cc9bdfa89c62949e62f21b9c632f5d9dea34afb`, and its JUnit is exactly one test, one
failure, zero errors, and zero skips. It selects no mode and is not evidence. The runner now uses bounded,
interruptible Oxia waits, drains every submitted completion before rethrowing the first exact failure, attaches a
cleanup failure only as suppressed, and exposes a separate 10k STRICT real-Oxia fault-batch task that writes no
receipt. The expanded eight-test allocator contract and two-test native-path gates pass with zero
failure/error/skip.

That fault-batch task then ran at exact clean source `c44b60f1f9f5c304effebcbb996bca33d9271959` and returned the
previously hidden first failure in 3.732 seconds: exact-current-Cell proof correctly rejected a stale version while
affected-ledger Head takeovers were interleaved with rollovers that changed the shared Cell. Its JUnit is one test,
one failure, zero errors, and zero skips; external checksum-manifest SHA-256 is
`cca2f457601402c9647e5da25694bf3d0c4f0544bfcda3fb5451cd7cbfd81816`. This is diagnostic only and selects no
mode. The harness now separates the concurrent Head-takeover phase from the following fresh-owner rollover phase, so
all Head takeovers prove the unchanged exact Cell before any rollover mutates it, and each concurrent Cell proof read
has a request-local telemetry binding. The fresh nine-test allocator contract and two-test native-path gates pass
with zero failure/error/skip. The replacement exact-source diagnostic at
`6335ed1d62e04509972de66d79509f5ec40715cc` then passed the full STRICT 10k nine-cut batch as one test with zero
failure/error/skip in 86.270 seconds; its external checksum-manifest SHA-256 is
`e9e9a4dc5805daa71683658441540c256dfaa4b3876ed6df7b96a7804be3cb27`. It emits no receipt, covers neither RANGE
nor 100k/throughput rows, selects no mode, and promotes no scenario. Its historical complete-matrix rerun requirement
is superseded by ADR 0104; only a completed validator-proof V2 campaign may become selection input after the new
pre-campaign gates pass.

The next complete-matrix attempt at exact clean source `e4f376c63e1b8458c8798d5ea9ca56cf39364377` ran for 40 minutes
4 seconds, completed the native matrix, and then failed in the STRICT 10k mass-takeover second phase because 2,500
fresh-owner rollover completions did not drain within 120 seconds. Its JUnit is exactly one test, one failure, zero
errors, and zero skips; external checksum-manifest SHA-256 is
`e15ac54a7ef3e5b53cb98d034a6952dcf856d4ee84b9cf89b686a6c928e957bb`. It is diagnostic only, writes no selection,
and promotes no scenario. The failure exposed a workload mismatch rather than permission to extend a threshold:
ADR 0100 now fixes the mass-recovery endpoint as exact production Head takeover followed by no-allocation fresh-owner
append admission, while normal intervals remain the allocator-rollover throughput authority. The runner retains the
complete affected-ledger inventory, exact write/reread/typed-terminal proofs, frozen 30/60-second selection limits,
typed late timeout, and a separate 120-second post-deadline cleanup cap. The fresh seven-test raw parser suite and
ten-test allocator contract gate pass with zero failure/error/skip. The exact-source replacement diagnostic at
`023a2006f89e699ed621a61291c739d04d01dd54` then passed the complete STRICT 10k nine-cut batch as one test with zero
failure/error/skip in 5.871 seconds. Its external checksum-manifest SHA-256 is
`149a267fbdd29c1cc58c71a67db115f28ead75ce5599e0aa8f17c8390ee785d0`; every listed file rehashes exactly and the
task-owned Oxia container was removed. This remains diagnostic only: it emits no receipt, covers neither RANGE nor
100k/throughput rows, selects no mode, and promotes no scenario. The complete raw matrix at the eventual exact source
remains mandatory.

The following complete-matrix diagnostic at exact clean source
`d819500f6da8d024e77bc1bcb26ba7dcf4ee42da` ran for 5,071.065 seconds before RANGE concurrency exposed
`CELL_STATE_DRIFT`: an installed-range Head/node path had captured a cached Cell while a different request advanced the
same Cell through reserve/install/clear. JUnit is exactly one test, one failure, zero errors, and zero skips, and no
selection/evaluation/verifier file exists. The closed external directory is
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/full-matrix-d819500f-r4`; every file rehashes against
`SHA256SUMS`, whose SHA-256 is `bd2ae96c38ccb7a1fb416541dab9109684bdddfff3e0d734f74a84ce2805ca67`.
[ADR 0101](../../../decisions/0101-v2-m3-allocator-cell-proof-concurrency-scheduling-amendment.md) preserves the
production exact-current-Cell check and gives installed RANGE paths a fair shared proof phase while Cell mutation
chains use an exclusive measured phase. RANGE population construction captures its exact Cell only under that exclusive
phase, and construction rejects inherited injected latency. The focused formal-runner contract now passes 11 tests
with zero failure/error/skip. Its historical complete-matrix rerun requirement is superseded by ADR 0104; the
diagnostic cannot be promoted independently.

The first exact-source ADR-0101 diagnostic at
`69c81bef9348631e39dabf91db15385cf48cd116` constructed 10,000 RANGE-16 Heads/grants and overlapped 4,096 installed
Head/node allocations with Cell-wide range renewals. Its one testcase completed in 54.982 seconds with zero
failure/error/skip. The outer interactive zsh wrapper then returned exit `1` because it used bash-only `PIPESTATUS`,
even though the exact Gradle run log ended `BUILD SUCCESSFUL`; this post-test orchestration failure and the subsequent
exact-target manual removal of task-owned container
`706b6a0f2789ededf761c06b22b0ec7e8062f1835bb20af53eb605af660aa212` are retained in `orchestration.json` rather
than hidden. The closed directory is
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/diagnostic-range-cell-proof-69c81bef`, and its
`SHA256SUMS` SHA-256 is `854d44ecad346a4446d874628f00b97c49d079bccbe9c753a86f05bd2dfc5638`. This remains diagnostic only, emits no
selection/receipt, and does not replace a 10k RANGE nine-cut diagnostic or the complete five-candidate matrix.

The follow-up exact-source RANGE-16 fault diagnostic at
`3f1fa7caeb2c3e409d436e57c875562e573d9fdc` constructed the complete 10,000-Head population and executed all nine
ADR-0094 cuts against real Oxia. Its one testcase completed in 47.696 seconds with zero failure/error/skip, the bash
wrapper returned zero, and task-owned container
`43b7163e3fc6eb26b9d3ca5e00cb7b698698b92bf0d0dd0fe379039f285fe774` was removed by its exit trap. The closed
directory is `/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/diagnostic-range-fault-3f1fa7ca`, and
its `SHA256SUMS` SHA-256 is `68a4e4188e610bf8413c4256cf91446adbeb39a591fcf57ef9fdaf1b3d4c3d0d`. It emits no raw selection inventory or
receipt, covers neither 100k nor native/throughput rows, selects no mode, and promotes no scenario. It is not an input
to ADR 0104's V2 campaign.

The following complete five-candidate matrix at exact clean source
`1ef4f108307cb95a06fd5c55950b041eebadc813` then executed for 15,462.505 testcase seconds and returned exactly one
test with zero failure/error/skip. Sealing still failed closed: Gradle's exact JUnit XML was 113,519,059 bytes, of
which 113,518,209 bytes were `system-out` containing 970,241 copies of the expected native-harness cleanup warning
`ManagedLedgerImpl - Ledger was already deleted`. The fixed 16-MiB NAEA1 JUnit cap rejected it, so no
`evaluation.json`, `selection.nars`, or `raw-verification.json` exists and no mode or RANGE size was selected. The
closed diagnostic directory is
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/full-matrix-1ef4f108-r5`; all ten listed files
rehash against `SHA256SUMS`, whose SHA-256 is
`5d896755fb3434539e5817105bc871e2004124f431405bc92648ffe02c8a573d`.
[ADR 0102](../../../decisions/0102-v2-m3-allocator-junit-diagnostic-output-containment-amendment.md) leaves the cap,
parser, workload, SLOs, and selection rule unchanged. The exact runner now rejects only that exact WARN/message pair,
retains every other WARN and every ERROR, and verifies the active Log4j decisions before constructing a population.
Because the runner artifact digest changed, r5 raw archives cannot be resealed or reused. ADR 0104 later supersedes
that historical complete-matrix rerun requirement.

That immediate rerun at exact clean source `bd254d2463c6bdfd0ab46bc8cd8c6f5b9abe016e` retained the fixed JUnit cap
and exact-message filter, but failed during RANGE-1024 10k-to-100k population expansion. Its testcase ran for
14,635.286 seconds and returned one test, one failure, zero errors, and zero skips. The 1,988-byte JUnit XML contains
no filtered cleanup-warning residue; the exact failure is `allocator population construction did not drain 90000
completions within 600 seconds`. A strict read-only context scan shows native, STRICT, RANGE-16, RANGE-64, RANGE-256,
and RANGE-1024 10k measurement present, with no RANGE-1024 100k measurement context. No `evaluation.json`,
`selection.nars`, or `raw-verification.json` exists. The closed diagnostic directory is
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/full-matrix-bd254d24-r6`; all nine listed files
rehash, and its `SHA256SUMS` SHA-256 is
`a06a09fe9ed6a2fbc4340a9f8200ee57998b77de91057cd39569cfd6d4f5fad2`.
[ADR 0103](../../../decisions/0103-v2-m3-allocator-population-construction-batch-scheduling-amendment.md) keeps the
600-second overall construction cap, 120-second operation cap, exact Cell proof, candidates, workload, and selection
unchanged. RANGE construction now holds one outer exclusive immutable-Cell phase, drains unique Head creates in
parallel, proves the captured Cell unchanged, and only then executes initial grant chains in index order. Timeout
progress is diagnostic-only and the owned batch is interrupted/cancelled. The fresh focused contract now passes 13
tests with zero failure/error/skip. r6 remains non-promotable and is not reusable by ADR 0104's V2 campaign.

At the committed/pushed replacement source `9f88fbfb115e35d1e41ab8aacedeb0a1233fca0e`, the diagnostic-only 10k
RANGE-16 Cell-proof task passes its one testcase in 49.909 seconds with zero failure/error/skip against the exact real
Oxia image. Gradle completed successfully in 1 minute 3 seconds. The outer zsh wrapper then failed only while reading
the Bash-specific `PIPESTATUS`; orchestration records `wrapperExitCode=1`, preserves the exact successful Gradle log
and JUnit, and records exact manual removal of the task-owned container. The four listed files rehash under
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/diagnostic-range-cell-proof-9f88fbfb-r2`, whose
`SHA256SUMS` SHA-256 is `ce8cd001d7b48675daeddd8b56eac39e9f3b3c4546df96187980bec1b3db0d47`. This remains
10k diagnostic-only coverage and selects nothing. A separate
`realAllocatorRange100kConstructionDiagnosticTest` now exercises RANGE-1024 through the exact 10k-to-100k
construction boundary with the same production SPI, real Oxia adapter, and unchanged caps, while emitting no NAEA1,
selection, receipt, or scenario result. At exact clean source `e739799f9e22922124cf8369900d2d699b5c7518`, the
formal preflight admitted the locked real Oxia/Pulsar/Oxia-client tuple with zero blockers and the probe passed one
testcase in 459.537 seconds with zero failure/error/skip; Gradle completed in 7 minutes 51 seconds and the task-owned
container was removed automatically. Gradle 9 shortened the XML source basename because of its length, so the wrapper
did not copy the expected full-name path; orchestration records that boundary and the unique exact XML was copied
without modification. The five-file diagnostic set lives at
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/diagnostic-range-100k-e739799f-r1` and rehashes
under `SHA256SUMS` SHA-256 `1161419f12ad18b6402a31c36f42f2f7571a97ecc540f217d562a075d8e85229`.
This construction-only result is not native-relative evidence, selection, a child receipt, or scenario PASS; the
complete formal matrix must not be resumed. The later interrupted
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/full-matrix-16254510-r1` directory and all earlier
V1 products are immutable diagnostic-only. [ADR 0104](../../../decisions/0104-v2-m3-allocator-validator-proof-adaptive-campaign-amendment.md)
retains all 288 logical performance cells but replaces exhaustive formal execution with a validator-proof adaptive V2
campaign. It separates logical, executed, and disposition cells; freezes 13/17/288 execution bounds and independent
phase budgets through an offline `--plan-only`; requires four independent actor coordinators and bounded physical
admission; and separates completed evaluation from promotion. No full formal V2 campaign may run until its V2
planner/validator, V1 compatibility, bounded-runner conservation, four-actor workflow, and short real-Oxia diagnostics
all pass with zero failure/error/skip.

The first ADR-0104 implementation slice is now pure domain code. `AllocatorCampaignV2` freezes the 288 logical cells
and raw interval/fault schemas; `AllocatorCampaignPlannerV2` descends native and eligible candidate rates, distinguishes
executed cells from typed dispositions, and still searches RANGE after STRICT qualifies; the validator recomputes
every disposition and both offered/admitted conservation equations before the selector can emit any of the four
closed outcomes. Thirteen focused tests cover 13/17/288 executed-cell bounds, native-baseline absence, caller
disposition tampering, rate ordering, drop conservation, all four outcomes, and incomplete-evaluation rejection with
zero failure/error/skip. The code has no Oxia dependency and does not create a checkpoint, receipt, selection, or
scenario PASS. The next slice adds `BoundedVirtualLedgerAllocatorWorkflowV2` in the metadata SPI. One coordinator
executes the exact production STRICT or RANGE chain with a frozen request ID, descriptor, exact Head, and slice view;
predecessor-unchanged, indeterminate, and definitive-conflict outcomes have distinct bounded reconcile paths. It owns
no Java lock or worker pool, never changes candidate identity after derivation, and rejects owner, slice/context, and
descriptor drift with typed codes. Ten focused tests cover all response-loss points, two independent coordinators
contending through Cell CAS, retry/deadline/backoff exhaustion, post-timeout dispatch rejection, and proof that only
one ledger ID is consumed. The four-actor Runner,
`M3V2BoundedActorLaneRunner`, is now implemented with a physical `2 * offeredRate` queue split across four lanes and
one in-flight request per lane. At cutoff it drains every non-admitted request only to
`OVERLOAD_DROPPED_BEFORE_ADMISSION`; admitted work has the single frozen five-second cleanup deadline and exactly one
completed/failed/timed-out terminal. It records backlog, in-flight, waiter, per-lane maximum, queue age, rollover, and
terminal facts. `M3V2AllocatorFormalHarness` adapts four identity-distinct instances of the production-neutral
workflow and revalidates both conservation equations before returning an interval. Seven focused tests prove Cell
concurrency four, per-lane concurrency one, bounded overflow/cutoff, exact terminal partition, warm-up separation,
independent actor routing, and no Java correctness lock/worker pool. These tests use no real Oxia and create no V2
campaign or evidence. Checkpoint/resume/sealer/promotion wiring and short real-Oxia diagnostics were still required at
that intermediate boundary; the final implementation slice and exact-source result below close only those
prerequisites, not formal campaign or promotion authority.

The final ADR-0104 implementation slice now adds strict canonical protocol and gate wiring without running a formal
campaign. `NACP2` binds the full logical inventory, ordered executed facts and attachment digests, validator-reproved
dispositions, exact source/image/dependency/executor/workload tuple, independent remaining budgets, and checkpoint
lineage under a two-MiB cap. Resume cannot change the source tuple, remove/reorder an observation, increase a budget,
or skip a predecessor digest. Only complete NACP2 can seal fixed `NAEV2`; interrupted and infrastructure-failed states
produce no evaluation. NONE/BOTH seal normally but the promotion gate returns valid non-promotable status. The four
short real-Oxia scenario names seal separately as fixed diagnostic-only `NADV2`, and promotion additionally rehashes
all formal attachments, both diagnostic and formal zero-skip JUnit files, and the exact NAEV2 rederived from NACP2.
JUnit suite totals must equal the independently counted testcase failure/error/skip nodes.
The formal workflow envelope is now exactly 64 retries, four seconds total elapsed, and 25 milliseconds maximum
backoff, which terminates inside the Runner's fixed five-second cleanup grace. Its request-local lock-free store guard
prevents a late asynchronous completion from dispatching another operation after timeout. Gradle exposes offline
pre-campaign/checkpoint/evaluation/promotion tasks and a separate four-test real-Oxia
diagnostic task/sealer. The old script's default exhaustive/V1 path now refuses execution; no full formal campaign is
enabled.

At exact Nereus source `5d86b572e826f56a29726ca7c77f1c98bc941e4b`, the separate real-Oxia diagnostic
passes STRICT, installed RANGE, range renewal, and four-independent-coordinator conflict storm as `4/0/0/0` and seals
212-byte diagnostic-only NADV2 `9694673ac388f7ce79a7338ef7d9d932c227854973c3ee52843ace5bb4dbd6e3`; the
exact JUnit SHA-256 is `a8b0f884097b6698e56f69562700c6fe42a9bb284601667e0abd574a80255f3e`. It binds
Oxia image ID `7eef9af2...f4da`, source-lock SHA `c3e5dffb...dfe9d`, executor SHA `b0bdc2f4...e5b5f`, and
workload-plan SHA `1291acd8...9872f`. This is a prerequisite diagnostic only: it produces no NACP2, NAEV2, formal
receipt, mode/range selection, scenario PASS, or authority to run the prohibited full formal matrix.

[ADR 0105](../../../decisions/0105-v2-m3-preselection-evidence-source-lock-amendment.md) versions the typed-evidence
source-lock contract as `NEREUS_V2_M3_EVIDENCE_SOURCE_LOCKS_V2`. Its `UNSELECTED` state permits exact non-allocator
child evidence without fabricating a mode, while allocator sealing and Final still require the uniquely qualified
`STRICT` or `RANGE` result. Kafka, Pulsar, and allocator identities now derive from the dedicated M3 branch/image
locks, not historical M1/M2 bindings. Recording a future selected mode is a production-source change and requires
fresh Final-source child evidence.

At clean published source `e4d207e3e0526e85fece0401497a18f5e73d226c`, the locked MinIO C1 and Vault Transit
formal tasks plus their required ordinary session tests pass `4/0/0/0`. The resulting
[C1 child](../../evidence/v2-m3/children/04-C1_REAL_PROVIDER_KMS/receipt.json) is exact-source and
non-promotable with SHA-256 `184fa0e2e470d2800d8b504425cac0b5d9236f26e57c304bf37510e7e8cca180`;
its governed JUnit, KMS, and Provider attachments are `3fd6dd0d...b7f3`, `11ea8c8d...c940`, and
`e1e1b105...3a5f`. It keeps C2 false and Vault production deployment unproven. Because the source lock remains
`UNSELECTED`, this is a preselection checkpoint and must be rerun for Final after a selected-mode source change.

The D1 local half now has a separate formal execution chain. `v2M3LocalCapEvidenceTest` executes only the six
source-governed capacity testcase identities, and `ObjectWalLocalCapacityHarnessV1` writes the exact six-record
`NEREUS_V2_M3_D1_LOCAL_CAP_RESULT_V1` payload with CREATE_NEW after the runner hashes the harness, its test, and all
six production component sources from the exact tested Git commit. The runner seals the exact JUnit XML through the
generic child publisher and reparses the local result through the governed validator. The payload remains
allocation-free analytical format-cap conformance, claims no Provider transfer, and cannot substitute for the
separate fixed-digest C1 Provider/KMS evidence.

At clean published source `f0a3310ddca57da63b6854d5b841db813f883269`, the governed A/B execution passes
20/0/0/0 and publishes the exact-source, non-promotable
[NWG1 child](../../evidence/v2-m3/children/01-AB_NWG1_WIRE/receipt.json) with SHA-256
`26c5009843bcc69489882522d86ed7981c5a82484a039b3873fe064e6701e14b`. Its typed attachments bind the
single canonical JCS manifest containing six positive vectors, two synthetic external fixtures, all 16 component
kinds, 84 explicit mutation records, 240 paths, 25 rejection codes, 16 validation stages, and the fixed ZSTD 1.5.7
frames, plus the separate exact 114-row TSV. This preselection checkpoint claims neither production ZSTD exact
compressor output nor scenario promotion and must be rerun after a selected-mode source change for Final freshness.

Serial continuation order completed the ADR-0104 V2 schema/planner/validator/selector, bounded CAS/reconcile workflow,
bounded four-actor Runner, checkpoint/resume/sealer/promotion implementation, and required short real-Oxia diagnostic
slices. The present execution authority still explicitly prohibits any full formal allocator campaign even though
these prerequisite gates are clean. Work therefore continues with the remaining non-matrix M3 closure and child
receipts; final-source Provider/KMS and M2 regression freshness, scenario synchronization, and M3 Final remain open.
Each stable boundary must be committed and pushed before the next evidence-bearing boundary is evaluated.

## Current boundary

| Slice | Design or output | Status at this documentation cut |
| --- | --- | --- |
| M3-I0 | [NWG1 implementation-input closure](m3-i0-nwg1-implementation-input-closure.md), [ADR 0089 Header amendment](../../../decisions/0089-v2-m3-nwg1-v1-header-layout-amendment.md), and [ADR 0090 mutation-call profiles](../../../decisions/0090-v2-m3-nwg1-mutation-external-call-profiles.md) | accepted documentation-only input with exact Header offsets and explicit X0/XU call caps; no codec, runner, trace harness, Provider evidence, receipt, or scenario PASS |
| M3-W1 | current-source M2 regression plus M3 module/API input gate | M3 inputs and the eleven-module source-qualified API closure pass; historical M2 Final remains immutable; ADR 0098 closes the M3-only prerequisite projection; the exact-source `89a76612…` formal profile publishes 25/25 children and 687 zero-failure/error/skip tests as a non-promotable W1 checkpoint, while Final still requires a freshness rerun at its eventual exact tested source |
| M3-W2 | NWG1 production encoder/decoder, projection, six-vector A corpus, and exact wire gate | exact `f0a3310d...` A/B evidence passes 20/0/0/0 and publishes non-promotable child `26c50098...e14b`, binding the six-vector/two-fixture canonical manifest and exact 114-row TSV; a fresh Final-source rerun remains required after allocator selection |
| M3-W3 | 84-record/240-path B mutation manifest and runner | the same exact-source A/B child binds 84 authored records/240 paths, 25/25 rejection codes, and 16/16 validation stages with no generated inventory; a fresh Final-source rerun remains required after allocator selection |
| M3-C1 | 50-trace Object-WAL kernel harness | common kernel and manifest implemented; local 7-test gate covers 50 traces/21 outcomes, but backend integration and exact-source receipt remain open |
| M3-D1 | local capacity conformance and exact Provider C1/C2 evidence | exact `2b9636dd...` local-cap evidence publishes non-promotable 12/0/0/0 D1 child `6769769b...2a78` and explicitly claims no Provider/KMS execution; exact `e4d207e3...` real Provider/KMS plus ordinary session evidence passes 4/0/0/0 and publishes non-promotable C1 child `184fa0e2...a180`; fresh Final-source D1/C1 reruns remain required after allocator selection, and C2 remains non-promotable without independent benefit evidence |
| M3-R1 | WalRun Root/Pointer/checkpoint/Seal and Provider/KMS session implementation | common control/session/recovery source implemented and `:nereus-storage-object:check` passes 147 tests; Kafka/Pulsar backend integration and dedicated-fork compile checkpoints pass, while formal Provider/KMS receipts remain open |
| M3-K1 | Object `NWKCP1` plus `KafkaProtocolCheckpointHeadV1` | Kafka source implemented; local 260-test module check covers strict wire/key/caps, OPEN/TERMINAL Head, backend mapping and bounded recovery; the dedicated fork now compiles against the split F9/M3 artifact inputs with 6/6 tests, while the final source-qualified native receipt remains open |
| M3-U1 | M2 publication bridge, active-tail locators, Binding frontiers, recovery, and source protection | Kafka source implemented and locally tested, including one-fence owner-open staging and whole-suffix rollback; the dedicated-fork dual-repository compile checkpoint passes 6/6 tests, but a source-qualified receipt remains open and native broker/controller activation remains M6 |
| M3-P1 | Pulsar fixed-slice Object-WAL path and allocator evidence/selection | local Nereus Object-WAL/controller implementation is committed at `bc8691a636456cef48119ded637ea027679b0903` and its 140-test module gate passes; dedicated Pulsar branch `7ff908330809f2e9bc5c69ead87bb85c566bc0a9` passes its 5-test native-boundary compile checkpoint; ADR-0104's pure V2 schema/planner/validator/selector and 13 focused tests enforce the 288-cell inventory, 13/17/288 execution bounds, descending rates, independently reproved dispositions/conservation, and valid non-promotable NONE/BOTH outcomes without Oxia; the production-neutral bounded workflow preserves request/candidate identity through exact CAS/reread, four-second elapsed/25-ms backoff/64-retry bounds, typed fail-closed outcomes, and post-timeout dispatch rejection without a Java lock; the physical four-actor Runner and production-workflow harness cover queue/cutoff/terminal conservation and concurrency four with one in-flight per actor; strict NACP2/NAEV2/NADV2 checkpoint, evaluation, diagnostic and promotion gates are implemented without enabling the old full path; exact-source short real-Oxia diagnostics pass 4/0/0/0 and seal non-promotable NADV2 `9694673...d6e3`; formal real/native 10k/100k receipt, RANGE size, mode selection, and scenario PASS remain open, and no full formal campaign is currently allowed |
| M3-FINAL | exact-source aggregate and scenario promotion | fail-closed child/final checker and publisher contracts are implemented and their 74 governance tests pass; Final remains open and requires all owned slices, current-source M2 regression, real Provider/KMS/allocator evidence, and the exact M3 scenario allowlist |

Slice names are execution labels, not new durable wire codes. Implementations may split reviewable commits more
finely, but may not merge authority, evidence, or promotion boundaries merely to reduce the number of commits.

ADR 0089 closes the missing Header offset table before any production NWG1 byte exists, so `wireVersion=1` remains the
first production version. The Header has no node session, owner witness, body SHA, or duplicate packing-class field;
`laneId` is the permanent class ID. It fixes SHA-256/v1 as Object digest code `1/1` and all twelve accepted
first-satisfied close-reason codes, while normal target/linger values remain evidence-owned.

ADR 0091 separately closes M3-P1's allocator wire/key/transition input with exact `NVAC1`/`NVAH1`/`NVAN1` bytes,
STRICT's inseparable four-write path plus exact prior-owner RESERVED-node burn that consumes without publishing,
RANGE same-RESERVED takeover with no unused-tail regrant, exact stored Cell/Head/node provenance and one-ID burn, and
receipt-only exact-source activation. Its closed raw path is the same-directory five-file
`test/native/fault/scale-10000/scale-100000.naea` inventory plus fixed `selection.nars`; the parser rehashes source and
runtime artifacts, reparses exact JUnit XML, replays queue/interval/fault facts, and runtime activation hashes the
packaged domain/SPI/Oxia JARs. Request-keyed writer shards preserve one request's async endpoint order without
inventing a global file order. Formal candidates traverse the same production coordinator but remain
`runtimeActivated=false`. Its 48 ordinary allocator tests and deterministic `8 workloads x 9 cuts` schedule are local
implementation conformance only until the exact-source verification run records zero failure/error/skip.
`V2-OPEN-PUL-OBJ-09` remains evidence-blocked until real multi-broker/native 10k/100k execution selects an exact RANGE
size and at most one mode.

## Required execution order

The safe dependency order is:

```text
M3-I0
  -> M3-W1
  -> M3-W2 + M3-W3
  -> M3-C1
  -> M3-R1 + M3-D1
  -> M3-K1 + M3-U1 + M3-P1
  -> M3-FINAL
```

Independent code and evidence work may overlap only after their immutable input commit is fixed. No later slice may
rewrite a committed golden, Root contract, state trace, or older receipt to make a gate pass.

## Input and promotion rules

- The historical M2 Final receipt is an immutable ancestor proof, not current-source regression.
- M3 Final binds a complete current-source M2 regression receipt whose tested commit is exactly the M3 tested commit.
- Every production wire has one machine projection, one byte authority, strict parser caps, and immutable vectors.
- The NWG1 projection mechanically transcribes ADR 0089's exact Header table and cannot add an independent field.
- Every focused gate has non-zero tests and zero failure, error, and skip; a focused receipt remains non-promotable.
- Real Provider, real KMS, allocator scale/fault, and source-qualified cross-repository evidence cannot be inferred from
  deterministic fakes.
- Scenario Markdown and JSON move together and only after the owning executable evidence exists.
- M3 Final does not activate native Kafka or Pulsar broker/controller paths; M6 retains process integration.

## Explicit M3-I0 exclusions

The input closure deliberately excludes these as positive NWG1 authority:

```text
positive Storage Epoch ordinal
mixed FrameEncodingPolicy production support/evidence
exact output of the production Zstandard compressor
complete WalRunRoot/CurrentWalRunPointer canonical wire from a synthetic fixture
```

It also does not select evidence-dependent packing target/linger values, Provider proof mode, recovery-skip
certificate, Pulsar allocator mode/exact RANGE size, or production Root caps. Those choices close only through their
owned M3 evidence slices and synchronized ADR/open-question updates. ADR 0091's allowed RANGE domain is a hard
implementation cap, not an evidence-selected range size.
