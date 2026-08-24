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
- Exact M3 source locks, fresh child receipts, aggregate Final, and Markdown/JSON scenario promotions do not exist.
  `implementationStatus: InProgress` and `evidenceStatus: NotRun` therefore remain authoritative.

Serial continuation order is current-source M2 regression, formal exact-source Provider/KMS and allocator evidence,
and finally final source locks, child receipts, scenario synchronization, and M3 Final.
Each stable boundary must be committed and pushed before the next evidence-bearing boundary is evaluated.

## Current boundary

| Slice | Design or output | Status at this documentation cut |
| --- | --- | --- |
| M3-I0 | [NWG1 implementation-input closure](m3-i0-nwg1-implementation-input-closure.md), [ADR 0089 Header amendment](../../../decisions/0089-v2-m3-nwg1-v1-header-layout-amendment.md), and [ADR 0090 mutation-call profiles](../../../decisions/0090-v2-m3-nwg1-mutation-external-call-profiles.md) | accepted documentation-only input with exact Header offsets and explicit X0/XU call caps; no codec, runner, trace harness, Provider evidence, receipt, or scenario PASS |
| M3-W1 | current-source M2 regression plus M3 module/API input gate | M3 inputs and the eleven-module source-qualified API closure pass at clean source `98a6384a9a0cf0a4579212f2df9b9a760a3b1f59`; historical M2 Final remains immutable, while the complete current-source M2 regression receipt at the eventual M3 tested source remains open |
| M3-W2 | NWG1 production encoder/decoder, projection, six-vector A corpus, and exact wire gate | common source implemented; local 17-test/114-row wire-source gate passes, but no exact-source child receipt exists |
| M3-W3 | 84-record/240-path B mutation manifest and runner | common source implemented; local 3-test gate covers exactly 84 records/240 paths with no generated inventory, but no exact-source child receipt exists |
| M3-C1 | 50-trace Object-WAL kernel harness | common kernel and manifest implemented; local 7-test gate covers 50 traces/21 outcomes, but backend integration and exact-source receipt remain open |
| M3-D1 | local capacity conformance and exact Provider C1/C2 evidence | S3 C1 and Vault Transit production adapters plus formal clean-source tasks are implemented; nine ordinary tests and two fixed-digest diagnostic real tests pass with zero failure/error/skip, but diagnostic results are non-evidence, formal exact-source receipts remain open, and C2 remains non-promotable without independent benefit evidence |
| M3-R1 | WalRun Root/Pointer/checkpoint/Seal and Provider/KMS session implementation | common control/session/recovery source implemented and `:nereus-storage-object:check` passes 147 tests; Kafka/Pulsar backend integration and dedicated-fork compile checkpoints pass, while formal Provider/KMS receipts remain open |
| M3-K1 | Object `NWKCP1` plus `KafkaProtocolCheckpointHeadV1` | Kafka source implemented; local 260-test module check covers strict wire/key/caps, OPEN/TERMINAL Head, backend mapping and bounded recovery; the dedicated fork now compiles against the split F9/M3 artifact inputs with 6/6 tests, while the final source-qualified native receipt remains open |
| M3-U1 | M2 publication bridge, active-tail locators, Binding frontiers, recovery, and source protection | Kafka source implemented and locally tested, including one-fence owner-open staging and whole-suffix rollback; the dedicated-fork dual-repository compile checkpoint passes 6/6 tests, but a source-qualified receipt remains open and native broker/controller activation remains M6 |
| M3-P1 | Pulsar fixed-slice Object-WAL path and allocator evidence/selection | local Nereus Object-WAL/controller implementation is committed at `bc8691a636456cef48119ded637ea027679b0903` and its 140-test module gate passes; dedicated Pulsar branch `7ff908330809f2e9bc5c69ead87bb85c566bc0a9` passes its 5-test native-boundary compile checkpoint; allocator production/evidence code is committed at `a95e492e2597a48c77bcbdc2354ca189d93fc552` and its ordinary 48-test inventory passes; ADRs 0091/0094/0097 freeze the wire, raw workload/SLO/selection, and reproducible evidence-image inputs, but the formal real/native 10k/100k receipt, RANGE size, mode selection, and scenario PASS remain open |
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
