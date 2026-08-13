---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: ImplementationDesign
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m1/g1/README.md
---

# G1 receipt validation and M1 gates

## Boundary

G1 turns the accepted M1-2 receipt limits into a JDK-only production parser and supplies the Fast, Exact Source, and
Final gate mechanics accepted by ADRs 0081, 0084, and 0085. It consumes K1/P1/R1 focused outputs; it does not rerun a
referenced gate from Final, select an allocator mode, activate a data path, delete V1, promote a scenario before N3, or
claim M1 PASS from its focused test alone.

The receipt package lives in `nereus-domain`. It has no JSON library, Gradle, Kafka, Pulsar, Oxia, async-framework, or
filesystem-facade dependency. The virtual-ledger receipt kinds remain exactly `REGISTRY_CONFORMANCE` and
`HARNESS_CONFORMANCE_ONLY`; they are not an exhaustive enum for every M1 focused wrapper.

## Closed receipt root

`VirtualLedgerReceiptV1` accepts exactly the persisted M1-2 grammar:

```text
schema
kind
sourceTuple
scenarios[]
attachments[]
```

It enforces strict UTF-8, canonical closed-model JSON bytes, the accepted numeric caps, sorted and unique scenario,
suite, and attachment identities, checked count equations, non-zero all-pass mandatory suites, and the fixed source
tuple. Canonical bytes are the receipt identity. There is no caller-supplied aggregate PASS, run ID, retry count,
dynamic-test count, allocator selection, or scenario-promotion bit.

Attachment verification validates the complete allowlisted path before I/O and streams exactly the declared bytes
through SHA-256. On providers with `SecureDirectoryStream`, each component and final open is descriptor-relative with
`NOFOLLOW`. Providers without that API use the accepted equivalent: every component is read with `NOFOLLOW`, the
final file is opened with `NOFOLLOW`, and a non-null file key, type, and size must remain identical before and after the
stream. Symlink, replacement, early EOF, extra byte, wrong length, or wrong digest fails closed.

## Final index and resolver

`M1FinalIndexV1` accepts only:

```text
schema
sourceTupleSha
requiredGateRefs[]
receiptRefs[]
```

Every reference is typed and binds a safe relative path, length, and SHA-256. Gate results are closed to Fast and
Exact Source with PASS/FAIL plus the same source-tuple SHA. `M1FinalResolverV1` reads already generated canonical
objects, verifies both required gates are PASS, verifies one exact common source tuple, and applies the fixed
`M1PromotionPolicyV1`. It never invokes Gradle, a process, Docker, a fork, a test framework, or a network service.

The promotion policy requires `V2-POSITION-003..009` from `REGISTRY_CONFORMANCE` and
`V2-POSITION-010..011` from `HARNESS_CONFORMANCE_ONLY`, with named suite coverage. Missing, extra, duplicated, wrong-
kind, failed, skipped, aborted, source-mismatched, or digest-mismatched evidence cannot pass. This policy covers only
the virtual-ledger conformance receipts; Fast and Exact Source retain authority over the remaining M1 K1/P1/local and
cross-repository evidence.

## Evidence-only allocator candidates

The deterministic STRICT and RANGE candidate model is under `nereus-domain/src/test` only. It covers reserve, install,
node/head, clear, response-loss exact reread, incarnation-owned RANGE takeover, stale-candidate rejection, and exact
single-ID burn. No candidate type, mode enum, range size, selection result, or allocator mutation API exists in
production domain, metadata SPI, or metadata Oxia code. Its only promotable envelope kind is
`HARNESS_CONFORMANCE_ONLY`, whose non-selection meaning is fixed by kind.

## Gate split

- `v2M1G1ValidatorCheck` is a focused, non-promotable implementation gate for parser, safe references, Final resolver,
  policy, and allocator-harness isolation.
- `v2M1Check` is the no-Docker/no-fork fast gate and additionally owns the final pure-V2 active graph and V1 absence.
- `v2M1ExactSourceCheck` runs the already defined exact Kafka/Pulsar/Oxia and real-Oxia checks against the immutable
  source tuple.
- `v2M1EvidenceFreshnessCheck` binds the authoritative N3 Final index to the current clean Git checkout. Both receipt
  kinds must name one tested Nereus commit and the exact current `source-locks.json` digest. The tested commit must be a
  strict ancestor of HEAD, and every intervening single-parent commit may modify only
  `docs/v2/evidence/v2-m1/n3/`; intermediate code changes that are later reverted still fail.
- `v2M1FinalCheck` accepts the authoritative Final index, depends on that freshness check, and only resolves its
  referenced canonical objects. It never depends on the Fast or Exact execution tasks and therefore cannot
  accidentally rerun them.

The three promotion CLI tasks execute from the already built JDK-only `nereus-domain` JAR and resolve their complete
argument vector during task configuration. This avoids cross-project runtime-classpath resolution and script-object
closures, so Gradle configuration-cache storage is itself part of the executable N2 gate boundary.

Before N2, focused evidence remains non-promotable and scenarios remain `PLANNED`. N2 generates gate results and
canonical receipts under the final source tuple. N3 may commit only those evidence objects and the exactly covered
scenario promotion. Any code, gate, workflow, ADR, or source-lock change returns to N2. The promotion workflow requires
a protected `v2-m1-promotion` environment and dedicated `nereus-v2-m1` runner, checks out the source-locked external
repositories, requires the source-qualified Oxia image, regenerates both gate-result bytes, compares them with N3, and
then invokes Final. Missing environment/runner configuration, a queued workflow, or an unsuccessful run means no
refreshed promotion evidence.

## Focused verification

The historical focused implementation at Nereus `ba11fe4a29c3158bb4d7c46e379c9a918745b7ef` runs four receipt suites with
49 tests and two allocator evidence suites with 14 tests. The checkout-to-Final addition separately runs eight
deterministic Git-history boundary tests for clean evidence-only descendants, dirty worktrees, non-ancestors,
intermediate reverted code, source-lock mismatch, and symlinked authority.
It checks exact inventory, zero failure/error/skip, JDK-only imports, absence of test/build execution from Final, and
absence of allocator candidates in production. The [focused receipt](../../evidence/v2-m1/g1/README.md) binds the
exact implementation commit and remains `PASS_G1_FOCUSED_ONLY`. It is not Fast, Exact Source, Final, N2, N3, or M1
PASS evidence.
