# ADR 0081: V2 M1 pure active graph and promotion boundary

## Status

Accepted for the 0.2 M1 implementation and cross-repository promotion workflow. M1 implementation, the pure-V2
active-graph prune, exact-source validation, N3 receipts, and M1 Final are complete for the current source tuple. The
focused slice receipts remain deliberately non-promotable; current completion comes only from the canonical N3
evidence and scenario manifest accepted by `v2M1FinalCheck`.

### Implementation refinement (2026-08-11)

`v2M1FoundationCheck` enforces the first two dependency nodes, exact capability inventory, forbidden API surface, local
tests/goldens, and reproducible artifact hashing. It is deliberately not the ADR's `v2M1Check`/Exact/Final ladder and
does not weaken the requirement to remove the active V1/KoP runtime graph before M1 completion.

### Active-graph edge-cut refinement (2026-08-13)

The accepted first prune cut limited settings, BOM, publication, ordinary CI, and compiled `nereus-metadata-oxia`
sources to BOM/domain/SPI/Oxia only. The following independent mechanical commit removed the disconnected V1/KoP
runtime files, non-V2 Oxia sources/tests, Phase/F9 scripts, and V1 Admin Docker runtime while retaining archives and
deferred design. This completed the pure active-graph precondition before N2/N3 promotion.

### Promotion completion refinement (2026-08-13)

The trusted N2 execution produced canonical Fast and Exact Source PASS results over the pure-V2 graph and exact
Kafka/Pulsar/Oxia/artifact tuple. Evidence-only N3 promotes exactly `V2-POSITION-003..011` through one
`REGISTRY_CONFORMANCE`, one `HARNESS_CONFORMANCE_ONLY`, and one Final index. No allocator mode or M2 data path is
selected or activated.

## Context

M0 froze the V2 contracts while `main` still compiled and published a large V1 module graph. The root build also kept
historical Phase/F9 gates and three different Pulsar source identities across CI, Gradle defaults, and the V2 source
manifest. Building that graph cannot establish V2 M1 readiness, while deleting the graph in the same change as the new
metadata architecture would make a roughly thousand-file mechanical removal inseparable from correctness review.

M1 also needs one logical aggregate validator in Nereus, Kafka KRaft, and the Pulsar/Oxia path. Making Kafka metadata
depend on the current broad `nereus-api` would pull V1 data-path types into KRaft; copying a second validator would make
semantic drift a correctness risk. Cross-repository evidence has a separate circularity: a receipt cannot be committed
at the product commit whose previously unknown commit ID it claims to test.

## Decision

### Pure V2 active graph

M1 finishes with a pure V2 Gradle, BOM, publication, CI, and executable graph. No V1 runtime, deprecated compatibility
shim, Phase/F9 task, or V1-only script remains compilable, publishable, or runnable from `main`. KoP runtime is outside
the active graph while its design documents remain present. Protected `v0.1` and `v0.1.0` history is the only
implementation reference for removed V1 code.

The transition is deliberately reviewable:

1. create the V2 domain/SPI foundation and dependency-boundary checks;
2. cut old modules from settings, BOM, publication, and CI;
3. remove V1 runtime sources and historical executable tasks/scripts in separate mechanical commits.

Architecture replacement, gate rewiring, and the large mechanical deletion are not one commit. M1 Final nevertheless
fails if any forbidden V1 graph remains.

### Contract modules

The dependency direction is:

```text
nereus-domain <- nereus-metadata-spi <- nereus-metadata-oxia
```

`nereus-domain` targets Java 17 and is JDK-only. It owns the logical canonical forms, deterministic identities, and
closed validators. It has no Kafka, Pulsar, Oxia, asynchronous-framework, or backend-version dependency.

`nereus-metadata-spi` depends only on `nereus-domain` and exposes exactly the aggregate publisher/reader, Pulsar topic-
generation selector, and Pulsar virtual-ledger namespace Registry capabilities. It cannot expose child binding/Epoch
stores, a generic key/value `get/put/delete` facade, or an umbrella `MetadataStore`. One
`VersionedAggregateSnapshot` supplies both domain projections. Create and CAS operations use the closed result sets in
ADR 0082 and exactness includes key, schema, digest, and canonical stored bytes.

Physical Kafka API-key-32000 wire is still generated and owned by Kafka. Kafka `:metadata` depends only on the exact
`nereus-domain` artifact, maps the physical record directly to a domain value, and invokes the domain validator without
a canonical encode/decode round trip. It never transitively imports the SPI or Oxia and implements none of those SPIs.

Every cross-repository domain artifact is immutable and source-qualified. Promotion records its JAR and POM SHA-256;
Kafka/Pulsar cannot consume an overwriteable SNAPSHOT, a `changing=true` dependency, or an unconstrained whole-product
composite.

### Milestone boundaries

M1 owns Kafka's complete KRaft metadata authority: feature-2 bootstrap activation, API-32000, atomic CreateTopics
publication, TopicImage/Delta/replay/snapshot/remove ownership, and publication-boundary validation form one activatable
source tuple. A generated record may land dormant, but no feature advertisement, format, or record emission may precede
that complete tuple. M6 owns broker/controller process integration and end-to-end Produce/Fetch/Admin/restart evidence;
it does not reimplement record/image authority.

M1 owns the Pulsar selector, an ABA-safe backend-native opaque ownership witness, stale-install-safe ACTIVE-fence
installation/invalidation, and focused tests. Full aggregate-to-retired-tombstone replacement remains M5, and complete
Pulsar process integration remains M6. Normal append/read captures and rechecks one atomic local fence word.

M1 owns the mode-independent virtual-ledger Registry SPI, real-Oxia conformance, INSTANCEID-derived compatibility-
namespace identity for a genuinely fresh ledger root, inline complete writer commitment/admission interlock, and
versioned derived slice view. Registry correctness emits a distinct
`REGISTRY_CONFORMANCE` receipt. STRICT/RANGE candidate SPI, fault cuts, and receipts are test/evidence-only; production
metadata SPI cannot expose them. Harness evidence remains `HARNESS_CONFORMANCE_ONLY` with `selectionEligible=false`,
runs only deterministic and small smoke workloads, persists no allocator mode, and cannot promote Registry scenarios.
M3 owns 10k/100k multi-broker capacity evidence and any mode selection.

### Gates, source locks, and promotion

`docs/v2/source-locks.json` is the only expected-SHA authority for external Kafka, Pulsar, Oxia, and other admitted
sources. Checkout paths may be overridden; expected SHAs may not. Gradle, CI, and scripts parse the manifest directly
and keep no V2-path SHA default. The current Nereus commit is not self-locked by that manifest; the final receipt binds
the tested product commit.

M1 has three non-duplicating gates:

- `v2M1Check` uses no Docker, fork, or composite build and covers local domain/schema/SPI/codec/harness contracts, the
  active module graph, and V1 absence;
- `v2M1ExactSourceCheck` verifies clean exact Kafka/Pulsar checkouts before and after execution, uses an isolated
  immutable artifact repository, runs real Oxia and focused fork suites, and rejects every source/artifact mismatch;
- `v2M1FinalCheck` first runs `v2M1EvidenceFreshnessCheck`, then aggregates those outcomes and validates the receipt
  schema without rerunning their suites. Freshness requires a clean checkout, the receipt-bound Nereus commit as a
  strict ancestor of HEAD, the current source-lock-file digest, and a linear descendant history in which every changed
  path is under `docs/v2/evidence/v2-m1/n3/`.

Zero discovered tests, skipped mandatory tests, failures, dirty/source-changing checkouts, stale Final evidence, or
digest mismatches cannot pass. Fast PR CI runs `v2M1Check`; exact/final gates run only after the repository's promotion
workflow is backed by a protected `v2-m1-promotion` environment and dedicated `nereus-v2-m1` runner. That workflow
regenerates Fast/Exact gate results, normalized JUnit reports, both canonical receipts, and the Final index, then
byte-compares all seven N3 files before Final; its YAML, environment configuration, runner registration, or a queued
run is not promotion evidence without a successful run.

Cross-repository promotion uses four stages and at least five commits:

1. N1 publishes the foundation/domain/SPI in `InProgress`;
2. P1 and K1 consume the exact N1 artifact in Pulsar and Kafka;
3. N2 locks P1/K1, records candidate gate state, runs Final, and produces a receipt binding N2/P1/K1;
4. N3 commits only the receipt/evidence and promotes only its exactly covered scenarios.

Virtual-ledger conformance receipts share one RFC-8785/JCS canonical JSON envelope with the closed payload kinds
`REGISTRY_CONFORMANCE` and `HARNESS_CONFORMANCE_ONLY`. They record tested product/fork commits, source-lock digest,
domain JAR/POM SHAs, Oxia server-image/client/test-artifact identities, and one authoritative
`scenarios[] -> suites[]` hierarchy. Each suite has `discovered/executed/passed/failed/skipped/aborted`; scenario and
overall results are deterministic derived summaries, never independent authorities. Harness non-selection is a schema
constant. Accounting, retry prohibition, safe attachment grammar, and PASS semantics are fixed by ADR 0084. Registry
bytes, admission evidence, writer interlock snapshots, test reports, and sanitized log excerpts are content-addressed
allowlisted attachments whose root references contain kind, safe relative path, length, and SHA-256. The canonical root
contains only `schema`, `kind`, `sourceTuple`, `scenarios[]`, and `attachments[]`. It has no leaf IDs, independent
aggregate result, or allocated/random/time-based `runIdentity`; canonical receipt SHA-256 is its content identity.
Registry and harness receipts cannot substitute for one another.

The Final index is only a promotion manifest containing `schema`, `sourceTupleSha`, `requiredGateRefs[]`, and
`receiptRefs[]`. Each typed reference binds safe relative path, length, and SHA-256. The validator reads those objects
and computes final status; a persisted display status, if any, is exact-rechecked and never overrides them. Exact root,
count, path, attachment, and log numeric caps remain OPEN until representative early-M1 all-pass, maximum-failure,
fault-cut, Registry/interlock, and multi-scenario evidence derives them with margin.

N3 may commit only receipts/evidence attachments and the scenario status/index exactly covered by them. It cannot
change code, gates, workflows, ADRs, or source locks; such a change returns to N2 and reruns promotion. Content hashes
bind bytes but do not prove provenance, which comes from the trusted workflow and protected N3 commit. A scenario whose
M1 portion passes cannot thereby claim an M3, M5, or M6 result; cross-M1 scenario rows are split before implementation
promotion.

## Consequences

- M1 review separates architecture, build-boundary changes, and mechanical V1 deletion, while still enforcing a pure
  V2 final graph.
- Kafka accepts a small immutable domain dependency in exchange for one executable validator and no V1/Oxia leakage.
- Normal Kafka/Pulsar data paths gain no remote I/O, canonical hashing, or string parsing from M1 metadata validation.
- The main costs are low-frequency metadata CAS, takeover validation, immutable artifact construction, and trusted
  cross-repository promotion.
- M1 can verify registry and harness conformance without making an unevidenced allocator selection or borrowing M3/M5/
  M6 PASS status.

This decision is refined by ADRs 0082..0085, refines ADRs 0006, 0009, 0032, 0034, 0042, 0043, 0050, 0051, 0054, and 0055 and
is tracked by the M1 implementation plan and its milestone-specific scenario rows.
