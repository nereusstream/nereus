---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: ImplementationDesign
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m1/p1/README.md
---

# P1 Pulsar selector and ownership fence

## Status and boundary

This is the code-level design for P1. It implements the accepted contracts in ADRs 0033, 0051, 0081, 0082, and 0085
without reopening their semantics. P1 owns the Pulsar name-generation selector, immutable aggregate publication/read,
authoritative ownership witness, A/read/B validation, continuity and record invalidation, and one allocation-free local
ACTIVE fence. It does not activate Produce/read storage, choose a virtual-ledger allocator, replace M5 aggregate
retirement, or claim a scenario/M1 PASS. Full BrokerService/PersistentTopic process integration remains M6.

The source inputs are:

- Nereus `main` and immutable N1 domain/SPI source `330aaec349c51fb2ace52b1085e8a9e5a60b5e3e`;
- Pulsar pure-V2 fork base `5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551`;
- Oxia client continuity fork `091a42c2780d92da56e9ec1f02ce1c3d988adc16`;
- Oxia server source `37a17bef17202d5fd6e23282da5fd26d94865484` and the source-locked focused image.

The original focused receipt remains immutably bound to the V1-residue development base
`11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` and fork `778862323d8a86e2f36064a12166e09918ed9429`.
The final M1 source is the clean pushed pure-V2 branch `nereus/v2-m1-p1-selector-fence-pure-v2` at
`d1cfd863b0e0ffad9c141abf68beeb2350a1ea16`. It replays only the P1 authority commits onto the stock base and has no
`nereus-pulsar-adapter`, `0.1.0-f2-dev`, non-V2 Nereus runtime, dynamic SNAPSHOT, `changing=true`, Maven Local, or
Nereus composite-build input. The N2 exact-source gate reruns the focused inventory against this final fork; it does
not relabel the historical receipt.

## NPS1 selector encoding

`NPS1` is the Oxia value for `PulsarTopicGenerationSelectorValueV1`. It is a flat big-endian encoding:

```text
magic                              4 bytes, ASCII NPS1
schemaVersion                      u16be, exactly 1
state                              u16be: RESERVED=1, ACTIVE=2, DELETING=3, DELETED=4
bindingGeneration                  u64be interpreted as positive signed long
persistenceNameLength              u32be
persistenceName                    strict UTF-8, 1..4096 bytes
aggregateBindingId                 32 bytes
aggregateCanonicalStoredDigest     32 bytes, SHA-256 of exact NTA1 bytes
```

The exact length is `84 + persistenceNameLength`, with a maximum of 4,180 bytes. The decoder rejects malformed UTF-8,
unknown schema/state, zero/negative/overflow generation, non-canonical Pulsar persistence names, truncation, trailing
bytes, and a value whose authority key does not rederive from the persistence name. `canonicalStoredBytes` in the SPI
value is exactly this NPS1 byte sequence; `canonicalStoredDigest` is its SHA-256. Neither digest is embedded in NPS1.
Aggregate bytes remain NTA1 at their separate incarnation-scoped key.

The legal transitions are exact and closed:

```text
ABSENT       -> RESERVED(1)
DELETED(g)   -> RESERVED(g + 1)
RESERVED(g)  -> ACTIVE(g)
ACTIVE(g)    -> DELETING(g)
DELETING(g)  -> DELETED(g)
```

Every successor except recreation preserves persistence name, generation, aggregate binding ID, and aggregate digest.
Recreation requires the next positive generation and a matching new aggregate identity. Unknown responses use exact
reread and only the SPI closed outcomes. `EXISTING_EXACT`, `APPLIED_EXACT`, and `PREDECESSOR_UNCHANGED` require exact
key, schema, digest, canonical stored bytes, and backend version semantics; partial-field equality never advances the
machine.

Creation/open performs selector reserve, immutable aggregate create, exact aggregate reread/validation, then selector
ACTIVE CAS. Deletion performs ACTIVE-to-DELETING before native deletion and DELETING-to-DELETED after the native cut.
RESERVED and DELETING are recovery states, not hot-path admission states. Full aggregate-to-tombstone replacement stays
in M5.

## Nereus implementation slice

`nereus-metadata-oxia` replaces only the unavailable selector codec with:

- `Nps1SelectorAuthorityCodec` and immutable golden/corruption vectors;
- `PulsarTopicGenerationTransitionsV1`, a pure validator for the five legal edges above;
- `PulsarTopicAuthorityCoordinator`, which composes the existing four narrow SPI capabilities without adding an
  umbrella metadata store or a cross-key transaction claim;
- a local authority-notification registration that maps exact selector/aggregate key notifications and every O1
  continuity loss/close to invalidation callbacks.

`OxiaV2CodecSet.productionP1()` enables NTA1 plus NPS1 while Registry remains fail closed. The aggregate and selector
adapters continue sharing one `AsyncOxiaClient`, conditional mutation engine, authority root, and store-wide continuity
epoch. `productionActivationReady()` remains false until R1 and native runtime composition are present; a narrower
`pulsarSelectorReady()` may report the codec/store capability only.

The current Nereus implementation now includes the exact NPS1 codec and transition kernel, the
`PulsarTopicAuthorityCoordinator` reserve/create/reread/activate/delete state machine, and a capability-store-owned
exact-key plus store-wide continuity invalidation registry. The latter arms before authority reads, supports bounded
idempotent local deregistration, de-duplicates one binding callback across its selector and aggregate keys, handles
range deletion, and treats READY only as permission to revalidate. The source-locked real-Oxia integration now proves
concurrent exact creator convergence, lifecycle restart/recreation, exact aggregate reread, and record-notification
invalidation in two focused, zero-skip tests against `nereus/oxia-o1:37a17bef1720`. Pulsar fork `09fe914e4a` first added the
canonical broker/acquisition identities, closed transition validator that precedes `force`, authoritative direct-get
witness, A/read/B installer, and single-word local fence with stale-install and stale-close exclusion. Final fork
`778862323d` additionally locks exact N1/P1/O1 artifacts, consumes opaque SPI metadata versions, cross-validates the
selector/aggregate authority, aligns Oxia to 0.9.4, and adds a closed capability gate that rejects every unqualified
ownership writer, TableView, syncer, continuity, or ordered-invalidation configuration. Pure-V2 final fork
`d1cfd863b0` reproduces that V2 package and its 7-suite/34-test inventory on the stock `5.0.0-M1` graph while omitting
the V1 adapter entirely. Exact P1 artifact
packaging exposes only the V2 capability package, the public READY-only continuity permit used around A/read/B, and
deterministic binary/source artifact tasks. The immutable bundle at Nereus `23064b3b` is locked as
`com.nereusstream:nereus-metadata-oxia-p1:0.2.0-p1.23064b3be10169d0fe1bb6f23abd7f2bded4bbd5`; two clean builds are
byte-identical, and `v2M1P1ArtifactCheck` verifies the closed package, exact N1/O1 dependencies, source descriptors,
manifest, current non-zero regression tests, and non-promotion receipt. `v2M1P1FocusedCheck` now binds that artifact,
the clean pushed Pulsar fork, 14 Nereus metadata suites with 94 tests, one real-Oxia suite with two tests, and
seven Pulsar suites with 34 tests. Every selected test has zero failure/error/skip. The resulting `P1_FOCUSED_ONLY` receipt
remains `promotionEligible=false`; it is not process activation, scenario promotion, or M1 PASS.

Notification callbacks invalidate but never grant admission. Continuity registration is armed before authority reads.
READY only permits bounded revalidation; a gap, ARMING, CLOSED, reassignment, client close, or unknown reconnect first
invalidates all P1 local words. Exact record notifications invalidate the affected binding. Registration and close are
bounded and owned by the capability store; callbacks perform no blocking metadata read.

## Pulsar native ownership witness

P1 supports only MetadataStore-backed ELM with syncer disabled and all ownership writers upgraded. Legacy ownership,
system-topic TableView, an eventual local TableView read, `force` bypass, unconditional syncer writes, stable broker
endpoint, and resettable `versionId` do not qualify. Unsupported backends fail V2 admission.

Each broker process creates a non-zero, CSPRNG 128-bit `brokerIncarnationId`. Each real service-unit acquisition creates
a non-zero, process-deduplicated CSPRNG 128-bit `acquisitionId` before the first conditional Assigning write. A response-
unknown retry and same acquisition renewal reuse it. Transfer target, forced takeover, missing/tombstone recreation,
split child, process restart, and SessionLost reacquisition use a fresh acquisition ID; restart and SessionLost also
use a fresh broker incarnation. A qualified same-session reconnect may retain the identities but invalidates ACTIVE and
requires A/read/B again.

The MetadataStore ELM ownership value carries canonical lower-hex 16-byte broker-incarnation and acquisition IDs.
Assigning creates them for the target, Owned preserves them, source-side Releasing/Splitting preserves the incumbent,
and a target Assigning or split-child acquisition replaces them. One closed transition kernel validates every native
writer; `force` changes the allowed business edge but does not bypass identity/version validation.

An authoritative witness read directly reads the exact `/service_unit_state/<service-unit>` record and backend Stat,
not the eventual TableView. It accepts only exact `Owned`, the local broker, valid identities, canonical bytes, and an
unchanged backend version. The provider returns one immutable witness containing service unit, broker incarnation,
acquisition ID, canonical ownership bytes/digest, and backend version. Missing, malformed, stale, non-local, or
unsupported storage returns no authority.

## A/read/B and local ACTIVE fence

For one binding, the installer executes:

```text
arm continuity and exact-key invalidation
capture exact INVALID word and READY continuity permit
authoritative ownership witness A
read exact ACTIVE selector
read exact immutable aggregate
authoritative ownership witness B
require A == B and exact selector/aggregate/incarnation agreement
require continuity permit still current
CAS exact INVALID word -> VALID authority word
```

The cache is binding-scoped and uses one `AtomicReference` to an immutable word. INVALID contains a strictly increasing
local sequence and continuity epoch. VALID additionally binds persistence name/incarnation, binding/storage-epoch IDs,
selector and aggregate backend versions/digests, broker incarnation, acquisition ID, and ownership backend version.
The whole object reference is the compare-and-set unit; fields are never published separately.

Ownership loss, selector/aggregate notification, continuity loss/close, store close, or native transition away from
local Owned first replaces the current word with a higher-sequence INVALID word. An old installer can therefore never
restore VALID. A watch only invalidates. READY, cache miss, or ownership existence alone never installs authority.

Normal append/read admission loads the VALID reference once and completion/ACK or response publication rechecks
reference identity. It allocates no object, parses no digest/token, and performs no Oxia/MetadataStore I/O. A changed
word yields fenced/outcome-unknown behavior, never success. Control-plane revalidation may be coalesced by service unit
and bounded by Cell/host policy.

## Implementation and commit order

1. Nereus: NPS1 codec/goldens, transition kernel, notification registration, fake response-loss and continuity cuts.
2. Nereus: real Oxia selector/aggregate create-CAS-read tests and a source-qualified P1 adapter artifact/manifest.
3. Pulsar: lock exact N1/P1 artifacts; add broker/acquisition identities and the closed native ownership transition
   kernel in reviewable commits.
4. Pulsar: add authoritative witness provider, A/read/B coordinator, atomic ACTIVE cache, and focused race/fault tests.
5. Nereus: bind clean Pulsar before/after HEAD, exact artifacts, real Oxia result, test accounting, source locks, and a
   `P1_FOCUSED_ONLY`, `promotionEligible=false` receipt.

Each implementation step runs focused tests, related module checks, formatting/checkstyle, and `git diff --check`
before its commit and push. Architecture edits and external-source code remain separate commits.

## Focused evidence and stop condition

The P1 gate must prove NPS1 golden/corruption boundaries; every selector state and response-loss cut; conflicting
creator and generation overflow; exact aggregate coupling; authoritative A/B mismatch; ownership transfer/recreation;
gap/reconnect/SessionLost; stale installer; old completion after invalidation; local hot-path capture/recheck; unsupported
backend refusal; clean exact source before/after; and real Oxia CAS/notification continuity.

The focused P1 implementation is complete: both repositories are clean and pushed, exact source/artifact/receipt
identities are committed, all selected tests have non-zero counts and zero failure/error/skip, and
`v2M1P1FocusedCheck` passes. This result still does not select an allocator, prune V1, activate full process data paths,
promote a scenario, or complete M1.
