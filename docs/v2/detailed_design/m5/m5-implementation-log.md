---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: FocusedOnly
authority: ImplementationLog
sourceTuple: v2-m1
---

# M5 implementation log

This log tracks implementation descendants of the immutable M5 hard-freeze. It does not amend the six documents
bound by `m5-design-freeze.json`, and it is not a child receipt, scenario receipt, canonical Final, staging
certification, or production authority.

## Design freeze

- accepted design commit: `c86fde3ed6f4319642987fd599022bd32e2cca5e`;
- design aggregate at that source: `v2M5DesignCheck` = `DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED`;
- immutable predecessor: M4 tested source `595c8b34779d1e88187eb0084bf18e65ab2dd742` and Final SHA-256
  `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07`.

## Accepted M5-D target authority amendment (governance only)

[ADR 0147](../../../decisions/0147-v2-m5-target-scoped-physical-delete-authority-amendment.md) accepts the narrow
`M5-AMENDMENT-2-TARGET-SCOPED-PHYSICAL-DELETE-AUTHORITY-V1`. The chained
`m5-design-amendment-2.json` binds the immutable base freeze, exact `m5-design-amendment-1.json`, and the two new
normative documents. It substitutes one permanent target-scoped authority key and exact same-key CAS for only the
unavailable M5-D multi-key linearization primitive; the original M5-D document remains byte-identical.

At this entry the amendment implementation remains NotStarted and evidence remains NotRun. Acceptance creates no
intent, dispatch, physical-delete, source-bound receipt, scenario-promotion, staging, or production authority. The
focused M5-D adapters below remain foundations until the authority record, complete closed-writer integration, real
Oxia intent/done path, external execution composition, and source-bound child all pass.

## M5-A materialization and manifest publication

Status: implementation-complete at the focused, non-promotable gate; source-bound child evidence has not run.

Implemented surfaces:

- exact common identity envelope, typed coverage, source cut, source membership root, deterministic task/output
  identity, task lifecycle, immutable generation, validation root, and manifest view;
- deterministic `REFERENCE_REUSE`, `INDEX_ONLY_GENERATION`, and `REWRITE_GENERATION` selection, with BookKeeper
  forcing rewrite and healthy Object-WAL payloads reused rather than copied;
- fixed NMS1 v1 physical projection with strict caps, source/extent/index directories, payload/index digests,
  canonical re-encode checks, and a fixed-size footer binding every section and total length;
- a production byte-preserving materializer that fully rereads every exact source, emits deterministic NMS1 payload
  and index candidates for rewrite/index-only modes, and emits zero new payload candidates for reference reuse;
- machine-readable physical codes, domains, offsets/caps, flags, and lookup rule in
  `m5-a-wire-projection.json`, validated by the M5-A source checker;
- canonical sparse lookup index implementing floor, exact-coverage, then successor behavior;
- independent full source/output/index reopening, length/SHA/Provider-version validation, byte-preserving comparison,
  boundary/gap lookup checks, and owner/worker/storage/capability/selector freshness checks;
- immutable source-cut/validation/generation/manifest publication followed by the existing M4 selector CAS as the
  only mutable read authority; duplicate and lost-response paths converge by exact reread;
- persisted finite per-Cell task/source/output/member/part/index/unknown-outcome reservation accounting; and
- a Cell-scoped C1 Object session wrapper that accepts only exact create/adopt results and performs bounded
  LIST-plus-full-GET reconciliation for response loss.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5MaterializationCheck
PASS_V2_M5_MATERIALIZATION_IMPLEMENTATION_NON_PROMOTABLE
M5MaterializationV1Test: 7 tests, 0 failures, 0 errors, 0 skipped
```

The focused gate also reparses the immutable M4 dependency, keeps all 17 M5 scenario rows `PLANNED` with null
receipts, rejects a missing runtime surface or broken design ancestry, and runs storage-object Spotless/Checkstyle.
It does not satisfy the future `MATERIALIZATION_MANIFEST_PUBLICATION` real-provider/BookKeeper child, does not remove
fallback or release M4 protection, and grants no metadata-retirement or physical-delete authority.

## M5-B Kafka semantic compaction and complete index rebuild

Status: implementation-complete at the focused, non-promotable gate; native source-bound differential evidence has
not run.

Implemented surfaces:

- strict parsing of exactly one assigned Kafka magic-v2 `RecordBatch` through Kafka clients 3.9.0, including CRC,
  compression, sparse/empty records, producer, transaction, control-marker, timestamp, and leader-epoch facts;
- a frozen candidate cut with exact source locators/bodies, policy generation, Durable/LEO/HW/LSO frontiers, all
  protocol-state roots, complete-domain key proofs, transactions, leader epochs, recovery-required offsets, and
  finite per-task caps;
- deterministic whole/partial/no-data selection for all seven dispositions, including bytewise cross-batch latest-key
  proofs, null-key retention, exact tombstone deadline behavior, conservative `RETAIN_UNKNOWN`, and unconditional
  transaction/control retention;
- sparse batch rewriting that preserves absolute offsets, producer ID/epoch/sequence interpretation, timestamps,
  keys, values, headers, transaction state, control bytes, leader epoch, and Kafka CRC validity;
- all eight index families rebuilt from canonical output, with explicit checksum/coverage gap rows and shared
  floor/coverage/successor behavior across leading, internal, and trailing gaps;
- independent semantic reread/selection/output/index validation and domain-separated plan, task, disposition,
  protocol-state, output-record, suppression, and semantic-validation roots;
- immutable fallback filtering that prevents a raw predecessor from resurrecting superseded values or expired
  tombstones removed by the preferred generation;
- persisted per-Cell admission for dirty bytes, batches, records, keys/key bytes, output/index bytes, transactions,
  tombstones, backlog age, spill, Provider/KMS/metadata operations, and response-unknown slots; and
- a final policy/root/frontier reread followed by M5-A's exact Object creation, semantic generation validation, and
  sole M4 selector CAS path.

The implementation-selected codes, caps, exact eight-index set, lookup rule, dependency lock, suppression rule, and
publication rule are machine-readable in `m5-b-wire-projection.json`.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5KafkaCompactionCheck
PASS_V2_M5_KAFKA_COMPACTION_IMPLEMENTATION_NON_PROMOTABLE
KafkaSemanticCompactorV1Test: 8 tests, 0 failures, 0 errors, 0 skipped
```

This focused result is not the future `KAFKA_COMPACTION_INDEX_REBUILD` child receipt. It does not promote
`V2-KAF-DATA-012/013/022`, close fallback, release M4 protection, retire metadata, or authorize physical deletion.

## M5-C retention core, rejected capability path, and accepted amendment

Status: implementation-complete at the source-locked, real-Oxia, non-promotable gate; the source-bound child receipt
has not run. The original multi-key retirement path remains rejected evidence, while ADR 0146 and the accepted
single-Binding authority amendment authorize the implemented exact single-key CAS path.

Direct source revalidation distinguishes Oxia's internal write batching from the required transaction. At client
commit `091a42c2780d92da56e9ec1f02ce1c3d988adc16`, `AsyncOxiaClient` exposes only individual key operations and
`client/.../batch/WriteBatch` is package-private transport batching. At server commit
`37a17bef17202d5fd6e23282da5fd26d94865484`, `oxiad/dataserver/database/db.go::applyWriteRequest` continues across
puts whose `applyPut` returns per-operation `UNEXPECTED_VERSION_ID`, then commits the accumulated RocksDB batch.
Consequently the storage commit is atomic, but one failed version condition does not abort the other writes; this
cannot implement the accepted all-conditions-or-zero-mutations protocol.

Implemented surfaces:

- a closed ten-class retention-floor snapshot, monotonic typed logical trim, exact version/value vectors, and a
  verifier that rereads every bound authority fact before accepting a snapshot or proof;
- exact closed floor/reference adapter registries and a deterministic assembler that rejects a missing adapter,
  foreign target/class, partial summary, present reference, or any authority version/value change during scanning;
- the complete closed 15-kind reference-veto inventory, bounded scan summaries, authoritative absence rules, audit
  deadline, and exact M4 `RELEASED` member bindings;
- canonical bounded codecs and deterministic keys for floor snapshots, trim frontiers, reference-free proofs,
  `FULL_V1` batches, permanent `RETIRED_V1` batch tombstones, and permanent Pulsar incarnation tombstones;
- an exact metadata transaction SPI that forbids sequential-CAS emulation, plus retirement coordinators whose
  externalization, batch retirement, and Pulsar aggregate replacement use that SPI only;
- full selector/batch response-loss reconciliation and impossible split-state quarantine, exact M4 inline-batch
  removal/release validation, same-key irreversible batch retirement, and Pulsar `DELETED(generation)` plus completed
  physical-cleanup prerequisites;
- persisted per-Cell/Binding hard caps for all 19 closed retention/admission limit kinds, exact derived usage,
  reserve-before-exceed behavior, and typed alerts; and
- an honest Oxia 0.9.4 adapter that supports response-loss-safe single-key CAS but returns `UNSUPPORTED` before any
  call for multi-key transactions. It contains no selector/batch sequential-CAS fallback.

The implementation-selected `m5-c-capability-projection.json` binds the closed inventories, source-locked client
identity, explicit unsupported behavior, and false authority flags.

Focused core gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5RetentionCoreCheck
PASS_V2_M5_RETENTION_CORE_NON_PROMOTABLE_OXIA_ATOMIC_TRANSACTION_UNSUPPORTED
M5RetentionRetirementV1Test: 36 tests, 0 failures, 0 errors, 0 skipped
M5RetentionEvidenceAssemblerV1Test: 67 tests, 0 failures, 0 errors, 0 skipped
Oxia09ExactMetadataTransactionStoreV1Test: 2 tests, 0 failures, 0 errors, 0 skipped
```

This historical focused-core result deliberately did not create the reserved `v2M5RetentionRetirementCheck`, did not
declare M5-C implementation-complete, and is not a child receipt. The accepted
[ADR 0146](../../../decisions/0146-v2-m5-single-binding-retirement-authority-amendment.md) and
[M5-C amendment](m5-c-single-binding-retirement-authority-amendment.md) preserve this failed path as a counterexample
and replace it with one Binding-scoped selector authority cell, durable reference-mutation tickets, a target scan
fence, and one exact Oxia single-key retirement CAS. The later complete implementation gate below realizes that path
without converting it into source-bound evidence. Logical trim and admission remain metadata-only; no physical
deletion, scenario promotion, staging certification, or production authority is granted.

The first amended implementation slice now passes `v2M5BindingAuthorityCheck` with these bounded surfaces:

- canonical `M5R1` `BindingRetirementAuthorityV1` at the existing M4 selector key, including byte-exact legacy
  selector migration, never-reused full/retired BatchId slots, predecessor/canonical digests, and hard value/count
  caps;
- a transparent M4 metadata facade that projects only activation-ordered `FULL_V1` slots, preserves all M5-only
  fields on every selector CAS, blocks M4 control mutation under a scan fence, and admits no second batch key;
- explicit enrollment of every closed floor/reference writer class before ticket or fence acquisition;
- durable target tickets, a zero-ticket `REFERENCE_SCAN_FENCED_V1` transition, exact M4 member-release reread, stable
  proof-vector reread, one exact single-key retirement CAS, and same-key response-loss convergence; and
- regression tests proving exact M4 projection, both ticket/fence orders, lost response, permanent tombstone
  projection removal, zero transaction calls, and no separate batch-key creation.

That Binding-only result remained a focused non-promotable slice; Pulsar aggregate authority was its next ordered
implementation dependency rather than authority supplied by the Binding gate.

The distinct Pulsar aggregate implementation slice now passes `v2M5PulsarAggregateAuthorityCheck`:

- one exact legacy NTA1 aggregate-key CAS installs a canonical `M5PA` authority envelope while the production Oxia
  publisher/reader continue to expose the exact original NTA1 aggregate bytes and digest;
- the envelope reuses the closed writer enrollment and durable per-reference-kind ticket protocol, then binds its
  zero-ticket scan fence to the exact permanent `NPS1 DELETED(generation)` selector version/value;
- same-name generation ABA, wrong BindingId, wrong original NTA1 digest, stale selector, incomplete cleanup, and stale
  proof-vector inputs all fail closed;
- only a complete `PhysicalCleanupSummaryV1` permits one exact aggregate-key CAS from fenced `M5PA` to permanent
  `M5PR`, with response-loss convergence from that same key and zero multi-key transaction calls; and
- Object-WAL `M5R1` and Pulsar `M5PA`/`M5PR` remain different codecs and authority cells.

That Pulsar-focused gate deliberately did not implement or certify M5-D cleanup, and it left closed writer
integration to the next ordered slice.

The closed writer integration slice now passes `v2M5ClosedWriterIntegrationCheck`:

- a canonical source-digest-bound registry assigns exactly one owner to every one of the 10 floor classes and 15
  reference kinds; missing, duplicate, mixed-capability, unknown, or oversized writer declarations fail before
  enrollment;
- the registry root becomes the exact enrollment implementation root in both `M5R1` and `M5PA` authorities;
- one shared guard exact-reads the enrolled authority, installs and rereads a durable target ticket, and only then
  dispatches the external mutation for the registered writer;
- only an exact authoritative terminal result with the ticket-bound external key-set root can clear the ticket;
  response loss, exceptions, partial/conflicting results, root mismatch, or failed clear retain the target; and
- exhaustive floor/reference ownership tests, all-reference ticket response-loss tests, ambiguous-retry recovery,
  fence-first no-dispatch, and both authority families run with no multi-key transaction fallback.

This remains a focused integration contract rather than a source-bound child receipt. It is an explicit predecessor
of the complete M5-C implementation gate.

The complete implementation gate now passes `v2M5RetentionRetirementCheck`:

- the task rebuilds `nereus/oxia-m3-allocator:37a17bef1720` from the exact clean Oxia server source commit
  `37a17bef17202d5fd6e23282da5fd26d94865484` and rejects any image whose ID differs from
  `sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da`;
- it binds Oxia client 0.9.4 source `091a42c2780d92da56e9ec1f02ce1c3d988adc16` and client JAR SHA-256
  `0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5` to the existing source locks;
- real Oxia tests migrate both legacy authorities, install/reread tickets, fence reference mutation, execute exact
  permanent one-key retirement CAS, reconcile exact retry, reconnect a fresh client, and reject stale predecessors;
- both authority families make zero multi-key transaction calls; and
- the task-owned container is removed on completion, while the projection keeps receipt, M5-D, physical-delete,
  scenario-promotion, and production-authority flags false.

Full implementation gate:

```text
./scripts/run-v2-m5-retention-retirement-check.sh /Users/liusinan/apps/ideaproject/nereusstream/oxia-worktrees/nereus-v2-m3
PASS_V2_M5_RETENTION_RETIREMENT_IMPLEMENTATION_NON_PROMOTABLE
M5RetentionOxiaIntegrationTest: 2 tests, 0 failures, 0 errors, 0 skipped
60 actionable tasks: 60 executed
PASS_V2_M5_RETENTION_RETIREMENT_REAL_OXIA implementation-only image=nereus/oxia-m3-allocator:37a17bef1720 id=sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da
```

This is not the `RETENTION_METADATA_RETIREMENT` child receipt because the eventual five children must bind one exact
tested Nereus source after M5-D and current-source isolation are complete. It promotes no scenario and authorizes no
physical deletion.

## M5-D version-match Object delete Provider foundation

Status: focused Provider slice implemented and executed against exact-digest MinIO; the full M5-D intent/done,
orphan, Pulsar, and BookKeeper implementation gate has not run.

- `ObjectProviderTransport` adds a default-unsupported delete capability and typed exact-version outcomes without
  changing the M3 C1 create/read/list contract or forcing existing adapters to claim deletion support;
- `S3C1ObjectProviderTransport.admitVersionMatchDeleteV1` admits deletion only after a live bucket-versioning read
  returns `ENABLED`, caps canonical version tokens, and dispatches `DeleteObject` against the exact version ID;
- `M5ObjectDeleteSessionV1` full-GETs and hashes the complete expected body, requires the immutable version from that
  response, constrains every key/list to one Cell namespace, and reconciles response loss through bounded complete
  LIST plus another full GET;
- exact old-version presence is retryable, a different version or body is conflict/quarantine input, and a recreated
  current version is never deleted by a stale exact-version operation; and
- fixed image `quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
  with image ID `sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
  passes the real versioning, delete, recreation, and lost-response paths.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5VersionMatchDeleteCheck
PASS_V2_M5_VERSION_MATCH_DELETE_PROVIDER_NON_PROMOTABLE
M5MinioVersionMatchDeleteTest: 1 test, 0 failures, 0 errors, 0 skipped
```

This slice has no API that can create an M5-D intent or declare `DELETE_DONE`; therefore it grants no physical-delete,
scenario-promotion, receipt, staging, or production authority.

## M5-D exact BookKeeper deletion adapter foundation

Status: focused adapter slice implemented and executed against the exact BookKeeper 4.18.0 server image; the complete
M5-D intent/done, orphan, and Pulsar cleanup gate has not run.

- `M5BookKeeperDeleteAdapterV1` captures a target only from an exact closed ledger and binds ledger/run/Cell identity,
  sealed last entry and length, quorum/digest/password-credential identity, metadata format/token, creation/state,
  custom metadata, and all ensembles into a deterministic SHA-256 fingerprint;
- an absent pre-read is idempotent completion without dispatch, while unsealed, rebound, or changed metadata is a
  conflict input and an ambiguous metadata read remains unknown;
- a delete return value, exception, or discarded response is never completion: every dispatch is followed by an
  authoritative metadata read, only the two BookKeeper no-such-ledger codes prove absence, the exact old target
  remaining is retryable, and changed metadata quarantines;
- unit tests cover response loss, exact-remains, changed metadata, pre/post-read ambiguity, stale identity, and both
  no-such-ledger outcomes; and
- the real fixed-image suite rejects a stale target, deletes one exact sealed ledger, reconciles absence, retries
  idempotently, and confirms target capture now reports definitive absence.

Focused real gate:

```text
./scripts/run-v2-m5-bookkeeper-delete-check.sh
PASS_V2_M5_BOOKKEEPER_DELETE_ADAPTER_NON_PROMOTABLE
RealBookKeeperCellSessionV1RealTest: 7 tests, 0 failures, 0 errors, 0 skipped
PASS_V2_M5_BOOKKEEPER_DELETE_REAL implementation-only image=apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d id=sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605
```

The adapter deliberately exposes no intent creation or dispatch-authority method. It may be invoked only by the
future complete M5-D coordinator after its exact persisted intent/fences authorize dispatch. This focused result is
not a source-bound child and grants no physical-delete, scenario-promotion, staging, or production authority.

## M5-D physical orphan and per-Cell admission core

Status: pure orphan/admission slice implemented; it has no external mutation transport and the complete M5-D gate has
not run.

- `M5PhysicalOrphanProtocolV1` closes the exact six-class taxonomy. Only physical output, multipart residue, and
  released-source candidates may enter mark; permanent metadata fences and allocator no-reuse evidence are always
  retained, while unknown/foreign identity is quarantine-only;
- mark requires authoritative owner-absent/released proof, a complete all-reference-absent scan, reconciled unknown
  create paths, and current fences. A live deterministic owner is adopted, contradictory facts quarantine, and LIST
  discovery or age alone retains;
- the immutable mark binds physical identity/content/version roots, the first complete scan, task/manifest versions,
  fences, response-loss root, and authority-time grace. Only a second complete exact rescan at or after the deadline
  produces `FUTURE_INTENT_CANDIDATE`;
- `M5PhysicalGcCellAdmissionV1` accounts candidate/intent/done inventory, delete/reconciliation/unknown queues, every
  Object/multipart/BookKeeper/Oxia/KMS/network concurrency class, rates/retries, cache/buffers, scanner work, and
  quarantine separately per Cell; and
- reserved minima cannot exceed the same Cell's hard limit, identities cannot cross-borrow, every cap and arithmetic
  overflow fails closed, while another Cell continues inside its own envelope.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5OrphanAdmissionCheck --rerun-tasks
PASS_V2_M5_BOOKKEEPER_DELETE_ADAPTER_NON_PROMOTABLE
PASS_V2_M5_ORPHAN_ADMISSION_CORE_NON_PROMOTABLE
M5PhysicalOrphanProtocolV1Test: 7 tests, 0 failures, 0 errors, 0 skipped
M5PhysicalGcCellAdmissionV1Test: 5 tests, 0 failures, 0 errors, 0 skipped
17 actionable tasks: 17 executed
```

The public protocol methods are only classify, mark, and rescan; readiness is explicitly a candidate for the future
persisted intent transaction, not authority to create intent or dispatch deletion. No receipt, scenario promotion,
physical-delete, staging, or production authority follows from this result.

## M5-D Pulsar root/data/multipart cleanup ordering core

Status: pure ordering slice implemented; it invokes neither the M2 unconditional delete seam nor any external
mutation, and the complete M5-D gate has not run.

- `M5PulsarObjectCleanupOrderV1` binds the exact sealed-ledger attempt/UUID, deterministic NPO1 root and NPD1 data
  keys, canonical lengths/full-body SHA-256 values, immutable Provider versions, persisted-intent binding,
  M4-release/reference-free/multipart inventory, and Provider-admission roots into one deterministic target root;
- only `DELETE_AFTER_VERIFIED` attempts can form a target, and a mismatched/rebound target root is rejected;
- the state machine first requires authoritative NPO1 root absence, then NPD1 data absence, then exact owned
  multipart-residue absence. Data-before-root and multipart-before-data are typed order violations and cannot advance;
- an exact old identity remaining is retryable without advancement, an unknown response remains unknown, and a
  different/foreign identity permanently quarantines; and
- the root Gradle gate explicitly selects the local Pulsar composite, preventing accidental resolution of the
  unpublished ManagedLedger snapshot dependency.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5PulsarCleanupOrderCheck --rerun-tasks
PASS_V2_M5_ORPHAN_ADMISSION_CORE_NON_PROMOTABLE
PASS_V2_M5_PULSAR_CLEANUP_ORDER_NON_PROMOTABLE
M5PulsarObjectCleanupOrderV1Test: 6 tests, 0 failures, 0 errors, 0 skipped
45 actionable tasks: 45 executed
```

The only public operations create an exact target, start pure progress, and observe typed reconciliation. There is no
external-mutation or intent-mutation API, no `DELETE_DONE`, and no receipt, scenario-promotion, physical-delete,
staging, or production authority.

## M5-D exact owned multipart cleanup adapter foundation

Status: exact owned multipart execution adapter implemented and exercised against fixed-digest MinIO; the complete
M5-D persisted intent/done coordinator and dispatch-authority gate have not run.

- `ObjectProviderTransport` adds a default-unsupported `EXACT_UPLOAD_ID_ABORT_V1` capability, exact immutable
  key/upload-ID values, bounded listing pages, two-marker continuation tokens, exact abort, and typed response loss;
- `M5MultipartCleanupSessionV1` binds the Cell Provider Scope, exclusive namespace, exact Provider identity, a
  non-empty persisted owned-inventory root, and finite page/upload/byte/key/upload-ID limits. It performs a complete
  exact-key scan before abort, refuses any same-key upload not in the persisted inventory, aborts only exact owned
  pairs, and completely rescans all persisted keys after every abort result;
- empty relist is the only authoritative absence result. Exact residue remains retryable, a changed/foreign identity
  quarantines, and incomplete, repeated-token, malformed, or over-budget listing fails closed;
- the S3 adapter encodes both key marker and upload-ID marker in one canonical bounded token. Fixed-digest MinIO
  demonstrates its narrower product behavior: directory-prefix multipart listing returns no inventory, while exact
  object-key listing is complete and paginates multiple upload IDs. The admitted adapter therefore never treats a
  directory-prefix scan as evidence; and
- the real test uploads a full 5 MiB part into each incomplete upload so the residue is observable, then covers
  same-key pagination, exact abort, response-loss reconciliation, and foreign-upload veto.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5MultipartCleanupCheck --rerun-tasks
PASS_V2_M5_MULTIPART_CLEANUP_NON_PROMOTABLE
M5MultipartCleanupSessionV1Test: 7 tests, 0 failures, 0 errors, 0 skipped
M5MinioMultipartCleanupTest: 1 test, 0 failures, 0 errors, 0 skipped
29 actionable tasks: 29 executed
```

The session has an external abort adapter because this slice proves transport/reconciliation behavior, but it has no
method to create intent, publish `DELETE_DONE`, or decide that dispatch is authorized. The focused result is not a
source-bound child and grants no physical-delete, scenario-promotion, staging, or production authority.

## M5-D target-scoped authority foundation

Status: pure target key/record/codec/state-machine foundation implemented; no metadata mutation or external adapter is
composed, and the complete M5-D gate has not run.

- `M5TargetDeleteAuthorityKeysV1` domain-separates the exact target by Cell Provider Scope and target kind, then
  derives the one permanent `v2/physical-delete-m5/.../authority-v1` key and complete dispatch-token root;
- `M5TargetDeleteAuthorityRecordsV1` closes five target kinds, ten proof-changing writer classes, four irreversible
  states, exact external identity, fixed intent/dispatch ownership and permanent completion proof under hard caps;
- `M5TargetDeleteAuthorityCodecV1` supplies the strict `M5DA` v1 canonical envelope, self digest, predecessor digest,
  trailing/truncated/unknown-code rejection and a revision increment for every successor; and
- `M5TargetDeleteAuthorityStateMachineV1` constructs pure writer-ticket acquire/reconcile, CAS-1 read fence, CAS-2
  identity/attempt/owner/capability binding, fenced-owner takeover and done candidates. It performs no CAS itself and
  calls no Provider or BookKeeper transport.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5TargetDeleteAuthorityFoundationCheck --rerun-tasks
PASS_V2_M5_TARGET_DELETE_AUTHORITY_FOUNDATION_NON_PROMOTABLE
M5TargetDeleteAuthorityV1Test: 11 tests, 0 failures, 0 errors, 0 skipped
```

The projection deliberately records `persistedMutationCoordinatorPresent=false`,
`closedWriterRuntimeIntegrationPresent=false`, `externalDeleteCompositionPresent=false`, and
`realOxiaExecutionPresent=false`. Pure candidate construction is not persisted dispatch authority and grants no
physical-delete, source-bound receipt, scenario-promotion, staging, or production authority.

## Remaining ordered work

1. Persist the ADR 0147 authority through exact same-key CAS, integrate all ten proof-bound writer classes, and compose
   the fenced identity read, intent/done recovery and external cleanup adapters above the pure foundation.
2. Five current-source evidence children, exact-source Final publication, 14-row promotion, and aggregate
   `v2M5Check`.

`V2-KAF-DATA-012`, `V2-KAF-DATA-013`, and `V2-KAF-DATA-022` remain M6-deferred. Tombstone deletion,
allocator-orphan GC, M6/M7/M8, and production deployment authority remain excluded.
