---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M5-I0 implementation-input closure

## Purpose and authority

This document closes the inputs needed to implement M5 without reopening the accepted M1-M4 contracts. It defines
where authority lives, which modules own future code, the only admitted lifecycle order, and the closed vocabulary
used by M5-A through M5-E. It is not runtime implementation or evidence.

Authority order is:

1. accepted V2 normative contracts and ADRs;
2. immutable M2, M3, and M4 Finals at their own tested sources;
3. this M5 detailed-design freeze;
4. future exact-source M5 code, tests, child receipts, and aggregate Final; then
5. operational configuration within evidence-selected numeric bounds.

A lower level cannot reinterpret a higher level. In particular, code existence, a local test, a controller command,
or an Object-store absence cannot substitute for an accepted state transition.

## Frozen inputs

| Input | Required identity or contract | M5 use |
| --- | --- | --- |
| M2 global Final | `docs/v2/evidence/v2-m2/final/m2-final.json`, SHA-256 `2ba2d1cab0547c456ec7e492edaf9b953e9e0d71707770d3c4b4fe8a4d6217dd`, tested Nereus `4af3278234d84df7a2fdce4fc6b3e4e227916d56` | NBKE2, Kafka recovery state, Pulsar NPD1/NPO1 and existing BookKeeper delete state machine |
| M3 Final | `docs/v2/evidence/v2-m3/final/e5e53e62865c21845621037bea5f18c092bd4259/m3-final.json`, SHA-256 `81c7004a923e5b96cab0a3c8b4f1fa26d71606a2208bbabe779f0d872f84f84a` | NWG1, provider/session, Object-WAL run/checkpoint, active-tail and allocator authority |
| M4 Final | `docs/v2/evidence/v2-m4/final/final-source-595c8b34779d1e88187eb0084bf18e65ab2dd742/m4-final.json`, SHA-256 `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07` | coherent read generation, source plans, pins, proof intervals, batch identity and exact protection `RELEASED` |
| Source locks at M4 | SHA-256 `02601b3de76857d5f0c8657b285bc91584486d08077e201a4d9fd34f377b07a2` | historical dependency identity only; future M5 evidence must bind its own current source locks |

M5 implementation must rerun all affected M2/M3/M4 predicates at one M5 tested source. These historical Finals are
dependencies, not permission to reuse stale runtime results as current-source M5 evidence.

## Existing accepted decisions consumed without amendment

- ADRs 0012/0014/0049: Storage Epoch, Protocol Cell, Provider Scope, and configuration ownership.
- ADRs 0017/0020/0024/0029/0035/0036/0052: Pulsar offload pair, dual-source reads, retention class, and
  `BK_DELETE_NONE -> BK_DELETE_INTENT -> BK_DELETE_DONE`.
- ADRs 0018/0025/0030/0038/0040/0053/0057/0062/0063/0066: immutable Object identity, provider uncertainty,
  namespace, checkpoint, policy, and recovery contracts.
- ADRs 0043/0046/0051: exact Topic Binding Aggregate reference-free retirement and permanent incarnation tombstone.
- ADRs 0069 through 0080: M4 selector, intervals, pins, proof, source protection, immutable retirement batch, and
  `FULL_V1 -> RETIRED_V1` permanent compact tombstone.
- ADR 0087: Kafka frontiers, indexes, recovery, gap seek, and distinction between materialization and semantic
  compaction.

The detailed design selects implementation composition under those decisions. It does not create a second release,
retention, manifest, or deletion authority.

## Module ownership

No new Gradle module is added for M5. Future implementation uses the current module graph:

| Module | M5 responsibility |
| --- | --- |
| `nereus-domain` | protocol-neutral immutable identities and value validation only when they are not Object-provider-specific |
| `nereus-storage-api` | narrow protocol-neutral materialization/delete capability interfaces; no Provider product code |
| `nereus-storage-object` | task/source-cut model, generation/manifest descriptors, M4-batch migration, retention/reference proof, Object deletion coordinator, admission accounting |
| `nereus-storage-object-s3` | S3/compatible provider delete transport, exact-version or fenced-key capability evidence, multipart reconciliation |
| `nereus-storage-bookkeeper` | generic sealed-ledger identity/delete adapter where not owned by a protocol |
| `nereus-kafka-bookkeeper` | Kafka materialization reader, Kafka compactor, index rebuild/validation, producer/transaction/leader-epoch semantics |
| `nereus-pulsar-offload` | NPD1/NPO1 reuse, native ManagedLedger reference inventory, root/data/multipart cleanup, existing BK delete coordinator composition |
| `nereus-metadata-spi` | typed conditional metadata operations only if the existing capability vocabulary cannot express the required transaction |
| `nereus-metadata-oxia` | exact-version CAS/transaction implementation, authoritative scans, batch externalization, tombstone and delete-intent persistence |

`settings.gradle.kts` remains unchanged. M5 does not resurrect the removed V1 materialization module and does not
place protocol semantics in the shared Object provider adapter.

## Closed lifecycle

One ordinary source may advance only through this dependency chain:

```text
FROZEN_SOURCE_CUT
  -> OUTPUT_CREATED_OR_REUSED
  -> OUTPUT_VERIFIED
  -> GENERATION_PUBLISHED_WITH_SOURCE_FALLBACK
  -> M4_FALLBACK_REMOVED
  -> exact protection-generation RELEASED
  -> RETENTION_AND_REFERENCE_FREE_VERIFIED
  -> SOURCE_RETIREMENT_BATCH_FULL_METADATA_RETIRED
  -> DELETE_INTENT
  -> AUTHORITATIVE_ABSENCE
  -> DELETE_DONE
```

Compaction inserts `KAFKA_SEMANTICALLY_VALIDATED` between output creation and publication. Logical trim may advance
only after its own retention snapshot; it does not skip any later state. Pulsar incarnation metadata uses a stricter
tail: the full `TopicBindingAggregateRecord` remains until every physical source is `DELETE_DONE`, then exact
reference-free proof permits the final same-key `RetiredTopicIncarnationTombstone` replacement. A failure, unknown value, unsupported
capability, fence change, missing record, invalid digest, or budget exhaustion produces `RETAIN` or `QUARANTINE`,
never an inferred successor state.

## Persistent identity domains

Every new M5 persistent value binds the following common envelope, either inline or by an exact immutable reference:

- schema and format version;
- Protocol Cell and Cell Provider Scope;
- Topic Protocol Binding, Topic Incarnation, and Storage Epoch;
- typed Position Domain and exact inclusive/exclusive Protocol Coverage;
- Owner Epoch, worker epoch, and storage/source generation where applicable;
- deterministic task, source-set, policy, and output identities;
- canonical body length and SHA-256 for every Object/control value;
- predecessor key/version/value SHA and successor key/value SHA for every conditional transition; and
- the closed capability generation/digest authorizing the backend operation.

Identifiers are domain-separated SHA-256 of canonical bytes. They contain no random value, wall-clock time, host
path, process ID, retry counter, or provider response text. Time may appear only as a captured policy fact or grace
deadline in a value whose identity and authority are explicitly defined.

## M5 state vocabulary

| Domain | Closed states | Terminal interpretation |
| --- | --- | --- |
| materialization task | `PLANNED`, `OUTPUT_VERIFIED`, `PUBLISHED`, `CANCELLED_STALE`, `QUARANTINED` | only `PUBLISHED` names a selected generation; cancelled/quarantined output is orphan-candidate input, not deletable by itself |
| generation selection | `PREFERRED_WITH_FALLBACK`, `PREFERRED_ONLY` | transition to preferred-only is M4-owned; M5 never manufactures it |
| logical trim | monotonic typed `TrimFrontier` generations | hides only positions below the exact frontier and never proves physical absence |
| Object-WAL batch metadata | `FULL_V1`, `RETIRED_V1` | exact same-key irreversible CAS; retired is permanent metadata-only fact |
| Pulsar incarnation metadata | full aggregate, `RetiredTopicIncarnationTombstone` | exact same-key irreversible replacement; tombstone is permanent metadata-only fact |
| Object deletion | `DELETE_NONE`, `DELETE_INTENT`, `DELETE_DONE` | done requires authoritative absence of the exact identity and residues |
| Pulsar BookKeeper deletion | `BK_DELETE_NONE`, `BK_DELETE_INTENT`, `BK_DELETE_DONE` | done requires physical delete or authoritative no-such-ledger; compatibility boolean is not proof |
| reconciliation outcome | `APPLIED_EXACT`, `EXISTING_EXACT`, `DEFINITIVELY_NOT_APPLIED`, `OUTCOME_UNKNOWN`, `CONFLICT` | only the first two advance; unknown retains; conflict quarantines |

No `FORCE`, `SKIP`, `ASSUME_ABSENT`, `DELETE_ANY_VERSION`, mutable released-count, or age-authorized terminal exists.

## Ownership and fencing

- The binding/manifest owner creates source cuts and publishes generations under the current Owner Epoch.
- A worker may build bytes but never owns manifest, retention, retirement, or deletion authority.
- The retention resolver reads protocol-owned roots and publishes an immutable snapshot/proof; it does not delete.
- The retirement coordinator owns exact metadata transitions after verifying every closed reference class.
- The GC coordinator owns intent and dispatch only within one Cell Provider Scope and exact capability generation.
- Provider transports may be pooled; sessions, credentials, namespace, queues, caches, intents, and budgets are Cell
  scoped.
- Every retry rereads the current owner/worker/storage fence. A newer fence makes an old attempt stale even if its
  bytes are deterministic and otherwise valid.

## Configuration and evidence-selected values

Correctness state machines, reference classes, identity fields, validation predicates, and ordering are fixed and
non-configurable. Later evidence may select finite positive values for task bytes/count, compaction backlog, source
age, tombstone bytes/count, scan page/count/bytes, grace, retry concurrency, provider/KMS/metadata concurrency, cache,
and per-Cell I/O/request budgets. Selection must persist at the exact task/Cell/capability generation and use the
minimum of product, Cell, host, and provider bounds. Missing or invalid selection stops admission.

## Required implementation slices

1. M5-A adds canonical projections/codecs, task/source cut, generation descriptors, validators, and fenced manifest
   publication, with no compaction or deletion.
2. M5-B adds Kafka rewrite planning, native decoder compatibility, semantic validator, and all rebuilt indexes.
3. M5-C adds typed retention snapshots, reference-free proof, batch externalization/retirement, Pulsar aggregate
   retirement, permanent tombstones, and admission/alerts.
4. M5-D adds provider delete capabilities, intent/reconciliation, Pulsar root/data and BookKeeper composition,
   physical-orphan cleanup, and Cell-isolation enforcement.
5. M5-E evidence tooling is added only after A-D code exists and never turns a focused test into aggregate authority.

No blocking design question remains. Exact byte offsets, parser hard caps, finite budget values, and performance
thresholds are implementation/evidence-selected within the schemas and invariants frozen here; changing the schema,
field meaning, lifecycle, or authority requires a design amendment.
