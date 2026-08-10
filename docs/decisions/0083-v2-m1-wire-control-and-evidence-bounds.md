# ADR 0083: V2 M1 wire, control-plane, and evidence bounds

## Status

Accepted for the 0.2 M1 implementation. The complete NTA1 FrameEncodingPolicy/legality/cap table, Pulsar UTF-8 and
total-payload caps, writer count, receipt/attachment numeric caps, and
executable current-source evidence remain implementation-readiness descendants. ADR 0084 closes the protocol/leaf,
Kafka precedence, minimal continuity, native hash, and receipt-accounting descendants while keeping the complete NTA1
table, provider API/source tuple, Registry writer caps, and receipt numeric caps OPEN. ADR 0085 closes the client-only
continuity direction and exact 120-byte writer row, removes receipt run identity, and permits M1.1a foundation work
without claiming the deferred codecs or validators.

## Context

ADR 0082 freezes the outer domain and control authorities but intentionally leaves several choices open. An exact
aggregate claim without a complete field and cap table would be false precision. Persisting a second random ledger-ID
namespace marker or build SHA in the Registry would make ordinary rebuilds control-plane migrations without proving
the process actually runs those bytes. Reusing a Pulsar acquisition across process restart would weaken the intended
ABA fence. Repeatedly serializing every previously accepted Kafka topic while sizing a large CreateTopics request
would turn a control-plane guard into quadratic work.

M1 therefore freezes the useful structural cuts now, while keeping unproved numeric wire limits explicitly open.

## Decision

### Canonical Cell and incarnation bytes

The stable `cellBytes` sub-encoding used by NTB1 is:

```text
Kafka:
NPC1 || u16be(KAFKA)
     || deploymentId[16]
     || kafkaCellId[16]

Pulsar:
NPC1 || u16be(PULSAR)
     || deploymentId[16]
     || reservationDomainId[16]
     || pulsarCellId[16]
```

`reservationDomainId` remains part of Pulsar Cell identity because it owns the fixed virtual-ledger slice. The
BookKeeper compatibility-namespace identity, provider scope, broker endpoint/alias, backend/session version, and other
rotatable configuration are excluded. Protocol code zero is illegal; exact non-zero numeric codes are frozen with the
remaining closed discriminator table before codec implementation.

The stable `incarnationBytes` shape used by NTB1 is:

```text
Kafka:
NTI1 || u16be(KAFKA)
     || topicId[16]
     || u32be(canonicalTopicNameLength)
     || canonicalTopicNameAscii

Pulsar:
NTI1 || u16be(PULSAR)
     || u32be(canonicalPersistenceNameLength)
     || canonicalPersistenceNameUtf8
     || u32be(canonicalTopicNameLength)
     || canonicalTopicNameUtf8
     || u64be(bindingGeneration)
```

Kafka accepts at most 249 bytes from its pinned ASCII topic-name alphabet and rejects the zero UUID plus the exact
reserved-UUID set frozen before implementation. Pulsar generation is `1..Long.MAX_VALUE`, encoded as unsigned
big-endian bits in the `u64be` field; zero or increment overflow fails closed. Pulsar's pinned source supplies no
equivalent native name-length maximum, so its per-name and total UTF-8 caps remain V2 format decisions that must be
fixed before a parser exists. The NTA1, NPC1, and NTI1 protocol discriminators must agree exactly. These encodings and
hashes execute only at create/replay.

### NTA1 structural boundary without invented numeric caps

NTA1 v1 is one flat, canonical, sequential aggregate encoding. It has no TLV, map, self-digest, unknown-field bag, or
extension tail. `cellBytes` and `incarnationBytes` remain explicitly `u32be` length-framed. The initial sealed-end
presence byte is exactly `0x00`; `0x01` is illegal in v1 rather than a hidden evolution channel. A future extension
requires NTA2 and a new Kafka wire version.

This decision does not call the payload “exact NTA1” yet. Before codec implementation, the remaining field widths,
numeric enum codes, legal `NONE` combinations, discriminated variant payloads, strict Pulsar UTF-8 byte caps, total
payload cap, and checked-arithmetic derivation must be frozen together. Deployment may only lower new-write admission;
it cannot lower or reinterpret the persisted-v1 decoder cap.

### Kafka input-only profile and classifier

M1 exposes exactly one Nereus CreateTopics pseudo-config:

```text
nereus.storage.profile = OBJECT_WAL
                       | BOOKKEEPER_WAL_ONLY
                       | BOOKKEEPER_WAL_ASYNC_OBJECT
```

The key and values are case-sensitive and are not trimmed. Null, empty, or an unknown value for that exact key is
`INVALID_CONFIG`. Unknown `nereus.*` keys are not intercepted and continue through stock config validation.

User topics resolve from one versioned Deployment default unless that exact input is supplied. M1 infers no Kafka
Namespace from the topic name. Classifier v1 treats only the pinned source's three built-ins
`__consumer_offsets`, `__transaction_state`, and `__share_group_state` as internal TopicImage topics. The KRaft
`__cluster_metadata` log is not a TopicImage topic; Streams, Connect, MM2, `__remote_log_metadata`, and other
application/Admin-created topics follow the user-topic path. Classifier membership does not exempt stock tiered-
storage bootstrap from native replication/minISR admission: M6 must disable that subsystem or prove compatible explicit
settings before it is admitted.

Built-ins reject an explicit profile and use only the versioned internal-topic Deployment policy. Every code path that
can create a TopicImage topic must invoke the same pure resolution, aggregate construction, validation, and admission
kernel; public CreateTopics is not a privileged completeness boundary. Only the exact pseudo-key above is intercepted.
Unknown `nereus.*` names remain inputs to stock config validation and retain stock error precedence.

Incremental and legacy AlterConfigs reject SET, DELETE, APPEND, SUBTRACT, and same-value SET for the pseudo-key.
DescribeConfigs reports explicit input as `DYNAMIC_TOPIC_CONFIG` and inherited input as `DEFAULT_CONFIG`, but both are
aggregate-derived, `readOnly=true`, and have no synonyms. A resolved inherited value is frozen create history and does
not track a later Deployment default.

### Kafka residue-free linear batch admission

Kafka retains the stock request-wide 10,000-partition guard and `POLICY_VIOLATION`. Independently, aggregate-expanded
record-count or encoded-batch-byte overflow rejects that topic with per-topic `POLICY_VIOLATION`.

Admission processes request items in request order. It may reject a candidate B that no longer fits and still accept a
later smaller C. It neither sorts nor backtracks topics; “request order” is the active controller's received
`CreatableTopicCollection` insertion order, so callers with unordered inputs do not gain a stronger winner-order
guarantee. All accepted topics nevertheless publish as one atomic controller batch. Each item is first an externally
side-effect-free `TopicCreateCandidate`; only an admitted candidate commits quota, success-map state, topic ID, and its
records. Candidate construction may generate the UUID needed by the complete aggregate and exact sizing, but rejection
never publishes or retains that UUID. A rejected candidate leaves none of those residues. `ConfigRecord` records sort
by config name and partition records by partition ID before sizing and publication; companion native configuration-
derived records retain their required semantic order. ADR 0084 refines duplicate pseudo-configs to one linear
insertion-order last-wins collapse and forbids a first-match parser.

The complete candidate contains:

```text
TopicRecord
+ TopicBindingAggregateRecord
+ native configuration-derived records*
  (currently ConfigRecord plus applicable ClearElrRecord)
+ PartitionRecord*
+ record/batch serialization overhead
```

The fork extracts a pure incremental sizer from the exact `BatchBuilder` record-size logic, using
`MetadataRecordSerde`, one shared serialization cache, and the same effective controller record/byte limits that the
final Raft append will enforce, including test-injected limits. The existing fit-oriented builder method is not treated
as a no-allocation estimator. Each generated record is sized at most once, and accepted counts/bytes accumulate with
checked arithmetic. Implementations may not reserialize the whole accepted prefix after each topic or allocate a
maximum-sized batch buffer merely to measure it. The work is `O(total generated records)` for the request. Raft's final
guard remains defense in depth, not the first ordinary oversized-request detector. If the accumulator's current batch
cannot fit a candidate, it must re-estimate the candidate as the first records of a fresh batch, with offset-delta state
reset, before declaring it too large. The current-batch delta encoding may not turn an otherwise valid fresh-batch
candidate into a late `RecordBatchTooLargeException`.

Live creation origins converge on this same candidate/admission kernel. Replay and snapshot do not rerun policy
resolution; they validate the persisted TopicRecord/Aggregate pairing and aggregate semantics before publication.

### First Pulsar ownership-witness adapter candidate

The first M1 witness candidate is limited to the Oxia 0.9.0-backed MetadataStore ELM. The pinned source verifies useful
direct GET, Stat, and versioned-CAS primitives; it does **not** already implement a qualifying witness adapter. M1 must
add dedicated authoritative reads, a provider-qualified lifecycle/gap hook, acquisition fields and transitions, and
one closed transition kernel that every ownership writer uses. The current eventual TableView, force bypass,
unconditional syncer write, and conflict-swallowing wrapper do not qualify and cannot be described as that kernel.

Initial admission requires MetadataStore ELM, syncer disabled, every ownership writer upgraded, and the qualified
lifecycle/gap hook proven. Legacy locks, system-topic TableView, mixed writers, generic best-effort notifications, or a
third-party backend without the whole capability fail V2 admission. This adds no sidecar ownership authority.

The same response-unknown acquire retry and renewal within the same process and backend session reuse an acquisition.
A brief `ConnectionLost -> Reconnected` in that same session may retain it only under the admitted backend contract.
`SessionLost` or process restart creates a new broker incarnation. Every real service-unit reacquisition after either
event, plus transfer target, forced takeover, missing/tombstone recreation, and split-child acquisition, creates a new
acquisition ID bound to the target broker's current incarnation. Restart never inherits the old acquisition merely by
reread. A qualified same-session reconnect may retain identities but immediately invalidates the local word and repeats
A/read/B before reinstalling it.

Ownership, selector, and aggregate invalidation all advance one local INVALID sequence. Installation is a CAS from the
exact `INVALID(seq)` to `VALID(seq)` word, so any earlier callback makes a stale installer fail. INVALID/VALID is not a
durable metadata state. Normal admission captures one atomic word and rechecks exact equality before ACK/response
publication; it does not parse the witness or access remote metadata. Control validation may coalesce a bounded service-
unit batch.

### BookKeeper-native compatibility identity and inline writer membership

For 0.2, `ledgerIdCompatibilityNamespaceId` is a 32-byte domain-separated SHA-256 derivation of the exact BookKeeper
ledger root's native `INSTANCEID`. A format operation can replace `INSTANCEID`, so the derived identity changes, but
that fact alone does not prove that the ledger-ID namespace is empty: the pinned Pulsar factory's format path is not a
qualified full-root cleanup. No independently surviving random Nereus marker is introduced. If a later implementation
needs a marker, it must bind the exact INSTANCEID and become permanently invalid after format/nuke; it cannot name a
second namespace. The hash excludes ledger-root URI/path, deployment/reservation-domain identity, and source/artifact
SHA; a copied INSTANCEID therefore maps conservatively to the same compatibility namespace. The exact hash separator/
framing and INSTANCEID byte validation remain to be frozen before implementation.

0.2 admits only a genuinely fresh ledger root that is authoritatively absent immediately before init: either never
created or removed by a qualified non-force nuke bound to the expected old INSTANCEID, after fencing every old writer/
admin principal and proving the absent-root postcondition. `INSTANCEID` absence/recreation, format success, force nuke,
or an unqualified direct nuke is not freshness evidence. Existing-root migration remains outside 0.2. Registry absence
means inactive and there is no persisted `INACTIVE` state. The Registry stores one bounded inline canonical writer set,
not an external membership snapshot. Each row contains only:

```text
writerKind
writerEntryId
allocator/exclusion contract version
independently revocable principal generation/digest
interlock policy generation/digest
typed conformance-evidence reference
```

Source commit and artifact SHA belong to the conformance receipt, not long-lived Registry identity. Old and new writers
must have independently revocable principals; a shared credential is insufficient. Writer count and row bytes have
separate format hard caps in addition to the 64-KiB aggregate cap; their exact numbers remain open.

First activation establishes exclusive ACL/admin interlock, proves/initializes the fresh root, rereads the exact
INSTANCEID, upgrades every possible writer to exclusion/interlock support, revokes the old unrestricted principal,
proves negative allocation through it, and only then activates the Registry with its final create/CAS. After activation,
format/nuke/INSTANCEID/root mutation is forbidden; missing or changed identity invalidates Registry admission and every
derived view. Later addition commits a writer before start; removal fences/drains and revokes it before membership
removal. Allocators keep using a versioned derived slice view and do not read/copy the Registry for each rollover.

### Canonical M1 virtual-ledger evidence envelope

M1 virtual-ledger conformance evidence uses one canonical JSON envelope whose payload discriminator is the closed union
`REGISTRY_CONFORMANCE | HARNESS_CONFORMANCE_ONLY`; it does not define two serialization frameworks. Canonical bytes use
RFC 8785/JCS plus a closed schema that rejects duplicate/unknown fields, floating-point numbers, BOM, and any non-
canonical encoding. Numeric size/count limits remain to be fixed before the receipt validator is implemented.

`HARNESS_CONFORMANCE_ONLY.selectionEligible=false` is fixed by its schema/kind and cannot be supplied as a free
boolean. Suite- and scenario-bound test results contain at least
`discovered/executed/passed/failed/skipped/aborted`; the validator freezes their accounting equations, retry and
parameterized-test treatment before promotion. The source tuple includes the Oxia server image digest and client/test
artifact identities in addition to the product/fork/source-lock/domain identities required by ADR 0081.

Registry raw bytes, canonical writer set, ACL/interlock snapshots, and logs are content-addressed attachments. The root
receipt stores only canonical relative path, length, and SHA-256 rather than copied base64 payloads. Hashes prove byte
identity, not trust: trusted promotion workflow plus the protected N3 evidence commit supply provenance, and M1 adds no
receipt self-signature. N3 may change only receipts, attachments, and their exactly covered scenario status/index; it
may not change code, gates, or source locks. The general M1 final evidence index may reference this closed virtual-
ledger union and other gate outputs; it cannot reinterpret one kind as another.

## Consequences

- Identity, config resolution, NTA1 validation, Kafka sizing, Registry mutation, and evidence encoding remain create,
  replay, bootstrap, or promotion work rather than append/read work.
- Kafka pays one linear sizing pass for large CreateTopics requests and avoids both late Raft failure and quadratic
  prefix serialization.
- Pulsar restart pays a new witness acquisition; stable admitted access still performs only local atomic capture and
  recheck.
- An INSTANCEID change invalidates the old Registry binding and every derived view, but does not prove cleanup or fence
  all clients; the fresh-root admission proof supplies that safety without a second random identity.
- Rebuild SHAs remain evidence facts rather than Registry migrations; completeness still depends on independently
  revocable principals, writer membership, and the allocation interlock.
- Complete NTA1 policy/legality/caps, Registry writer count, and receipt numeric caps remain OPEN and must not be
  inferred from this ADR.

This decision is refined by ADRs 0084 and 0085 and refines ADRs 0028, 0032, 0033, 0034, 0041, 0042, 0050, 0051, 0054, 0081, and 0082. It is tracked by
`V2-META-003..006`, `V2-KAF-META-001..004`, `V2-POSITION-003..010`, and the M1 gates.
The complete confirmed answer is preserved in
[M1 Readiness Grill round 3](../v2/grill-notes/24-m1-readiness-round-3-wire-control-and-evidence.md).
