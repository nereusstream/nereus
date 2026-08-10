# ADR 0051: V2 Pulsar selector state machine and cached fence

## Status

Accepted for the 0.2 Pulsar metadata integration. Implementation and runtime evidence are not started at M0.

## Context

Pulsar's name-scoped selector and incarnation aggregate use separate MetadataStore/Oxia keys. Oxia 0.9.0 supplies
single-key conditional writes, not an all-conditions-or-no-writes transaction across both keys. Revalidating them from
Oxia on every append or read would avoid a local cache problem by putting remote metadata on the hot path.

## Decision

`PulsarTopicGenerationSelectorV1` uses only exact forward-CAS states
`RESERVED -> ACTIVE -> DELETING -> DELETED`.

- First creation is `putIfAbsent(RESERVED(1, aggregateKey, aggregateSha))`.
- Same-name recreation is `CAS(DELETED(g) -> RESERVED(g+1, ...))`; positive-generation overflow fails closed.
- The creator `putIfAbsent`s the exact immutable incarnation aggregate, then CASes the same reservation to ACTIVE.
- Deletion CASes ACTIVE to DELETING before native topic deletion and DELETING to DELETED only after that deletion cut.
- Every uncertain response rereads the exact key/value and resumes only the one legal successor step. RESERVED is a
  selector recovery state, not an externally usable aggregate `CREATING` state.
- Selector creates and CASes expose only ADR 0082's closed results. `EXACT` compares authority key, schema, digest, and
  canonical stored bytes; a partial-field match cannot activate or advance the selector.

Topic open and ownership acquisition use a backend-native opaque witness. A 128-bit CSPRNG `acquisitionId` is generated
before the first conditional acquire; all-zero and a process-observed duplicate are rejected. The same response-unknown
acquire retry and lease renewal inherit it. Reacquire after loss, transfer, forced takeover, or missing-record recreate
generate a fresh value. The guarantee is collision resistance, not mathematical never-reuse; a qualified backend
creation/session revision is also bound when available. A stable endpoint or resettable state counter is insufficient.

The control path performs authoritative witness A, exact ACTIVE/aggregate read, then authoritative witness B. An ELM
eventual TableView is not an authoritative A/B read. Exact `A == B` while still locally owned is necessary, but the
installer must also compare-and-set the expected ownership and selector invalidation/watcher sequences. An invalidation
that arrived before installation therefore cannot be overwritten by a stale installer. A backend unable to provide
this capability fails V2 admission; unsupported third-party backends need not implement it.

The watch/loss capability is armed before the exact read and must invalidate the same fence sequence on callback,
reconnect/session gap, and close. A generic best-effort watch without a registration barrier or gap invalidation is not
sufficient. If selector mutation and ownership transfer cannot be ordered with local invalidation, or a notification
gap cannot force global invalidation, the backend fails V2 admission rather than trusting a possibly valid old word.

The cached authority is one atomically comparable fence word, not tearable generation and valid fields. Normal
append/read admission captures that word and completion/ACK or response publication rechecks exact equality. It performs
no Oxia access, witness/SHA string parsing, or remote validation. Watch notifications atomically advance/invalidate but
never grant admission. Ownership loss invalidates before unload begins. Any cache miss, sequence/version change,
unknown acquisition, or restart removes admission until witness/read/witness validation succeeds again. Control reads
may be boundedly coalesced by service unit.

ADRs 0071/0073/0074 do not treat that cached fence, ownership change, current backend capability, or session-loss
observation as proof that old read pins have drained. Object-WAL source-protection release requires its separate
contiguous Read Admission Epoch proof and immutable historical capability-evidence contract.

ADR 0075's Binding-incarnation `BindingReadSelector` is a separate Object-WAL read-admission/source-view authority; it
does not reuse the Pulsar topic-generation selector, its cached ACTIVE fence, or a cross-key application reread.
ADRs 0077..0080 further keep that selector's `ADMITTING/STOPPED` state, small inline closure-anchor set/emergency
reserve, closed-verifier terminals, per-source retirement rows, and permanent compact batch tombstones distinct from
the topic-generation selector and aggregate retirement tombstone.

The state machine and validation cannot be disabled. Cache size, refresh scheduling, watch concurrency, and host memory
are performance policy at Cell/host scope and cannot manufacture or extend authority.

## Consequences

- `V2-OPEN-PUL-META-02` is resolved.
- Creation/deletion pays several low-frequency single-key operations and recovery states, while normal data access keeps
  zero remote metadata I/O.
- A watch/cache accelerator never becomes topic-incarnation or ABA authority.
- M1 proves selector creation/deletion, every acquisition transition, authoritative A/B reads, sequence-CAS stale-
  installer rejection, atomic fence capture/recheck, every response-loss cut, conflicting creator, stale watch/cache,
  ownership transfer/recreation, overflow, unsupported-backend refusal, local-only per-access checks, and fail-closed
  invalidation. M5 separately proves exact reference-free full-aggregate-to-tombstone replacement.

This decision is refined by ADRs 0071/0073..0080 and 0082, refines ADRs 0019, 0028, 0043, and 0049, and is tracked by
`T-META-01`, `T-POLICY-01`, `V2-META-005..007`.
