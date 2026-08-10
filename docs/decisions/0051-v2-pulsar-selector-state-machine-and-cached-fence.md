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

Topic open and ownership acquisition use a backend-native opaque ownership witness containing a non-reusable ownership-
acquisition identity; a stable broker endpoint or a state counter that may reset after deletion/recreation is not
sufficient. The control path captures witness A, reads and validates ACTIVE plus the exact generation, incarnation,
aggregate key/SHA and metadata versions, then captures witness B. Only exact `A == B` while still locally owned installs
the cache. A backend unable to provide that ABA-safe witness fails V2 admission.

The cached fence stores primitive ownership/cache generation and valid state plus the validated selector/aggregate
identity. Normal append/read performs only local primitive/volatile checks; it performs no Oxia access, token/SHA string
parsing, or remote validation. Watch notifications can invalidate but never grant admission. Ownership loss invalidates
the fence before unload begins. A watch change, cache miss, stale version, ownership change, unknown acquisition, or
restart removes admission until the control path repeats the witness/read/witness validation.

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
- M1 proves selector creation/deletion, ownership-witness capture, cache install/invalidate, every response-loss cut,
  conflicting creator, stale watch/cache, ownership transfer, recreation, overflow, local-only per-access checks, and
  fail-closed invalidation. M5 separately proves exact reference-free full-aggregate-to-tombstone replacement.

This decision is refined by ADRs 0071/0073..0080, refines ADRs 0019, 0028, 0043, and 0049 and is tracked by `T-META-01`,
`T-POLICY-01`, `V2-META-005..007`.
