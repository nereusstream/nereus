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

Topic open, ownership acquisition, and any observed selector/aggregate metadata-version change validate ACTIVE plus the
exact generation, incarnation, aggregate key, and aggregate SHA. The result is cached as a local versioned fence.
Normal admitted append/read checks that local fence and the current owner/metadata version; it performs no Oxia access
per operation. A watch change, cache miss, stale version, ownership change, or restart removes admission until the
control path revalidates exact authority.

ADRs 0071/0073/0074 do not treat that cached fence, ownership change, current backend capability, or session-loss
observation as proof that old read pins have drained. Object-WAL source-protection release requires its separate
contiguous Read Admission Epoch proof and immutable historical capability-evidence contract.

The state machine and validation cannot be disabled. Cache size, refresh scheduling, watch concurrency, and host memory
are performance policy at Cell/host scope and cannot manufacture or extend authority.

## Consequences

- `V2-OPEN-PUL-META-02` is resolved.
- Creation/deletion pays several low-frequency single-key operations and recovery states, while normal data access keeps
  zero remote metadata I/O.
- A watch/cache accelerator never becomes topic-incarnation or ABA authority.
- M1/M5 must prove every crash cut, lost response, conflicting creator, stale watch/cache, ownership transfer,
  recreation, overflow, no per-access Oxia calls, and fail-closed invalidation.

This decision is refined by ADRs 0071/0073/0074, refines ADRs 0019, 0028, 0043, and 0049 and is tracked by `T-META-01`,
`T-POLICY-01`, `V2-META-005/006`.
