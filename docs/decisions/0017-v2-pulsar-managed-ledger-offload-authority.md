# ADR 0017: V2 Pulsar ManagedLedger offload authority

## Status

Accepted for Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`. Implementation and runtime evidence are not started at M0.

## Context

BookKeeper is the primary WAL for the Pulsar async-Object profile, so its ACK boundary must remain native ManagedLedger
and BookKeeper quorum. The later Object copy needs one crash-recoverable authority for attempt identity, completion,
offloaded reads and fallback, source-deletion eligibility, and cleanup. Giving a parallel Nereus manifest equal
lifecycle authority would create two state machines that can disagree about whether a ledger is still required.

The pinned Pulsar development source already records an offload attempt UUID before calling `LedgerOffloader`, records
completion after the offloader succeeds, opens offloaded reads from ManagedLedger ledger metadata, and consults the
offload context before deleting the BookKeeper source. That source observation is design input, not V2 runtime evidence.

## Decision

Native ManagedLedger ledger/offload metadata is the sole offload and lifecycle authority for Pulsar
`BOOKKEEPER_WAL_ASYNC_OBJECT`:

- ManagedLedger owns offload-attempt identity, completion state, retry/cleanup association, read selection and fallback,
  and BookKeeper source-deletion eligibility.
- Nereus supplies a custom `LedgerOffloader` that writes the accepted Nereus immutable Object format and returns success
  only after the offloaded data and its authoritative root are durable and readable.
- A Nereus manifest may be a rebuildable derived read/materialization index. It cannot mark a native offload complete,
  overrule ManagedLedger fallback, or authorize deletion of a BookKeeper ledger.
- A Nereus-owned lifecycle record may add observation, work scheduling, or source protection, but disagreement fails
  closed in favor of the native ManagedLedger authority.
- Offload remains asynchronous to append. A lag or capacity policy may throttle or stop later admission, but no accepted
  BookKeeper append waits synchronously for Object storage.

Completion of the offloader only makes the source eligible for the rest of the native retention/deletion protocol. It
does not bypass cursor/read pins, deletion lag, response-loss reconciliation, or other accepted source protections.

## Consequences

- `V2-OPEN-BK-01` is resolved.
- Pulsar and Kafka deliberately use different async-Object publication authorities: Kafka uses the Nereus typed
  manifest, while Pulsar uses ManagedLedger ledger/offload metadata.
- Nereus Object layout and execution mechanics must fit the `LedgerOffloader`/ManagedLedger lifecycle rather than
  replacing it with a generic cross-protocol state machine.
- M2 must prove attempt recovery, completion publication, offloaded read/fallback, retention, and BookKeeper deletion
  cuts against the exact pinned Pulsar source.

Execution timing is refined by [ADR 0020](0020-v2-pulsar-sealed-ledger-async-offload.md), and the sealed-ledger Object
pair is refined by [ADR 0024](0024-v2-pulsar-sealed-ledger-object-layout.md). This decision is tracked by `T-BK-01`,
`V2-BK-001`, `V2-BK-002`, and `V2-BK-004`.
