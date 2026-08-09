# ADR 0024: V2 Pulsar sealed-ledger Object layout

## Status

Accepted for 0.2 Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`. Implementation and runtime evidence are not started at M0.

## Context

ADRs 0017 and 0020 make native ManagedLedger metadata authoritative and define one sealed, non-current ledger per
offload attempt. The native `(ledgerId, UUID)` lifecycle does not inventory arbitrary child extents. Mapping one attempt
to multiple independently addressable data objects would therefore require another durable partial-attempt inventory
for cleanup before a root exists and after a partially successful delete.

## Decision

One 0.2 sealed-ledger attempt maps to exactly two deterministic, attempt-scoped provider objects:

1. one bounded immutable data `ObjectExtent` containing the ledger entries;
2. one immutable sparse-index/root object describing the complete ledger view over that data object.

Within the Cell Provider Scope namespace, both object identities are deterministically derived from the native ledger
ID and attempt UUID. Multipart upload may construct the data object, but successful completion still yields exactly one
data object. Ledger byte, entry-count, and age/rollover limits bound its size.

The data object is durably published and verified before the root. The `LedgerOffloader.offload(...)` future succeeds
only after both objects, their integrity descriptors, contiguous entry coverage from `0..LAC`, and the resulting
ledger-equivalent `ReadHandle` are verified. ManagedLedger may then publish native completion.

`readOffloaded` presents the pair as one complete sealed ledger. `deleteOffloaded` derives both keys without relying on
the root being present and is idempotent for a successful, failed, or response-uncertain attempt. All multipart upload
state and committed objects remain UUID-scoped so cleanup of an old attempt cannot affect a newer attempt.

0.2 does not publish multiple independent data extents for one sealed ledger. Such a layout would require a new durable
attempt-inventory and partial-delete contract.

## Consequences

- `V2-OPEN-BK-04` is resolved.
- 0.2 gives up independent data-extent parallelism and relies on rollover to bound one data object.
- The layout stays close to native Pulsar's whole-ledger attempt/read/delete boundary and keeps failed-attempt cleanup
  discoverable through two deterministic keys.
- M2 must prove data-before-root publication, root/coverage corruption rejection, multipart response-loss cleanup,
  whole-ledger `ReadHandle` equivalence, idempotent two-key deletion, and UUID isolation.

The deterministic keys, root contract, and lifecycle order are refined by
[ADR 0029](0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md); NPO1 and native dual-source safety are refined by
[ADRs 0035](0035-v2-pulsar-npo1-sealed-ledger-root-format.md) and
[0036](0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md); NPD1 and composite source pins by
[ADRs 0044](0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md) and
[0045](0045-v2-pulsar-dual-source-read-handle-and-pins.md), and BookKeeper-source deletion facts by
[ADR 0052](0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md). Checked NPD1 streaming/capability and policy
evidence are refined by [ADRs 0056](0056-v2-npd1-checked-envelope-and-derived-entry-row.md) and
[0057](0057-v2-npd1-policy-default-authority-and-evidence.md). This decision refines ADRs 0017/0020 and is tracked by
`T-BK-01`, `V2-BK-002`, and `V2-BK-004..013`.
