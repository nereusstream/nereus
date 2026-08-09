# ADR 0044: V2 Pulsar NPD1 sealed-ledger data blocks

## Status

Accepted for 0.2 Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0035 freezes the bounded NPO1 root, but a sparse row still needs one independently verifiable unit inside the data
Object. Stock-style padded blocks require scan-forward decoding and do not bind a per-block digest, independent
compression/AEAD state, or a bounded entry directory. One Object range per entry would preserve random access while
discarding useful compression and request amortization.

## Decision

The data Object uses a distinct `NPD1` major format composed of ordered, gap-free, independently decodable multi-entry
blocks.

Each NPO1 `SPARSE_INDEX` row binds exactly one NPD1 block ordinal, one contiguous inclusive entry-ID range, the block's
byte offset and encoded length, decoded length, codec family/version, encryption family/version, and SHA-256/v1 of the
exact encoded block bytes.

Each block contains one bounded canonical entry directory followed by the exact ManagedLedger entry bytes. ADR 0056
fixes a 16-byte row containing decoded offset, payload length, and flags; entry ID is derived from the authenticated
NPO1 `firstEntryId` plus row ordinal rather than repeated per row. Entry IDs are ordered and contiguous within the
block, and the directory plus NPO1 facts must prove ordered gap-free coverage across all blocks. One entry never
crosses a block. An entry larger than the target block size uses one dedicated oversize block and is rejected before
upload if it exceeds the hard format maximum.

Compression, AEAD, and integrity state reset at every block. NPD1 has no padding, cross-block compression state, or
cross-block AEAD stream. A reader validates NPO1 range/digest facts and the bounded block directory before returning
entry bytes; it does not scan an unbounded predecessor block or require a whole-data-Object GET.

## Consequences

- `V2-OPEN-BK-09` is resolved.
- A single-entry read pays one block range GET and decode, while multi-entry blocks preserve compression and request
  efficiency.
- Independent block verification keeps corruption/fallback decisions local without weakening the final whole-data
  Object proof.
- ADR 0056 freezes the 32-byte NPD1/64-byte NPB1 headers, checked length domains, derived-ID row, streaming processing,
  and complete provider-capability categories. Exact remaining field IDs, numeric block/Object/part limits, codec
  thresholds, and golden vectors remain downstream gates.
- M2 must prove contiguous coverage, sparse-row/block substitution, directory bounds and ordering, oversize handling,
  independent decode, no cross-block state, and exact ManagedLedger entry fidelity.

This decision is refined by ADRs 0056/0057, refines ADRs 0024, 0026, 0035, and 0036, and is tracked by `T-BK-01`,
`T-PROTOCOL-01`, `T-POLICY-01`, and `V2-BK-006/009/012/013`.
