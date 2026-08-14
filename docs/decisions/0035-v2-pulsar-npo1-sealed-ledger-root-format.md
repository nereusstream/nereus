# ADR 0035: V2 Pulsar NPO1 sealed-ledger root format

## Status

Accepted for 0.2 Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0029 requires a bounded canonical root but does not freeze its major format or parser attack surface. The stock
offload index does not bind the V2 attempt, data digest, complete sealed-ledger facts, and root self-integrity, and it
does not provide the hard count/length limits required before trusting untrusted Object bytes.

## Decision

Root v1 is a new big-endian canonical binary format with magic `NPO1`; it does not extend or relabel the stock Pulsar
offload index. The fixed 32-byte header carries magic, format version 1, minimum-reader version 1, required flags, header
length, section count exactly four, total root length, and SHA-256/v1 root-self-digest family facts.

Exactly four sections follow in this order:

1. `ATTEMPT`;
2. `SEALED_LEDGER`;
3. `DATA_EXTENT`;
4. `SPARSE_INDEX`.

The v1 `ATTEMPT` section persists the attempt UUID and ledger, provider scope, deterministic Object-key derivation
version, retention class, resolved block target, and one wrapped attempt-key envelope. The envelope is not a plaintext
key: it carries format version 1, a canonical provider ID, exact wrapping-key ID and version, canonical wrapping
algorithm ID, and opaque wrapped bytes. Wrapping-key ID is capped at 4 KiB, wrapping-key version at 1 KiB, and wrapped
bytes at 16 KiB. Empty, oversized, malformed, or unsupported envelopes fail closed. A key manager creates one random
AES-256 attempt key plus its envelope before NPD1 generation; native restart reads validate NPO1 first and then unwrap
that exact persisted envelope. Current driver configuration cannot substitute a new wrapping key or reconstruct the
attempt key from host-local defaults, and plaintext attempt-key bytes are never stored in driver metadata or NPO1.

Every section begins with one 16-byte typed/versioned/required/length header. Unknown required section kind/version,
duplicate or reordered sections, unknown flags or enum values, inconsistent lengths, integer overflow, or trailing bytes
fail closed. Strings use strict UTF-8 with explicit length prefixes. Maps use unique keys sorted by unsigned UTF-8 key
bytes; duplicates and non-canonical ordering fail closed. A final 32-byte SHA-256 covers every preceding canonical root
byte.

The hard v1 decode limits are:

- complete root: 8 MiB;
- ledger metadata: 1 MiB;
- data descriptor: 256 KiB;
- data Object key: 1,024 bytes;
- sparse-index rows: 65,536;
- custom metadata: 1,024 entries and 1 MiB total;
- ensemble segments: 65,536;
- members per ensemble segment: 1,024;
- Bookie ID: 4 KiB;
- any other string: 64 KiB;
- entry count: `1..2^31-1`.

An empty sealed ledger is not encoded as an NPO1 offload attempt. The offload path must exclude it before publication.
The reader first performs HEAD and rejects a root whose declared/provider length exceeds 8 MiB, then performs one
bounded full GET and validates total length plus self-digest before trusting section lengths, counts, offsets, or the
sparse index.

## Consequences

- `V2-OPEN-BK-06` is resolved.
- Extreme ledgers must roll earlier or use larger data blocks rather than expanding the root/parser without bounds.
- NPO1 duplicates some stock metadata intentionally to make the two-object attempt independently verifiable.
- NPD1 data-block structure is refined by ADR 0044 and checked envelope/derived row by ADR 0056. M2 fixes the NPO1
  attempt-key envelope above, the selected numeric block/Object limits and codec thresholds, and current-source golden
  vectors; a later major version may not reinterpret these v1 bytes.
- M2 must prove every boundary/overflow/duplicate/order/trailing-byte rejection, pre-allocation limit checks,
  empty-ledger exclusion, canonical re-encode, and self-digest corruption behavior.

This decision is refined by ADRs 0044/0056/0057, refines ADRs 0024/0029, and is tracked by `T-BK-01`,
`V2-BK-004..006/009/012/013`.
