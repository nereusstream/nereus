# ADR 0056: V2 NPD1 checked envelope and derived entry row

## Status

Accepted for the 0.2 NPD1 checked wire, numeric hard envelope, and provider admission for Pulsar
`BOOKKEEPER_WAL_ASYNC_OBJECT`. The M2-P6 source-qualified evidence closes the remaining numeric/provider decision.

## Context

ADR 0044 fixes independently decodable NPD1 blocks but not their exact length domains or entry-directory row. A block
count alone can still permit a multi-terabyte data Object. Repeating every contiguous entry ID in a 24-byte row also
adds avoidable overhead for small entries. Treating a future multi-gigabyte Object cap as an allocation size would turn
a parser bound into a memory hazard.

## Decision

NPD1 v1 uses a 32-byte big-endian data-Object header and a 64-byte big-endian `NPB1` block header. Each entry-directory
row is exactly 16 bytes:

```text
decodedOffset:uint64 | payloadLength:uint32 | flags:uint32
```

The row does not repeat `entryId`. The exact entry ID is derived by checked addition from the NPO1 sparse row's
`firstEntryId` plus the zero-based row ordinal. Rows are canonical, offsets are nondecreasing and contiguous with the
decoded payload layout, and all reserved/unknown flags fail closed.

The complete checked length domains are:

```text
decodedBlockBytes      = sum(entryPayloadLength)
directoryPlaintextBytes = entryCount * 16
compressedPayloadBytes = length(codec(concatenated exact entry payload bytes))
aeadPlaintextBytes     = directoryPlaintextBytes + compressedPayloadBytes
ciphertextBytes        = aeadPlaintextBytes
encodedBlockBytes      = 64 + ciphertextBytes + 16-byte GCM tag
dataObjectBytes        = 32 + sum(encodedBlockBytes)
```

For codec `NONE`, `compressedPayloadBytes == decodedBlockBytes`. NPD1 adds no padding or hidden cross-block bytes.
Every count, offset, addition, multiplication, codec result, range end, and derived entry ID is checked before range
use or allocation. Persisted lengths and format/admission maxima use unsigned-64 semantic domains represented by
validated Java `long` values; negative values and values above `Long.MAX_VALUE` are rejected. A parser allocates only
from validated actual counts and lengths, never from an absolute format maximum.

NPD1 defines `maxDataObjectBytes = 4 GiB`, `maxEntryBytes = 64 MiB`, `maxDecodedBlockBytes = 64 MiB`, and
`maxEntriesPerBlock = 65,536` for 0.2. The adapter admits at most 1,024 multipart parts. These are deployment/Cell hard
ceilings that a Namespace or Topic cannot raise. The data-Object hard cap is not
an in-memory buffer size, one-GET requirement, upload target, or normal Object size. Upload, SHA-256 calculation, and
response-loss full-body verification are streaming or bounded-segment operations; constructing a data-Object-sized
`ByteBuffer` is forbidden.

Multipart part count is not NPD1 wire identity. The Provider adapter/Protocol Cell resolves an operational upload cap
that a Topic cannot raise. Profile admission must validate all of these provider capabilities against the lower
deployment limits: maximum Object size, minimum and maximum part size, maximum part count, streaming upload, streaming
or bounded range/full read, and deterministic multipart-residue discovery/cleanup. Missing capability rejects
`BOOKKEEPER_WAL_ASYNC_OBJECT`; it never changes existing bytes or silently selects a weaker proof.

## Consequences

- `V2-OPEN-BK-11` is resolved by the M2-P6 candidate matrix, exact LocalStack protocol execution, fixed MinIO provider
  execution, published S3 capability sources, codec goldens, and fail-closed admission tests.
- Four GiB and 1,024 parts are accepted lower Nereus limits; they do not advertise a provider's larger limits or make a
  4-GiB allocation legal.
- Small entries avoid an eight-byte repeated-ID cost, while random access still derives an exact native entry ID from
  authenticated NPO1/block facts.
- M2 must prove overflow rejection at every term, row/entry-ID derivation, actual-count allocation, streaming upload /
  digest/full verification, provider-capability rejection, and multipart-residue cleanup.

This decision refines ADRs 0024, 0029, 0035, 0044, and 0049 and is tracked by `T-BK-01`, `T-POLICY-01`, and
`V2-BK-009/012`.
