# ADR 0090: V2 M3 NWG1 mutation external-call profiles

## Status

Accepted on 2026-08-23 as a corpus-schema amendment to
[ADR 0088](0088-v2-m3-nwg1-implementation-input-closure.md), before the tracked NWG1 manifest, mutation receipt, or
scenario evidence exists. This amendment defines the previously abbreviated `X0` and `XU` tokens; it does not change
NWG1 bytes, cryptography, rejection precedence, mutation recipes, or the frozen 84-record/240-path totals.

## Context

M3-I0 fixes `X0/XU = 30/54` and requires every authored mutation record to carry
`expectedMaximumExternalCallsByKind`, but it did not expand either token. The negative verifier starts at
`PRELOADED_VERIFIED_ROOT_AND_ACQUIRED_BYTES_V1`: Root authority, the leaf/key, and required Object bytes are already
provided. Leaving the two call profiles undefined would let an implementation reach the right counts while hiding
Provider calls or disagreeing about whether KMS unwrap is permitted.

## Decision

The tokens and their exact meanings are:

```text
X0 = NO_EXTERNAL_CALLS_AFTER_PRELOADED_CUT
XU = AT_MOST_ONE_KMS_UNWRAP_CALL_AFTER_PRELOADED_CUT
```

Every record authors all eleven closed M3 external-call kinds. `X0` sets every maximum to zero. `XU` sets
`KMS_UNWRAP=1` and every other maximum to zero:

```text
ROOT_AUTHORITY_READ
METADATA_READ
METADATA_CONDITIONAL_MUTATION
KMS_WRAP
KMS_UNWRAP
OBJECT_CONDITIONAL_PUT
OBJECT_HEAD
OBJECT_FULL_GET
OBJECT_PREFIX_RANGE_GET
OBJECT_FRAME_RANGE_GET
OBJECT_LIST_PAGE
```

These are maximums, not required calls. An `XU` mutation rejected before the KMS stage therefore records zero actual
unwraps and still conforms. It may never perform a second unwrap or any metadata/Provider call after the preloaded
cut. Local parsing, digesting, HKDF, AEAD, decompression, native validation, publication counters, and ACK counters are
not external calls and remain separately observable.

The canonical manifest authors exactly 30 `X0` records and 54 `XU` records. It uses the full token names above rather
than unexplained abbreviations, authors each per-kind maximum explicitly, and records actual counts per validation
path. The mutation gate rejects missing/extra call kinds, negative counts, actual counts above maximum, a hidden call,
or any distribution other than `30/54`.

## Consequences

- The accepted 84 records, 240 path executions, 25 rejection codes, 16 stages, and 50 deep Root/key domains do not
  change.
- The ten closed mutation-operation tokens and eight resign-operation tokens do not change.
- `X0` and `XU` prove only the post-preloaded verifier cut. They do not prove process-start Root discovery, Provider
  recovery, KMS service availability, or real Provider/KMS capacity.
- A future verifier that needs another external call profile requires an explicit manifest-schema/ADR amendment; it
  cannot extend this inventory at runtime.
