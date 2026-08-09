# ADR 0029: V2 Pulsar sealed-ledger root and lifecycle

## Status

Accepted for 0.2 Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0024 chooses one data Object plus one root Object per native `(ledgerId, attempt UUID)` offload attempt. The pair is
not ledger-equivalent unless its keys survive configuration drift, the root binds all necessary sealed-ledger facts,
publication validates the real read path, and cleanup remains deterministic when either object or a multipart response
is missing.

## Decision

Within the persisted Cell Provider Scope, key-derivation version 1 uses exactly these logical suffixes:

```text
pulsar-offload/v1/ledger-<ledgerId>/attempt-<uuid>/data
pulsar-offload/v1/ledger-<ledgerId>/attempt-<uuid>/root
```

`ledgerId` is non-negative decimal without leading zeroes; `uuid` is canonical lowercase RFC 4122 text. Both objects
use immutable conditional create. Native driver metadata persists the exact provider location/scope, key prefix, and
`keyDerivationVersion=1`; restart never reapplies current configuration to an old attempt.

Root v1 is a bounded canonical binary record containing at least:

- format version, total length, required flags, key-derivation version, and data-format version;
- complete ledger ID, UUID, and Cell Provider Scope attempt binding;
- sanitized closed-ledger metadata, including LAC, entry count, logical length, creation/fencing facts, quorum/digest
  descriptors, canonical custom metadata, and ordered ensemble segments, but never a BookKeeper password;
- the derived data key, exact canonical data-body length, SHA-256/v1 Object Extent Digest, optional qualified
  `ProviderObjectProof`, and outer compression/encryption descriptors;
- bounded sparse rows `{firstEntryId, entryCount, blockOffset, blockLength}`;
- an independently named root self-digest over the canonical root domain.

The index starts at entry zero, covers every entry through LAC exactly once, and has strictly increasing, non-overlapping,
overflow-safe byte ranges wholly inside the data body. `entryCount == LAC + 1`. Multipart part IDs are transport detail
and have no persisted read authority. A reader validates root length/count bounds and the complete root self-digest
before trusting any variable-length field or offset.

Publication order is fixed:

1. freeze and validate the sealed source view;
2. derive both keys and canonical data bytes;
3. conditional-create and prove the data Object under ADR 0025;
4. construct, locally round-trip, conditional-create, and exactly verify root v1;
5. open the actual `readOffloaded` path and verify attempt identity, sealed metadata, coverage, and representative index
   boundaries;
6. only then complete the offload future and permit native completion publication.

Deletion first derives both keys from persisted attempt facts. It proves the root absent before deleting data, then
proves data absent. It also aborts attempt-scoped incomplete multipart uploads or relies on an admitted provider
lifecycle rule that gives an equivalent bounded guarantee. Root absence never makes the data key undiscoverable, and
deletion succeeds only when both committed objects and covered multipart residue are absent.

The self-digest detects corruption but is not a signature against malicious whole-root replacement. Adding a MAC,
signature, or other external trusted binding requires a separate threat-model decision.

## Consequences

- `V2-OPEN-BK-05` is resolved.
- The root is more complete than the stock index and requires a full bounded verification before offsets are trusted.
- Persisted location/key derivation prevents current configuration from redirecting old attempts.
- M2 must prove canonical-root bounds, corruption and identity rejection, real `ReadHandle` equivalence, every
  publication cut, root-before-data deletion, configuration drift, response loss, and multipart residue cleanup.

NPO1 wire/bounds and native dual-source safety are refined by
[ADRs 0035](0035-v2-pulsar-npo1-sealed-ledger-root-format.md) and
[0036](0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md). This decision refines ADR 0024 and is tracked by
`T-BK-01`, `V2-BK-004..008`.
