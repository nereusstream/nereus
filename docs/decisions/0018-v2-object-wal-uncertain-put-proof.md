# ADR 0018: V2 Object WAL uncertain PUT proof

## Status

Accepted for `OBJECT_WAL`. Implementation and real-provider evidence are not started at M0.

## Context

An immutable Object WAL PUT can succeed at the provider while its response is lost. Retrying under a new identity can
duplicate physical data and obscure which object covers acknowledged protocol positions; treating the timeout as
success can acknowledge bytes that were never stored. ETag is not a portable content hash under multipart upload,
server-side encryption, or provider-specific semantics.

The WAL must resolve the uncertain result from deterministic identity and verified bytes without adding a remote
control-metadata commit to normal append.

## Decision

`OBJECT_WAL` admits only a provider/operation mode that supplies deterministic immutable create, prevents overwrite of
an existing identity, provides the required read-after-write behavior, and permits bounded byte verification.

After a lost PUT response, recovery uses this capability-tiered proof:

1. `HEAD` is sufficient only when it returns the exact expected length and a trustworthy whole-content checksum bound
   to the same immutable object identity/version.
2. If `HEAD` cannot supply that proof, recovery performs a bounded full `GET` of the immutable object and recomputes the
   expected checksum. The configured maximum WAL-group size and recovery byte/time budgets make the verification
   bounded.
3. ETag alone is never sufficient evidence of content identity.
4. A missing object may be retried only under the same deterministic immutable identity and conditional-create
   semantics. An existing object with mismatched length, version, or checksum is quarantined and fails closed; it is
   never overwritten.
5. Exhausting the verification budget does not produce an ACK. The operation remains uncertain for bounded
   reconciliation, and admission/backpressure prevents unbounded payload or future retention.

Provider capability is validated before a topic can use `OBJECT_WAL`; a provider that cannot satisfy these rules is
rejected for that profile rather than silently downgraded to weaker durability.

## Consequences

- `V2-OPEN-OBJ-02` is resolved.
- Rare response-loss cases may pay for a full Object WAL group GET and can delay or fail the append instead of guessing.
- The object format must carry or deterministically derive the expected exact length, immutable identity/version
  binding, and checksum material needed by recovery.
- M3 must test real-provider successful PUT/lost response, absent object, checksum/version mismatch, conditional-create
  races, verification-budget exhaustion, and restart reconciliation.

Checksum byte domains are refined by [ADR 0021](0021-v2-object-wal-checksum-domains.md), and the initial algorithms and
provider-proof fields are refined by
[ADR 0025](0025-v2-initial-checksum-algorithms-and-provider-proof.md). Run/group identity and discovery are refined by
[ADR 0030](0030-v2-object-wal-run-root-and-content-addressed-discovery.md). This decision is tracked by `T-OBJECT-01`,
`V2-APP-003`, `V2-OBJ-001`, `V2-OBJ-003`, and `V2-OBJ-005`.
