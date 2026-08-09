# ADR 0071: V2 durable owner-read quiescence and protection release

## Status

Accepted for the 0.2 `OBJECT_WAL` two-generation durable source-handoff cut, capability-tiered old-owner quiescence,
protection-release prerequisites, retained-source safety, and configuration scope. Exact quiescence accumulator/wire,
backend capability encoding, batch-size limits, and numeric time/capacity bounds remain M4/M5 work; implementation has
not started at M0.

## Context

ADR 0069 requires two pin-drained reclamation stages. A durable manifest generation that removes fallback stops new
fallback pins under the current view, but does not prove that another process or Owner Epoch has stopped using the
source. The existing planned handoff hint is explicitly non-authoritative. A normal Owner Epoch, KRaft/Pulsar ownership
fence, session-loss observation, or host timer also does not by itself provide read-admission expiry or a bound over
provider I/O, retry, fallback, decode, and source-backed buffers.

## Decision

Object-WAL source handoff uses two immutable, fenced manifest-generation states:

1. `PREFERRED_WITH_FALLBACK` names the preferred source, the exact bounded fallback source/ranges, and their source-
   protection generations;
2. `PREFERRED_ONLY` is selected by exact manifest-root CAS and names no fallback for that bounded retirement batch.
   An unknown CAS result converges only by exact generation/root reread.

`PREFERRED_ONLY` is necessary but never sufficient to release protection. GC eligibility requires all of:

1. the exact `PREFERRED_ONLY` generation is durably selected;
2. every current-owner slot that can name a fallback-bearing view has drained;
3. every older Owner Epoch capable of admitting such a read is covered by durable owner-read quiescence evidence; and
4. the exact source-protection generation is released by idempotent CAS before physical GC.

Planned handoff publishes a low-frequency durable `OwnerReadQuiescenceProof`; the optional handoff hint is not that
proof. The proof binds at least Binding/incarnation, fallback manifest/protection generation, old Owner Epoch, a durable
read-admission-stopped fence, drained-through read-view generation, and maximum admitted source-access deadline.
Repeated takeovers require a bounded, verifiable coverage statement such as `quiescedThroughOwnerEpoch`; this name is
valid only if the backend proves a Binding-local ordered Owner-Epoch contract. Otherwise an equivalent bounded lineage
proof is required. A proof of only the latest owner cannot cover earlier gaps.

Unplanned takeover may synthesize qualifying quiescence only when the Protocol Cell/backend capability proves all of:

- the lease/expiry both authorizes and limits read admission, not only writer admission;
- the authority supplies verifiable `notAfter`/expiry time semantics;
- every read batch has a hard `maxSourceAccessLifetime` covering provider I/O, retry, fallback, decode, and source-
  backed-buffer use;
- an old owner admits no new read after lease uncertainty/expiry;
- after GC pause, process pause, or network recovery, the old owner rechecks owner fence and deadline before new source
  I/O, fallback/retry, or response publication; and
- the new owner derives eligibility from authority time/expiry proof plus the admitted lifetime, clock-skew, and
  propagation-grace bounds, never from a host-local sleep.

Without either an exact planned-drain proof or that complete authority-expiry capability, 0.2 retains protection and
defers GC. It does not add a distributed per-read refcount, and Topic policy cannot weaken this rule. Capability is a
versioned Protocol Cell/backend admission fact, not a Topic performance switch.

One `PREFERRED_ONLY` generation may cover an evidence-bounded set of fallback sources/ranges, avoiding one manifest
CAS per extent. Retained protection count, bytes, age, and oldest deadline consume Cell admission and alerting budgets.
Pressure may block handoff, retirement, or new admission; it never manufactures quiescence or releases protection.

This ADR applies to Object-WAL manifest/source handoff. Pulsar sealed-ledger BookKeeper eligibility/deletion remains
owned by ADRs 0036, 0045, and 0052 and its native metadata state machine.

## Consequences

- `V2-OPEN-READ-04` is resolved without adding remote metadata to normal reads.
- Each bounded retirement batch pays one additional low-frequency manifest generation/CAS plus proof/release control
  work. A backend lacking quiescence capability may retain source bytes for a long time and eventually backpressure.
- M4/M5 must prove exact generation/root response-loss recovery, current/old-owner drains, multiple takeover gaps,
  planned proof versus non-authoritative hint, every expiry-capability clause, exact protection CAS, batch substitution,
  retained bytes/age admission, and zero read-path metadata I/O.
- M4 measures `PREFERRED_ONLY` CAS frequency, retained protection bytes/age, and takeover-to-GC-eligibility p99.
- Exact bounded owner-coverage accumulator and capability record are the next design frontier.

This decision refines ADRs 0049, 0051, 0067, and 0069 and is tracked by `T-MANIFEST-01`, `T-HANDOFF-01`,
`T-POLICY-01`, `V2-READ-001/003/004/006`, and `V2-OPEN-READ-06/07`.
