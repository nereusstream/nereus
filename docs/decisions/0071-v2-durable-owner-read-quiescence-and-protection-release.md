# ADR 0071: V2 durable owner-read quiescence and protection release

## Status

Accepted for the 0.2 `OBJECT_WAL` two-generation durable source-handoff cut, capability-tiered old-owner quiescence,
protection-release prerequisites, retained-source safety, and configuration scope. Read-admission ordering and the
source-independent proof window are refined by ADR 0073; immutable capability evidence is refined by ADR 0074. Exact
selector/terminal-proof cuts are refined by ADRs 0075..0077, while per-source release and batch retirement are refined
by ADR 0078. Exact physical encodings, batch-size limits, and numeric time/capacity bounds remain M4/M5 work;
implementation has not started at M0.

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
2. `PREFERRED_ONLY` is selected by the Binding/incarnation `BindingReadSelector` CAS or a proven equivalent atomic
   transaction and names no fallback for that bounded retirement batch. An unknown result converges only by exact
   selector/view/batch reread.

`PREFERRED_ONLY` is necessary but never sufficient to release protection. Source i's GC eligibility requires all of:

1. the exact `PREFERRED_ONLY` generation is durably selected;
2. every current-owner slot that can name a fallback-bearing view has drained;
3. for source i, every Read Admission Epoch in its exact `[first_i, sharedLast]` fallback-capable interval is covered
   contiguously by durable source-independent owner-read quiescence evidence; and
4. that exact source-protection generation is released by idempotent CAS before its physical GC.

Planned handoff publishes a low-frequency durable `OwnerReadQuiescenceProof`; the optional handoff hint is not that
proof. The source-independent proof binds at least Binding/incarnation, exact Read Admission Epoch and Owner Epoch, a
durable read-admission-stopped fence, drained-through read-view generation, safe-after authority time, exact
proof/capability digest, and planned-drain or qualified-expiry proof identity. A proof of only the latest owner cannot
cover earlier gaps.

ADR 0073 replaces a per-retirement-batch mutable accumulator with one Binding-scoped `ReadAdmissionEpoch` order, one
source-independent proof per epoch, and one reusable bounded proof window/head. Each immutable retirement batch carries
the exact fallback set and shared last epoch; each source row carries its own first epoch, and every source interval
must be covered contiguously before its release. A native Owner Epoch may substitute for that order only after proving
the same Binding-local publication/ordering contract. ADR 0077 fuses no-fallback selection, E closure, and E+1 grant;
ADR 0076 requires one irreversible epoch terminal cut and on-demand deterministic proof before coverage can advance.

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
versioned Protocol Cell/backend admission fact, not a Topic performance switch. ADR 0074 fixes the two closed
capability discriminators and requires every historical owner, proof, fold, batch, and release CAS to bind the exact
immutable admission-evidence digest rather than reinterpret a current backend configuration.

One `PREFERRED_ONLY` generation may cover an evidence-bounded set of fallback sources/ranges, avoiding one selector CAS
per extent. It does not eliminate at most N exact protection-release CAS operations or bounded O(N) authoritative
reconciliation. A quarantined source does not block eligible sibling release but does block complete batch retirement.
Retained protection/batch count, bytes, age, and oldest deadline consume Cell admission and alerting budgets. Pressure
may block handoff, retirement, or new admission; it never manufactures quiescence or releases protection.

This ADR applies to Object-WAL manifest/source handoff. Pulsar sealed-ledger BookKeeper eligibility/deletion remains
owned by ADRs 0036, 0045, and 0052 and its native metadata state machine.

## Consequences

- `V2-OPEN-READ-04` is resolved without adding remote metadata to normal reads.
- Each bounded retirement batch pays one fused selector/view CAS in inline mode, or one immutable create plus that CAS
  in an atomically validated reference mode, plus up to N source releases and bounded O(N) reconciliation. A backend
  lacking quiescence capability may retain source bytes for a long time and eventually backpressure.
- M4/M5 must prove exact selector/view/batch response-loss recovery, current/old-owner drains, multiple takeover gaps,
  planned proof versus non-authoritative hint, every expiry-capability clause, exact protection CAS, batch substitution,
  retained bytes/age admission, and zero read-path metadata I/O.
- M4 measures `PREFERRED_ONLY` CAS frequency, retained protection bytes/age, and takeover-to-GC-eligibility p99.
- Exact selector/terminal physical encoding, proof-window/fold encoding, and evidence-derived capability token/receipt
  limits remain downstream gates.

This decision is refined by ADRs 0073..0078, refines ADRs 0049, 0051, 0067, and 0069, and is tracked by
`T-MANIFEST-01`, `T-HANDOFF-01`, `T-POLICY-01`, `V2-READ-001/003/004/006/008..013`, and
`V2-OPEN-READ-08/09/14/15`.
