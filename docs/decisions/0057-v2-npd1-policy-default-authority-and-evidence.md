# ADR 0057: V2 NPD1 policy default authority and evidence

## Status

Accepted as the NPD1 block-policy resolution and evidence protocol for 0.2. No block class, target, codec threshold, or
default value is accepted, and implementation/runtime evidence has not started at M0.

## Context

Prematurely freezing five named block classes would multiply the compatibility, benchmark, and operations matrix.
Allowing both Namespace and Protocol Cell to choose a semantic default would also make the same Topic resolve
differently after placement or failover. A 16-MiB authenticated block can materially amplify a one-entry cold read
relative to Pulsar's current approximately 1-MiB offload read-buffer baseline.

## Decision

The evidence candidates are 1, 4, 8, and 16 MiB block targets only; they are not wire enum values. The benchmark covers
exact-entry random reads and sequential ranges, provider p50/p99 latency, request count and transferred bytes,
whole-block AEAD/decode CPU, heap/direct-memory peak and concurrency, compression ratio, 100-byte entries, native
maximum-entry and maximum-entries-per-ledger settings, the stock 5-MiB message case, and dedicated near-hard-cap
entries. It exercises codec `NONE` and eligible ZSTD behavior without a redundant RAW class and compares the pinned
native Pulsar path, including its approximately 1-MiB read buffer.

After source-qualified evidence, 0.2 may admit at most three common typed classes. Until then there is no durable class
enum and no implicit target.

Semantic default authority is one-way:

1. the Product/Deployment supplies one validated base default;
2. a Pulsar Namespace inherits or explicitly overrides it with an admitted typed class;
3. a Topic inherits the Namespace choice or uses an explicit admitted typed override;
4. the Protocol Cell performs class admission and applies hard caps/shared resource budgets but never chooses a
   replacement semantic default;
5. the host supplies only CPU, memory, I/O, cache, and concurrency ceilings.

The final resolved class is persisted in the native offload attempt before NPD1 generation and is read back after
failover. Cell/host pressure may force an earlier block close, backpressure, or admission rejection, but cannot relabel
or reinterpret already generated NPD1. A default or override change affects only a later offload attempt.

## Consequences

- `V2-OPEN-BK-13` remains open until the benchmark receipt selects no more than three classes and their exact values.
- Placement cannot create a second default authority, and normal Topics need not carry bespoke manual configuration.
- Earlier close may produce blocks below the class target; target size is operational intent, not a minimum byte
  promise or decoder rule.
- M2 must prove resolution precedence, failover stability, unsupported override rejection, Cell/host non-reinterpretation,
  every evidence candidate, and native-relative cold-read cost.

This decision refines ADRs 0020, 0044, 0049, and 0056 and is tracked by `T-BK-01`, `T-POLICY-01`, `V2-BK-012/013`,
and `V2-OPEN-BK-13`.
