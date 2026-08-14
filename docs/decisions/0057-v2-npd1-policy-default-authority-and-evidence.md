# ADR 0057: V2 NPD1 policy default authority and evidence

## Status

Accepted as the NPD1 block-policy resolution, selected class catalog, and Deployment default for 0.2. M2-P6 completes
the source-qualified candidate and pinned-native evidence required by the original protocol.

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

The source-qualified P6 evidence selects exactly three common typed classes: `latency-1mib` at 1 MiB,
`balanced-4mib` at 4 MiB, and `scan-8mib` at 8 MiB. `balanced-4mib` is the Product/Deployment base default. The 16-MiB
candidate is rejected: on the 20-MiB scan ledger it saved only one provider request relative to 8 MiB while materially
increasing exact-entry read amplification and did not improve the measured sequential result. Eligible ZSTD is
`ZSTD_IF_SMALLER`; each sparse row persists the actual `NONE` or `ZSTD` family.

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

- `V2-OPEN-BK-13` is resolved by the P6 receipt and the exact 1/4/8-MiB class catalog/default above.
- Placement cannot create a second default authority, and normal Topics need not carry bespoke manual configuration.
- Earlier close may produce blocks below the class target; target size is operational intent, not a minimum byte
  promise or decoder rule.
- M2 must prove resolution precedence, failover stability, unsupported override rejection, Cell/host non-reinterpretation,
  every evidence candidate, and native-relative cold-read cost.

This decision refines ADRs 0020, 0044, 0049, and 0056 and is tracked by `T-BK-01`, `T-POLICY-01`, and
`V2-BK-012/013`.
