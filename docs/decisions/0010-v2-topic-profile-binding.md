# ADR 0010: V2 topic profile binding

## Status

Accepted for the V2 design. Implementation and runtime evidence are not started at M0.

## Context

A topic profile changes the primary WAL authority, recovery procedure, read fallback, retention proof, and provider cost.
Treating that decision as a mutable tuning flag would create an online migration protocol before V2 has a customer or a
compatibility obligation.

## Decision

Every topic incarnation has an immutable semantic storage binding containing at least:

- protocol and durable topic identity;
- one of the three V2 storage profiles;
- primary-WAL and tiering semantics;
- payload mapping and durable format family;
- encryption/checksum family;
- binding version and topic incarnation.

The binding is created before the first append and cannot be changed online. A different semantic profile requires a new
topic incarnation or a future explicitly designed migration protocol.

Operational policy is separate and mutable. Batching, linger, cache size, concurrency, throttles, retention duration,
and compaction cadence may change without changing the semantic binding. A policy change that affects object layout or
encryption starts a new WAL run or segment generation at an explicit boundary.

## Consequences

- Recovery never guesses which provider or ACK contract produced a range.
- Operators retain normal performance tuning without a data migration.
- V2 does not implement in-place V1 migration or cross-profile historical reads.

This decision is tracked by `T-PROFILE-01`, `T-COMPAT-01`, and scenario `V2-PROFILE-001`.
