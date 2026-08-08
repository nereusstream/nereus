# ADR 0010: V2 topic profile binding

## Status

Superseded by ADR 0012.

The retained part of this decision is the immutable Topic Protocol Binding and the separation of mutable operational
policy. Storage profile, physical format, and checksum/encryption family now belong to immutable Storage Epochs in an
append-only chain rather than one lifetime binding.

## Context

A topic profile changes the primary WAL authority, recovery procedure, read fallback, retention proof, and provider cost.
Treating that decision as a mutable tuning flag would create an online migration protocol before V2 has a customer or a
compatibility obligation.

## Superseded decision (historical)

The original decision said every topic incarnation had one immutable semantic storage binding containing:

- protocol and durable topic identity;
- one of the three V2 storage profiles;
- primary-WAL and tiering semantics;
- payload mapping and durable format family;
- encryption/checksum family;
- binding version and topic incarnation.

This lifetime-profile restriction is no longer V2 authority. ADR 0012 replaces it with an immutable Topic Protocol
Binding plus append-only, epoch-scoped profiles.

Operational policy is separate and mutable. Batching, linger, cache size, concurrency, throttles, retention duration,
and compaction cadence may change without changing the semantic binding. A policy change that affects object layout or
encryption starts a new WAL run or segment generation at an explicit boundary.

## Historical consequences

- Recovery never guesses which provider or ACK contract produced a range.
- Operators retain normal performance tuning without a data migration.
- The original decision did not implement in-place V1 migration or cross-profile historical reads.

This decision is tracked by `T-PROFILE-01`, `T-COMPAT-01`, and scenario `V2-PROFILE-001`.
