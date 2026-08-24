# ADR 0096: M3 owner-open conservative rollover amendment

Status: Accepted

## Decision

This amendment replaces the ADR 0093 M3 owner-open path that would restore an admitting Root or an exact pending
candidate. M3 has no durable, fence-authenticated pending-dispatch wire sufficient to prove that state. After exact
current-Root lineage recovery, owner fence, one shared recovery envelope, physical checkpoint stream, all three
strong-LIST folds, and per-extent authenticated prefix/Directory verification, recovery may form a cut only if every
Provider/KMS outcome is terminal and no unresolved candidate remains. The restored runtime is always
`STOPPING/OWNER_REQUEST`, with `reserved == resolved`, no pending lane reservation, and no new sequence admission.
It must Seal and publish a successor.

Provider/KMS UNKNOWN, a LIST gap/duplicate/substituted identity, or an incomplete authenticated physical stream
prevents cut creation and session open. The cut does not infer absence, burn, or a pending plan identity.

For production `NONE` proof rows the compact physical spool uses `R=56` canonical bytes and `S=M*R`. The persisted
working cap must satisfy `W >= max(S + max(maxCheckpointPageBytes, maxDirectoryPrefixBytes, maxStoredFrameBytes,
maxDecodedFrameBytes+256, (M+1)*K+J*1024), J*B)`, with checked arithmetic. The Kafka protocol Object is verified in
the predecessor-lineage pass before the current-Root physical spool exists, so `J*B` is a sequential maximum and is
not added to `S`. The 256-byte term bounds synchronous protocol-stager digest/length/compact metadata beside one
borrowed decoded frame; a stager may not retain or copy the payload. The `+1` is the continuation/probe slot; VERSION
mode is reserved wire and cannot enter this M3 production path.

`W` is the Root-governed canonical working-set counter: it counts the fixed spool and each acquired canonical
Provider/control/decode region once. It is not a claim about JVM object headers, allocator slack, defensive-copy
amplification, RSS, or direct-memory high-water marks. Those host implementation measurements remain D3 evidence and
cannot be inferred from D1 or from this admission inequality. The production path must still release every logical
lease and erase secret scratch buffers; a future heap claim requires exact-source D3 evidence rather than relabeling
this counter.

### NWR1 control-golden lineage

This amendment also closes the previously unpublished control-test fixture after the recovery envelope became
worst-case closed. Its pre-closure SHA assertions were not backed by the final Root-admitted request/key and
owner-open working-set closure and never produced a receipt. They are superseded explicitly as follows; lengths do
not change:

```text
artifact                  bytes  pre-closure SHA-256                                               closed SHA-256
NWR1_BASE_NONE_V1           541  1a563d4791da52c8378e4977c58c81fc854492fcd4d6395bff6511d57c28188a  6cdab8ede9279e4c067a81a70f6ffb98b7433dd3df5ba199124b3f12d9734367
NWP1_BASE_NONE_V1           116  79d6b5954146e09399a7b085e04065675fb156bd373d078dcee5417b3afa33ef  2f402622fc7abba243aca9ef7e6b1b87aad286ada8e890c59d48c900beb7c3b8
NWS1_BASE_NONE_V1           263  c9bfaa41bbb2e4eb53a94bfc33cada3c2397b44c5319d6960434266794b5445a  bf26faa57404ff44b79baf8dab220449d56ff8c0d464d75345949e34a29b0a50
NWC1_BASE_NONE_V1           131  8994f43e8073f68fac3cf707298cf093c1b8a100c74f48179f1fe1ad53d98971  10ea321efb086c594e7566d51e7b54663d6c45f3e1b627d42c29681187a2fce9
NWH1_BASE_NONE_V1           283  b74c04a440277ac0d9c2a76be81a2cafb57c34c4d6753fb767dd52fca41534f3  5fb18c31560579b9f6a85aee828fcb0bf49e227e35ede8464170f8e1f244e952
CONTROL_PROJECTION_TSV     7855  ea304b1c0cbfb920791ea05d0ef671eeab117b8b65b3e77617f6fe8da9927acb  686468eb8e006bf05f7181885eada3f42825142bf67ac9076b857ef90e4f4b6e
NWR1_KAFKA_SUCCESSOR_V1     820  ff9fd35130d652f4bf84e7e426acf7e023236a5dba311b7c42c394526b844073  ff5ae6f5eaf6795addaff09b4a73de3942608fdfa0f0a4cd88a4f890f0ecf3f4
```

This is an explicit fixture/golden amendment, not a parser relaxation. `NWR1` wire version, field order, widths,
codes, and strict decoding are unchanged.

### Different-winner pointer conflict

ADR 0093's same-call validated-winner adoption is narrowed for M3. A prospective candidate has already charged its
own current-Root slot and the exact predecessor closure. If its pointer CAS observes a different successor, loading
that winner in the same cumulative envelope can require a third Root even when the legal minimum is
`maxLiveRoots=2,maxPredecessorRuns=1`; reusing either slot would silently validate the winner under the losing
candidate's persisted envelope.

The losing call therefore rereads only the bounded pointer. An exact self-winner remains successful; a different
winner returns a fail-closed retry-required error without reading or adopting its Root. Owner-open then bootstraps the
exact pointer winner and validates that Root plus its predecessor closure under the winner's own persisted envelope.
This is not a counter reset or missed adoption: it is a new owner-open attempt whose budget authority is the durable
winner. The losing candidate Root is immutable but orphaned and is never admission authority.

## Consequences

This is conservative M3 rollover, not same-Root resume. A future admitting resume requires a separate accepted
amendment with a durable fenced pending-dispatch wire, exact plan/object identity, and real Provider/KMS evidence.
`NWR1` and NWG1 bytes are unchanged.
