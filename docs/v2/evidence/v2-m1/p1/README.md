# P1 focused evidence

`p1-focused.json` binds the exact clean Pulsar fork, immutable N1/P1/O1 artifacts, the source-qualified Oxia image,
14 Nereus metadata suites with 94 tests, one real-Oxia suite with two tests, and seven Pulsar suites with 34 tests.
Every mandatory test has zero failure, error, and skip.

The receipt kind and result are `P1_FOCUSED_ONLY` and `PASS_P1_FOCUSED_ONLY`. It proves the selector/aggregate authority,
authoritative A/read/B ownership witness, continuity invalidation, stale-install exclusion, capability admission gate, and
one local ACTIVE fence. It does not activate BrokerService/PersistentTopic Produce or read paths, select an allocator,
promote a scenario, prune V1, or claim M1 PASS. Full process/data-path integration remains M6.

`p1-artifact.json` is the separate current immutable adapter-input receipt. Neither receipt is promotion eligible.

This historical focused receipt remains byte-for-byte bound to the V1-residue development fork
`778862323d8a86e2f36064a12166e09918ed9429`; it is not rewritten. Final M1 exact-source execution instead uses the
pure-V2 Pulsar fork `072aa1c440f85b808f60e7ea59de8a73c4e2a202`, where the current inventories are 14 Nereus metadata
suites with 100 tests, one real-Oxia suite with two tests, and seven Pulsar suites with 36 tests. That execution is
recorded in the N2 gate result and N3 promotion receipts.
