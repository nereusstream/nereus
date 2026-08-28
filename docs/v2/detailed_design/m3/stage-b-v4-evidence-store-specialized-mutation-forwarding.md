# Stage B V4 evidence-store specialized mutation forwarding

- Design status: Accepted through ADR 0132
- Runtime status: implementation pending exact-source diagnostic recertification
- Selection authority: none

## Failure attribution

Exact `e53b3af8...` proved that the acknowledgement survived both Oxia instrumentation wrappers: derived reconcile
retries fell to zero. It nevertheless retained four reads per workflow because `M3EvidenceAllocatorStore` inherited
the SPI defaults for the two installed-RANGE specialized methods. Those defaults deliberately route to the ordinary
proofful methods, so the production adapter performed the two safe mutation rereads.

Receipt `e4b39721...56aba` and its three-file archive remain diagnostic-only. Archive identity is
`4fd18526...4208`; manifest is `bac9bf6e...23e`; no bytes are promotable or reusable by another campaign.

## Corrected composition

The evidence decorator explicitly forwards:

- node create to `createNodeAfterStoreObservedRangeAuthorities`; and
- Head publish CAS to `compareAndSetHeadAfterStoreObservedRangeNode`.

Both calls use the existing exact-key binding and `mutation` terminal-accounting helper. The helper still owns raw
fault write proofs and typed terminal outcomes. `productionDelegate(binding)` remains the only adapter constructor, so
formal and diagnostic execute production codecs, resolver checks, acknowledged-success handling, and fallback logic.
Only the clean installed-RANGE path can select these methods.

## Required proof

A deterministic contract installs an acknowledged-only client below `M3EvidenceAllocatorStore`. The node create and
Head CAS must return exact decoded snapshots, increment the two acknowledged counters, leave the legacy counter at
zero, and record exactly `CREATE_IF_ABSENT` then `COMPARE_AND_SET` with no `READ`.

The next pushed exact source must rerun the 10k/25ms fixed-1000 and derived-800 sequence. Both rows require zero
drop/failure/timeout; the derived common path must be exactly four operations/two reads. Only after the full 23-test,
nine-suite diagnostic seals and parse-canonically validates NADV4 may a new immutable formal directory be created.
