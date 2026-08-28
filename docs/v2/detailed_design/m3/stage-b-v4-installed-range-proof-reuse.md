# Stage B V4 installed-RANGE proof reuse

- Design status: Accepted through ADR 0129
- Runtime status: exact `ad9dce4f...` diagnostic proved six operations but insufficient 25ms capacity
- Selection authority: none

## Measured boundary

The exact `d434f910...` 25ms attribution receipt has SHA-256 `42db8caa...b899` and JUnit 1/0/0/0. Fixed-1000 and
derived-800 both reached 256 global outstanding and drained every admitted request without failure or timeout, but
dropped 10,976 and 6,604 offers. Real RTT p99 stayed below 7.4ms and delay-scheduler p99 lag below 11.6ms; workflow
p99 reached 565.286/289.615ms. The dominant cost is the installed-RANGE proof chain, not terminal accounting.

## Scoped production path

`BoundedVirtualLedgerAllocatorWorkflowV2.acquireGrant` is the sole entry to proof reuse. It first performs the same
concurrent exact Cell/Head store read and confirms a usable installed RANGE grant. Its internal allocator calls then:

1. construct the candidate from those exact authorities;
2. dispatch create-if-absent and accept only the exact node from its mandatory same-key reread;
3. derive the exact successor Head from that node;
4. dispatch exact-predecessor CAS and accept only the mandatory same-key reread result.

This is six physical metadata operations in five sequential stages: initial Cell/Head, node create, node reread, Head
CAS, Head reread. It removes no mutation reconciliation.

The public allocator calls remain proofful. Grant renewal, STRICT, fault/takeover, response-loss, and any create
conflict use the prior independent reads and explicit reconcile paths. A local Cell snapshot is never durable
authority, and no shared Java lock is introduced.

## Required proof

Unit contracts bind the fast path to one initial Cell/Head read, no extra node proof read, exact one-ID consumption,
concurrent-authority observation, conflict rebase, response loss, timeout, and no-next-dispatch behavior. Direct API
contracts prove their Cell/Head/node proof reads are unchanged.

The full exact-source V4 diagnostic remains 23 tests/nine suites. The 25ms receipt must show the reduced operation
inventory and zero drop/failure/timeout for both RANGE-1024 rows before another formal campaign. Historical V4 results
and the plan/qualification/selection contract are unchanged.

The exact `ad9dce4f...` execution confirmed six operations per uncontended derived workflow but retained 2,882 drops.
ADR 0130 therefore supersedes only the clean-success mutation-reread portion through the server's exact applied
key/version acknowledgement. This record remains authoritative for the earlier duplicate-proof removal.
