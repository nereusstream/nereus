# Stage B V4 applied-mutation acknowledgement

- Design status: Accepted through ADR 0130
- Runtime status: implementation pending exact-source diagnostic recertification
- Selection authority: none

## Measured boundary

The exact `ad9dce4f...` 25ms receipt has SHA-256 `772908e1...dc1` and JUnit 1/0/0/0. ADR-0129 reduced the
uncontended derived row to six operations per request with zero reconcile retry, but fixed-1000/derived-800 still
dropped 8,018/2,882 offers. Derived workflow p99 remained 191.659ms. The byte-verified seven-file archive is bound by
identity `df5bd8bf...0414` and manifest `935dbe2b...a126`; it is not formal or NADV4 input.

## Scoped acknowledgement path

`AsyncOxiaConditionalClient` preserves the successful Oxia `PutResult` as an exact key/version acknowledgement.
`ConditionalMutationEngine` has separate acknowledgement-aware methods. They construct an exact versioned record from
the locally encoded candidate bytes and the server-committed version, then run the same production resolver. A
different key, absent acknowledgement, conditional miss, response loss, read/dispatch error, or non-exact decode uses
the prior same-key reread and typed outcome rules.

The SPI exposes default acknowledgement-aware RANGE operations that delegate to the ordinary proofful calls. Only
`OxiaVirtualLedgerAllocatorStore` replaces those defaults. `ProductionVirtualLedgerAllocator` invokes them only from
the package-private store-observed RANGE methods, and the bounded workflow's guarded store authorizes each call before
dispatch. Public allocator methods and every STRICT/renewal/conflict/fault path retain the ordinary mutation engine.

The clean installed-RANGE sequence is therefore:

1. concurrently read exact Cell and Head;
2. create the exact immutable node and accept its committed key/version acknowledgement;
3. CAS the exact Head predecessor and accept its committed key/version acknowledgement.

This is four operations in three sequential controlled-latency stages. It does not introduce a cache, shared Cell
lock, hidden queue, unbounded outstanding work, extra retry, or a dispatch after the workflow deadline.

## Required proof

Focused unit tests bind successful acknowledgement to zero rereads and exact versioned snapshots, while response loss
and version conflict retain one same-key reread. Workflow tests bind the specialized operations to the installed-RANGE
branch and keep direct API proof reads. Existing conflict/rebase/response-loss/deadline/no-next-operation contracts
remain mandatory.

The exact-source formal-equivalent 25ms receipt must show four operations per uncontended request and zero
drop/failure/timeout for fixed-1000 and derived-800. The complete current-source V4 diagnostic remains 23 tests in the
same nine suites and must seal/parse canonical NADV4 before another formal campaign. Historical evidence, V4 plan,
workload, qualification, and selection bytes are unchanged.
