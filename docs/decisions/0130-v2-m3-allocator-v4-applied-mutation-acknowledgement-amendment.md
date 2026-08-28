# ADR 0130: M3 V4 applied-mutation acknowledgement amendment

- Status: Accepted
- Date: 2026-08-29
- Extends: ADR 0129
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `ad9dce4f1665486e90d88d6ca539b6a64676615a` implemented ADR 0129 and ran the
diagnostic-only RANGE-1024 10k/25ms fixed-1000 then exact-derived-800 sequence. The receipt SHA-256 is
`772908e14a3c73eeaabad4ff706b2755a2c6f60683b8ba78df101985a6199dc1`; its filtered JUnit inventory is
1/0/0/0. The fixed row admitted/completed 21,982 of 30,000 offers and dropped 8,018. The derived row
admitted/completed 21,118 of 24,000 and dropped 2,882. Both had zero failure/timeout, reached 256 global outstanding,
and drained completely. The derived common path had zero reconcile retries and exactly 174,552 operations for 29,092
warm-up plus measured workflow completions: six operations per request. Its workflow p99 remained 191,659us, which
cannot sustain the frozen 1,600 req/s 2R storm without pre-admission drops.

The complete diagnostic and JUnit bytes are preserved at
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-ad9dce4f-v4-25ms-proof-reuse-insufficient-r1`.
Its archive-identity SHA-256 is `df5bd8bf5ba0da1643f559bbbe7d7b60e6c87de0ef77b87cb0fd613ce8010414`,
manifest SHA-256 is `935dbe2b150e971fc5e528ea4529c6915a8d2e4b839a1d65602353ab62d1a126`, and its seven
payload files total 5,592 bytes. It is diagnostic-only, non-authoritative, non-promotable, and cannot become a
campaign input.

The pinned Oxia async API returns a `PutResult` only after a successful conditional put. That result binds the
effective key and committed record version. The current narrow client erases this response to `Void`, so the
allocator performs another get even though it already owns the exact canonical candidate bytes and Oxia has returned
their committed key/version. Conflict or response-unknown completions do not return this successful acknowledgement
and still require the existing same-key reread.

## Decision

1. Add a narrow acknowledged conditional-mutation operation that preserves the successful Oxia `PutResult` key and
   version. The adapter must reject a null result, null version, different key, or negative version. Stores that do
   not implement the acknowledgement retain the existing mutation-plus-reread behavior by default.
2. Only the ADR-0129 store-observed installed-RANGE branch may use acknowledged node create and Head CAS. It locally
   encodes the exact candidate, validates the returned key, maps the returned Oxia version, decodes those exact bytes
   through the production resolver, and accepts only an exact candidate snapshot.
3. A clean successful acknowledgement replaces the redundant success get for that mutation. A conditional conflict,
   missing acknowledgement, response loss, dispatch failure, malformed result, or non-exact candidate still enters
   the existing same-key reread and typed reconcile path. Public allocator calls, STRICT, grant install/renewal,
   takeover, fault cuts, stale-node handling, and any post-conflict retry retain their current proof reads.
4. The uncontended installed-RANGE common path is four operations across three controlled-latency stages: concurrent
   Cell/Head reads, acknowledged node create, and acknowledged exact-predecessor Head CAS. It adds no cache authority,
   Java Cell lock, queue, retry, ledger-ID consumption, or late-completion dispatch.
5. Unit contracts must prove zero success rereads, exact committed-version propagation, fallback rereads for response
   loss/conflict, direct-API compatibility, one-ID consumption, and operation-context authorization of both new store
   calls. The formal-equivalent 25ms diagnostic must then prove four operations per uncontended request, zero
   drop/failure/timeout, complete drain, and current-source canonical 23-test/nine-suite NADV4 before formal entry.
6. V4 plan digest `1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975`, admission,
   workload, rates, SLOs, zero-drop rule, budgets, selection order, and NACP4/NAEV4/NARS4/NADV4 bytes remain unchanged.

## Consequences

This is a production correctness-proof amendment, not a qualification relaxation. The historical `83193069...-r1`
formal result and `ad9dce4f...` diagnostic remain immutable negative evidence. The implementation changes source and
executor bytes and therefore requires a new exact clean pushed source, a create-new diagnostic/NADV4, all source
gates, and a create-new formal directory. Allocator mode remains `UNSELECTED`; source locks, children, scenarios, and
M3 Final remain open until a uniquely qualified formal result exists.
