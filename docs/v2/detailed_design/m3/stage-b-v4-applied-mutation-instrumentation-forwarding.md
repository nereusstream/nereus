# Stage B V4 applied-mutation instrumentation forwarding

- Design status: Accepted through ADR 0131
- Runtime status: implementation pending exact-source diagnostic recertification
- Selection authority: none

## Failure attribution

Exact `3bc11088...` retained about six operations and four reads per derived workflow even though the production
adapter implemented ADR 0130. The exact receipt is `c5c24ec3...1892`; its seven-file diagnostic archive is bound by
identity `4dd9c050...656f` and manifest `82466fb0...7cc7`. It is non-authoritative.

The shared `InstrumentedClient` and request-bound `BoundClient` did not override the newly added acknowledgement
methods. The interface default deliberately converted a successful legacy mutation into an empty acknowledgement,
which forced the mutation engine's safe reread fallback. Formal and diagnostic therefore did not compose the exact
production acknowledged-success behavior.

## Corrected composition

Both wrapper levels explicitly forward `createIfAbsentAcknowledged` and `compareAndSetAcknowledged`. One generic
mutation instrument preserves the result value while retaining:

- exact binding-key rejection and request telemetry;
- diagnostic real/outstanding counters;
- controlled-latency scheduler and callback measurements;
- injected response loss after real apply;
- the crash-after-apply barrier; and
- exactly one terminal completion.

The crash barrier is generic and releases the original value only when the cut is released. A hidden response still
causes the production mutation engine to reread; an observed successful acknowledgement emits no synthetic reread.

## Required proof

A deterministic contract installs a delegate whose legacy mutation methods fail. Both acknowledged methods must pass
through the bound instrumented client, return exact versions, produce the two expected operation start/end pairs, and
leave legacy count zero. Protocol source contracts lock both wrapper overrides.

The create-new exact-source 25ms receipt must then show four common-path operations and zero drop/failure/timeout for
both rows. Full V4 diagnostic remains 23 tests/nine suites and must seal/parse canonical NADV4 before formal entry.
