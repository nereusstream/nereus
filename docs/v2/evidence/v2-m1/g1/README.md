# G1 focused receipt/Final validator evidence

This directory binds the production JDK-only receipt parser, secure bounded reference verifier, non-rerunning Final
resolver, exact virtual-ledger promotion policy, registered M1 gate surfaces, and evidence-only allocator harness at
Nereus `ba11fe4a29c3158bb4d7c46e379c9a918745b7ef`.

`v2M1G1ValidatorCheck` runs four receipt suites with 49 tests and two allocator evidence suites with 14 tests. All are
executed exactly once with zero failure, error, and skip. The source checker also proves that Final cannot invoke a
build/test process and that STRICT/RANGE candidates do not exist in production domain, metadata SPI, or metadata
Oxia source.

The focused result is `PASS_G1_FOCUSED_ONLY`, not a canonical conformance receipt and not M1 Final. It does not prove
the pure-V2 graph, run Exact Source, select an allocator, promote a scenario, perform N2/N3, or claim M1 PASS. The
pure-V2 graph prune is complete, making the registered Fast gate executable; it still does not prove Exact Source,
Final, N2/N3, scenario promotion, or M1 PASS.
