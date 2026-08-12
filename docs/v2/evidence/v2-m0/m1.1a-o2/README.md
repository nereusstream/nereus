# M1.1a-O2 local scaffold evidence

This receipt binds the completed local `nereus-metadata-oxia` capability-scaffold implementation to source tuple
`v2-m0`. It is focused local evidence, not real Oxia/Pulsar conformance and not a promotion receipt.

- Nereus implementation commit: `050f908afa1832694b99bd156b17c9e06b9e9a6c`
- O1 client implementation base: `24b730d1d66a1da701f4c99957361f6b3c5d748c`
- O1 client final fork: `091a42c2780d92da56e9ec1f02ce1c3d988adc16`
- immutable client/client-api manifest SHA-256:
  `521a7a3615b9f25d3e459633fff614f03208a13efda0ab9913b2255a9f2f40ab`
- Oxia server read-only source baseline: `37a17bef17202d5fd6e23282da5fd26d94865484`
- focused O2 tests: 9 suites, 69 discovered/executed/passed, 0 failure/error/skip/abort
- whole `nereus-metadata-oxia` module: 72 suites, 299 discovered/executed/passed, 0 failure/error/skip/abort
- structured result: [focused-local-scaffold.json](focused-local-scaffold.json)

The final local commands are:

```text
bash scripts/check-v2-documentation.sh
./gradlew v2DocumentationCheck v2M1FoundationCheck v2M1OxiaScaffoldCheck --no-daemon --console=plain
./gradlew :nereus-metadata-oxia:check --no-daemon --console=plain
git diff --check
```

`PASS_LOCAL_SCAFFOLD_ONLY` means the immutable dependency, four adapters, exact reread outcomes, production fail-closed
codec boundary, and local continuity/race behavior passed deterministic checks. `promotionEligible=false` because O2
does not contain complete NTA1, P1/R1, ownership A/read/B, Registry capacity/interlock, runtime activation, real
Oxia/Pulsar conformance, scenario promotion, or an M1 Final receipt.
