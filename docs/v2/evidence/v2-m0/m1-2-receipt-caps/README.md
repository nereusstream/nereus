# M1-2 receipt/parser capacity readiness evidence

## Result boundary

`RECEIPT_CAPACITY_READINESS_ONLY`; `promotionEligible=false`;
`productionReceiptParserImplemented=false`; `m1Final=false`; `scenarioPromotion=false`.

This deterministic test/evidence-only artifact binds Nereus `75593faf11c5934908d6ffcd9977648f8fa49ea2`, the source-lock input SHA-256
`4eeec6ea5f1445b8e3494c83be4289b3b4a6df17a48678d5b181f61298317814`, 36 focused tests, eleven named receipt samples, and exact root/attachment/path/log formulas. It
does not implement G1, publish N1, enter K1/P1/R1, run real Oxia, promote a scenario, or publish
any generated `REGISTRY_CONFORMANCE` / `HARNESS_CONFORMANCE_ONLY` test vector as an authoritative
N2, N3, or M1 Final receipt.

## Authority and generation rules

ADR 0084 is the sole normative cap table. `receipt-caps.json` is its machine-checked evidence
projection; this Markdown does not maintain another numeric cap table.

The baseline inventory is the sorted, exact module/suite count and XML-byte snapshot from the three
focused Gradle test tasks at the required baseline. Representative reports retain every named suite
and normalized count. The Registry attachment is the structured R0 `184 + 14*120 + 256*192` layout;
maximum-failure and sanitized-log artifacts emit one distinct semantic row per stable rejection or
named fault cut. No artifact is enlarged with an anonymous repeated string.

The JSON records the executable formulas. Root headroom is fourfold because the largest sample does
not simultaneously maximize the independently closed scenario axis; report/Registry, bundle, path,
segment, and log margins use their stated twofold/fourfold or closed-kind composition rules.

Observed maxima were 16079 root bytes, 7 scenarios in one kind-specific root, 73 suites, 20 attachment
references, 84025 generated single-attachment bytes, 158760 kind-complete bundle bytes, 115 path bytes, 5
segments, and 15425 sanitized-log bytes. The actual pre-M1-2 JUnit XML corpus at the required baseline is
91 suites / 386 tests / 96,248 bytes; R0's structured Registry boundary is exactly 51,016 bytes.

## Artifact identity

- JSON: `receipt-caps.json`
- JSON SHA-256: `2197c814dc887d742cdda119f4e68c4f5f2276df0f44b15de3d524a2445c692d`
- required baseline: `7ede023e19774309268350a866932804787a52a7`
- generated and committed JSON/Markdown bytes must be identical
