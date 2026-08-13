# R1 focused Registry evidence

`r1-focused.json` binds Nereus `8a213a85bfaa15769a9b9ea4f74ac7e0b2500b6d`, immutable N1, the exact O1
client, and the source-qualified local Oxia server image. The focused inventory is four domain suites with 35 tests,
two metadata-authority suites with eight tests, and one real-Oxia suite with two tests. Every mandatory test has zero
failure, error, and skip.

The gate exercises the production NLI1/NVR1/NVA1/RAE1 codecs, exact capacity and lifecycle validation, held writer
interlock, immutable admission evidence, closed create/CAS response-loss outcomes, concurrent creators, restart, and
versioned derived slice views. Its conformance subject is `REGISTRY_CONFORMANCE`, but the focused wrapper is deliberately
`R1_FOCUSED_ONLY`, `selectionEligible=false`, and `promotionEligible=false`. It is an input to G1/N2/N3, not the
canonical RFC-8785/JCS N3 receipt and not a scenario or M1 PASS.

No STRICT/RANGE allocator mode is selected, no Pulsar data path is activated, and every `V2-POSITION-003..009` scenario
remains `PLANNED` until the trusted promotion cut validates the final source tuple.

This receipt is not rewritten after shared Oxia V2 fixes. Current N2 execution binds the same closed inventory to
Nereus `42598fe63324ceceb07d39114ff36a770af35eb9`; the current source tuple is recorded only by the Exact Source gate and
N3 receipt.
