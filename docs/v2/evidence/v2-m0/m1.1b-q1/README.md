# M1.1b-Q1 NTA1 readiness evidence

## Result boundary

`READINESS_EVIDENCE_ONLY`; `promotionEligible=false`.

This receipt binds deterministic, test-scope measurements for the Proposed M1.1b design. It is not an accepted NTA1
contract, production encoder/parser, exact backend conformance result, scenario PASS, M1.1b completion, or M1 PASS.
No Docker, Kafka/Pulsar runtime, or Oxia backend was started.

The structured artifact is [readiness.json](readiness.json). It was generated from real current
`TopicBindingAggregateV1`, NPC1, NTI1, NTB1, and NSE1 objects by a test-scope-only candidate writer and bounded reader.

## Source and test identity

- Nereus evidence parent: `d35f75755ff3717eaf0cf5a083b0afae16df9707`;
- source tuple: `v2-m0`;
- pinned Kafka source: `76f62f3b83e882105219b6c7687dbde594a8b8a2`;
- pinned Pulsar source: `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9`;
- focused suite: `Nta1ReadinessEvidenceTest`;
- discovered/executed/passed: `14/14/14`;
- failed/errors/skipped: `0/0/0`.

The Kafka source fixes its 249-character ASCII rule. The Pulsar source provides canonical topic/persistence-name
conversion but no native length cap. Consequently, both Pulsar caps remain product-format candidates.

## Exact measured sizes

The zero-payload candidate table has 129 fixed NTA1 bytes outside the embedded Cell and incarnation. Existing
production leaf codecs produce:

| Quantity | Bytes | Basis |
| --- | ---: | --- |
| Kafka NPC1 Cell | 38 | `4 magic + 2 protocol + 2 x 16-byte IDs` |
| Pulsar NPC1 Cell / `maxCellBytes` | 54 | `4 + 2 + 3 x 16-byte IDs` |
| Kafka NTI1 fixed part | 26 | `4 + 2 + 16 + 4` |
| Kafka max incarnation | 275 | `26 + 249` |
| Pulsar NTI1 fixed part | 22 | `4 + 2 + 4 + 4 + 8` |

Measured 4-KiB-candidate vectors are `194, 202, 442, 239, 261, 8395` bytes for Kafka minimum, Kafka typical, Kafka
249-byte maximum, Pulsar minimum, Pulsar typical, and Pulsar maximum legal classic-persistent round trip.

| Candidate | per-name cap | checked `maxIncarnationBytes` | checked parser `maxNta1Bytes` | max legal measured vector | raw 100k typical Pulsar | raw 100k all-max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| performance-biased | 4,096 | 8,214 | 8,397 | 8,395 | 26,100,000 | 839,500,000 |
| compatibility-biased | 16,384 | 32,790 | 32,973 | 32,971 | 26,100,000 | 3,297,100,000 |
| earlier total proposal | 16,384 | 32,790 | 65,536 | no added legal field uses the extra 32,563 | 26,100,000 | at most 6,553,600,000 admitted bytes |

The legal maximum is two bytes below the generic formula because one classic persistent topic/persistence pair cannot
simultaneously consume both independent per-name ceilings. The parser uses the generic checked cap; it allocates only
the already validated actual Cell and incarnation lengths.

Candidate vector digests:

- Kafka typical, 202 bytes: `d5b3b88a17c2ccda75cc558b9787e47d32828c45a716c5cc44470739dbd33406`;
- Pulsar typical, 261 bytes: `0e5bd01d5bab5388b80c138939267e7f8a895b16e5b17af40ffa23ae0d239713`;
- 4-KiB maximum, 8,395 bytes: `f9a1afb80670da5a4d249f63885e07b20b2ef2cfba0f716c169203fe4f8fa0c2`;
- 16-KiB maximum, 32,971 bytes: `5a698b30d44aea459857d2368c8935c666a72f2b0dbbfb36b65d25180981b6c3`.

## Candidate assessment

| Decision | Source/contract basis | Measurement and margin | Complexity/performance | Recommendation | Alternative / OPEN |
| --- | --- | --- | --- | --- | --- |
| `maxCellBytes` | NPC1 layouts are frozen | actual max is exactly 54 | fixed tiny allocation; no hot-path work | 54 | none unless NPC2 exists |
| Kafka name/incarnation | pinned native rule | 249 name; 275 incarnation | no extra compatibility restriction | retain 249/275 | none for v1 |
| Pulsar per-name cap | no pinned native cap | typical fixture is 29/29 bytes; 4 KiB gives about 141x name-byte headroom; 16 KiB gives about 565x | only create/replay CPU, but metadata/snapshot amplification is up to 3.93x between candidates | 4 KiB for grill | 16 KiB when native compatibility dominates; production histogram still OPEN |
| `maxNta1Bytes` | flat/no-tail v1 | exact checked caps 8,397 or 32,973 | rounded 64 KiB permits no current legal bytes and weakens allocation bound | exact derived cap | rounded cap only if a separately accepted wire field justifies it |
| non-NONE policy | Object needs a closed policy; per-frame actual codec remains explicit elsewhere | both candidate policies add zero NTA1 payload bytes | kind/version-only is simplest; threshold alternative may avoid marginal compression CPU | `{1,1,empty}=IF_SMALLER` for grill | `{2,1,empty}=SAVES_12_5_PERCENT`; NWG1 benchmark still OPEN |

These are recommendations, not frozen values. The 4-KiB measurement uses synthetic maximum strings and the repository
fixtures; it is not a production-name distribution. The harness intentionally rejects `topic://` and `segment://`
until the grill decides whether M1.1b covers those pinned scalable-domain forms.

## Negative and safety coverage

The 14 tests cover all six protocol/profile rows; both non-NONE candidates; profile/NONE mismatches; mixed and unknown
kind/version; non-empty payload; Kafka 249/250; Pulsar 4-KiB/16-KiB/one-over boundaries; exact persistence/topic-name
round trip; strict Unicode/malformed UTF-8; unsigned `u32be`; checked-add overflow; total cap; truncation; strict EOF;
and validation-before-length-framed allocation.

Nothing here changes `V2-META-003`, `V2-META-004`, or any structured scenario status.

## Grill handoff

The user still needs to decide:

1. frame policy `{1,1,empty}`, `{2,1,empty}`, or defer codes pending NWG1 benchmark evidence;
2. 4 KiB versus 16 KiB per Pulsar canonical name;
3. classic `persistent://` only versus scalable `topic://`/`segment://` in NTA1 v1;
4. exact derived parser cap versus a rounded cap;
5. whether a production-name inventory is mandatory before acceptance.
